# processors/output.py
from typing import List, Dict, Any, Optional, Tuple
import concurrent.futures
import os
import time
import threading
import traceback

import pandas as pd

from utils.logging import get_logger
from core.interfaces import ETLProcessor
from core.context import ETLContext
from utils.file_lock_manager import file_lock_manager
from utils.file_utils import detect_file_format  # 引入檔案格式檢測函數


logger = get_logger(__name__)

# 線程局部存儲 - 用於跟踪線程狀態
_thread_local = threading.local()

class OutputProcessor(ETLProcessor[pd.DataFrame, Dict[str, bool]]):
    """
    數據輸出處理器
    純粹負責將DataFrame保存為各種格式的文件，不包含數據聚合邏輯
    """
    def __init__(self, context: ETLContext = None, output_dir: str = 'data/final'):
        """
        初始化輸出處理器
        
        參數:
            context: ETL上下文
            output_dir: 輸出目錄
        """
        super().__init__(context)
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        # 用於在多線程環境中保護統計信息更新
        self._stats_lock = threading.RLock()
    
    def _get_merged_params(self, filename: str, output_specific_params: Dict[str, Any], 
                          global_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        獲取合併後的參數，處理優先級順序和格式特定參數
        
        參數優先級: 輸出特定參數 > 格式特定參數 > 全局參數 > 基本參數
        """
        # 使用檔案格式檢測函數獲取基本參數
        try:
            format_info = detect_file_format(filename)
            writer_method = format_info['writer']
            base_params = format_info['params'].copy()
            
            # 提取特定格式的參數 (例如 to_excel_params)
            method_specific_params = global_params.get(f'{writer_method}_params', {})
            common_params = global_params.get('common_params', {})
            
            # 組合所有參數 (按優先級順序)
            all_params = {**base_params}
            all_params.update(common_params)
            all_params.update(method_specific_params)
            all_params.update(output_specific_params)
            
            # 添加默認值
            if writer_method in ['to_csv', 'to_excel'] and 'index' not in all_params:
                all_params['index'] = False
                
            return all_params
        except Exception as e:
            logger.error(f"參數合併失敗: {str(e)}")
            # 返回默認參數
            return {**output_specific_params, 'index': False}
    
    def _safe_write_file(self, df: pd.DataFrame, filename: str, 
                        format_info: Dict[str, Any], params: Dict[str, Any]) -> bool:
        """
        安全地將DataFrame寫入文件
        """
        try:
            writer_method = format_info['writer']
            
            # 確保目錄存在
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # 線程安全的文件寫入
            with file_lock_manager.get_lock(filename):
                # 動態調用適當的寫入方法
                writer = getattr(df, writer_method)
                writer(filename, **params)
            
            return True
        except AttributeError:
            logger.error(f"找不到寫入方法 {writer_method}: DataFrame不支持此方法")
            return False
        except Exception as e:
            logger.error(f"寫入文件 {filename} 失敗: {str(e)}")
            return False
    
    def process(self, df: pd.DataFrame, output_config: Dict[str, Any], **kwargs) -> bool:
        """
        將單個DataFrame保存為指定格式的文件
        
        參數:
            df: 輸入的DataFrame
            output_config: 輸出配置，包含filename等
            **kwargs: 額外的輸出參數
        
        返回:
            是否成功輸出
        """
        filename = None
        
        # 初始化線程本地存儲
        if not hasattr(_thread_local, 'current_task'):
            _thread_local.current_task = {'config': output_config}
            
        try:
            filename = output_config.get('filename')
            if not filename:
                raise ValueError("必須提供輸出文件名")
                
            # 驗證輸入數據
            if not isinstance(df, pd.DataFrame):
                raise TypeError("輸入必須是 pandas DataFrame")
                
            if df.empty:
                logger.warning(f"警告: 嘗試輸出空的DataFrame到 {filename}")
                
            logger.info(f"開始輸出文件: {filename}, 記錄數: {len(df)}")
            
            # 使用檔案格式檢測函數
            format_info = detect_file_format(filename)
            
            # 獲取合併後的參數
            output_specific_params = output_config.get('params', {})
            all_params = self._get_merged_params(filename, output_specific_params, kwargs)
            
            # 安全寫入文件
            write_success = self._safe_write_file(df, filename, format_info, all_params)
            
            if write_success:
                # 更新統計信息 - 線程安全
                with self._stats_lock:
                    if self.context and hasattr(self.context, 'stats'):
                        self.context.stats.file_processed(filename, len(df))
                        
                logger.info(f"完成輸出文件: {filename}, 記錄數: {len(df)}")
                return True
            else:
                return False
                
        except Exception as e:
            error_type = type(e).__name__
            # 線程安全地記錄錯誤
            with self._stats_lock:
                if self.context and hasattr(self.context, 'stats'):
                    self.context.stats.record_error(error_type)
            logger.error(f"輸出文件失敗 {filename}: {str(e)}")
            logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
            return False
        finally:
            # 清理線程本地存儲
            if hasattr(_thread_local, 'current_task'):
                delattr(_thread_local, 'current_task')
    
    def process_concurrent(self, 
                           df: pd.DataFrame,
                           output_configs: List[Dict[str, Any]],
                           max_workers: int = 3,
                           **kwargs) -> Dict[str, bool]:
        """
        並行處理多個文件的輸出
        
        參數:
            df: 輸入的DataFrame
            output_configs: 輸出配置列表，每項包含filename等
            max_workers: 最大工作線程數
            **kwargs: 傳遞給單個處理的額外參數
        
        返回:
            各輸出文件處理結果
        """
        if df is None or len(df) == 0:
            logger.warning("輸入數據為空，仍將嘗試進行檔案輸出")
            
        # 輸入驗證
        if not output_configs:
            logger.warning("沒有輸出配置需要處理")
            return {}
            
        # 計算實際線程數 - 避免線程過多浪費資源
        actual_workers = min(max_workers, len(output_configs))
        
        start_time = time.time()
        logger.info(f"開始並行輸出 {len(output_configs)} 個文件，將使用 {actual_workers} 個工作線程")
        
        # 安全的結果存儲和計數器
        results = {}
        results_lock = threading.RLock()
        task_counter = {'completed': 0, 'total': len(output_configs), 'successful': 0}
        counter_lock = threading.RLock()
        
        def process_output_with_tracking(output_config):
            """帶有跟踪功能的輸出處理函數"""
            # 處理默認文件名
            if 'filename' not in output_config:
                output_config['filename'] = f"{self.output_dir}/output_{id(output_config)}.csv"
                
            filename = output_config['filename']
                
            try:
                result = self.process(df, output_config, **kwargs)
                
                # 線程安全地記錄結果
                with results_lock:
                    results[filename] = result
                
                # 更新成功計數
                if result:
                    with counter_lock:
                        task_counter['successful'] += 1
                        
                return result
            except Exception as e:
                logger.error(f"處理輸出 {filename} 時發生意外錯誤: {str(e)}")
                logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
                
                # 記錄失敗
                with results_lock:
                    results[filename] = False
                return False
            finally:
                # 更新進度計數
                with counter_lock:
                    task_counter['completed'] += 1
                    completed = task_counter['completed']
                    total = task_counter['total']
                    success = task_counter['successful']
                    logger.info(f"輸出進度: {completed}/{total} ({completed*100/total:.1f}%), 成功: {success}")
        
        # 使用ThreadPoolExecutor並行輸出 (I/O密集型)
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # 提交所有輸出任務並等待完成
            futures = list(executor.map(process_output_with_tracking, output_configs))
        
        # 完成總結
        success_count = task_counter['successful']
        total_count = len(output_configs)
        total_time = time.time() - start_time
        
        logger.info(f"輸出階段完成, 成功: {success_count}/{total_count} ({success_count*100/total_count:.1f}%), "
                    f"總耗時: {total_time:.2f}秒, 平均每文件: {total_time/total_count:.2f}秒")
        
        return results