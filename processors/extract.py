from typing import List, Dict, Tuple
import concurrent.futures
import time
import traceback
import threading

import pandas as pd

from utils.logging import get_logger
from utils.file_utils import detect_file_format
from core.interfaces import ETLProcessor
from core.context import ETLContext


logger = get_logger(__name__)

# 線程局部存儲 - 用於在多線程環境中安全地儲存和訪問線程特定數據
_thread_local = threading.local()

# Extract 階段處理器
class ExtractProcessor(ETLProcessor[str, pd.DataFrame]):
    """
    數據提取處理器
    負責從文件中讀取數據並返回DataFrame
    """
    def __init__(self, context: ETLContext = None, processing_factor: float = 0.0001):
        """
        初始化提取處理器
        
        參數:
            context: ETL上下文
            processing_factor: 處理因子，用於模擬處理時間
        """
        super().__init__(context)
        self.processing_factor = processing_factor
        # 用於在多線程環境中保護統計信息更新
        self._stats_lock = threading.RLock()
    
    def process(self, file_info: str, **kwargs) -> pd.DataFrame:
        """
        處理單個文件的提取
        
        參數:
            file_info: 可以是字串路徑或包含路徑和格式資訊的字典
            **kwargs: 傳遞給 pd.read_xxx 的額外參數
        
        返回:
            提取的DataFrame
        """
        file_path = None
        try:
            if isinstance(file_info, str):
                file_path = file_info
            else:
                file_path = file_info['path']
            
            # 使用線程局部存儲記錄當前處理的文件
            if not hasattr(_thread_local, 'current_file'):
                _thread_local.current_file = file_path
            
            logger.info(f"開始讀取文件: {file_path}")
            
            # 使用檔案格式檢測函數
            format_info = detect_file_format(file_path)
            reader_func = format_info['reader']
            reader_params = {**format_info['params'], **kwargs}
            
            # 使用適當的讀取函數
            df = reader_func(file_path, **reader_params)
            
            # 模擬與數據量成正比的處理時間
            rows = len(df)
            cols = len(df.columns)
            processing_time = rows * cols * self.processing_factor
            time.sleep(processing_time)
            
            # 線程安全地更新統計信息
            with self._stats_lock:
                if self.context and hasattr(self.context, 'stats'):
                    self.context.stats.file_processed(file_path, rows)
            
            logger.info(f"完成讀取文件: {file_path}, 記錄數: {len(df)}, 處理時間: {processing_time:.2f}秒")
            
            return df
        except Exception as e:
            # 線程安全地記錄錯誤
            error_type = type(e).__name__
            with self._stats_lock:
                if self.context and hasattr(self.context, 'stats'):
                    self.context.stats.record_error(error_type)

            logger.error(f"讀取文件 {file_path} 時發生錯誤: {str(e)}")
            # 提供完整堆疊追蹤
            logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
            raise
        finally:
            # 清理線程局部變數
            if hasattr(_thread_local, 'current_file'):
                delattr(_thread_local, 'current_file')
    
    def process_concurrent(self, file_paths: List[str], max_workers: int = 5, **kwargs) -> pd.DataFrame:
        """
        並行處理多個文件的提取
        
        參數:
            file_paths: 文件路徑列表
            max_workers: 最大工作線程數
            **kwargs: 傳遞給 pd.read_csv 的額外參數
        
        返回:
            合併後的DataFrame
        """
        if not file_paths:
            logger.warning("沒有文件需要處理")
            return pd.DataFrame()
            
        start_time = time.time()
        all_data = []
        errors = []
        
        # 使用安全字典收集結果，避免並發修改問題
        results_dict: Dict[str, pd.DataFrame] = {}
        results_lock = threading.RLock()
        
        # 創建任務完成計數器
        task_counter = {'completed': 0, 'total': len(file_paths)}
        counter_lock = threading.RLock()
        
        def process_file_with_tracking(file_path, **kwargs):
            """帶有跟踪功能的文件處理函數"""
            try:
                result = self.process(file_path, **kwargs)
                # 線程安全地存儲結果
                with results_lock:
                    results_dict[file_path] = result
                return result
            except Exception as e:
                # 記錄詳細錯誤信息
                error_info = {
                    'file_path': file_path,
                    'error_type': type(e).__name__,
                    'error_msg': str(e),
                    'traceback': traceback.format_exc()
                }
                with results_lock:
                    errors.append(error_info)
                raise
            finally:
                # 更新進度計數
                with counter_lock:
                    task_counter['completed'] += 1
                    completed = task_counter['completed']
                    total = task_counter['total']
                    logger.info(f"進度: {completed}/{total} ({completed*100/total:.1f}%)")
        
        # 使用ThreadPoolExecutor並行讀取文件（I/O密集型操作適合多線程）
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有文件讀取任務
            futures = [
                executor.submit(process_file_with_tracking, file_path, **kwargs)
                for file_path in file_paths
            ]
            
            # 等待所有任務完成
            concurrent.futures.wait(futures)
        
        # 收集並處理所有結果
        for file_path, result_df in results_dict.items():
            all_data.append(result_df)
        
        # 記錄所有錯誤的詳細信息
        if errors:
            logger.error(f"提取過程中發生了 {len(errors)} 個錯誤:")
            for idx, error in enumerate(errors, 1):
                logger.error(f"錯誤 {idx}: 文件 {error['file_path']}")
                logger.error(f"錯誤類型: {error['error_type']}")
                logger.error(f"錯誤消息: {error['error_msg']}")
                logger.error(f"堆疊追蹤:\n{error['traceback']}")
        
        # 合併所有數據
        if all_data:
            total_time = time.time() - start_time
            success_count = len(all_data)
            total_count = len(file_paths)
            success_rate = (success_count / total_count) * 100
            
            logger.info(f"提取階段完成, 成功: {success_count}/{total_count} ({success_rate:.1f}%), "
                        f"總耗時: {total_time:.2f}秒, 平均每文件: {total_time/total_count:.2f}秒")
            
            try:
                # 安全合併數據框
                return pd.concat(all_data, ignore_index=True)
            except Exception as e:
                logger.error(f"合併數據時出錯: {str(e)}")
                logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
                
                # 嘗試更安全的合併方式
                logger.info("嘗試逐個合併數據框...")
                if len(all_data) > 0:
                    try:
                        result = all_data[0].copy()
                        for df in all_data[1:]:
                            result = pd.concat([result, df], ignore_index=True)
                        return result
                    except Exception as e2:
                        logger.error(f"替代合併方式也失敗: {str(e2)}")
                
                # 如果所有嘗試都失敗，返回第一個可用的DataFrame或空DataFrame
                return all_data[0] if all_data else pd.DataFrame()
        else:
            logger.error("提取階段失敗: 沒有成功讀取任何文件")
            return pd.DataFrame()  # 返回空DataFrame而不是None，保持一致性