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


logger = get_logger(__name__)

# 線程局部存儲 - 用於跟踪線程狀態
_thread_local = threading.local()

# Load 階段處理器
class LoadProcessor(ETLProcessor[pd.DataFrame, Dict[str, bool]]):
    """
    數據加載處理器
    負責將處理後的數據保存到目標位置
    """
    def __init__(self, context: ETLContext = None, output_dir: str = 'data/final_aggregated'):
        """
        初始化加載處理器
        
        參數:
            context: ETL上下文
            output_dir: 輸出目錄
        """
        super().__init__(context)
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        # 用於在多線程環境中保護統計信息更新
        self._stats_lock = threading.RLock()
    
    def _perform_aggregation(self, df: pd.DataFrame, groupby_cols: List[str], 
                             agg_dict: Dict[str, str], post_process=None) -> Tuple[pd.DataFrame, int]:
        """
        執行數據聚合操作，提取為獨立方法以便於錯誤追踪
        
        返回:
            (聚合後的DataFrame, 記錄數)
        """
        start_time = time.time()
        # 執行分組聚合
        agg_df = df.groupby(groupby_cols).agg(agg_dict).reset_index()
        
        # 應用後處理函數
        if post_process and callable(post_process):
            agg_df = post_process(agg_df)
        
        record_count = len(agg_df)
        agg_time = time.time() - start_time
        
        logger.info(f"聚合操作完成: {record_count} 條記錄, 耗時 {agg_time:.2f} 秒")
        return agg_df, record_count
    
    def _safe_write_file(self, df: pd.DataFrame, filename: str, **params) -> bool:
        """
        安全地寫入文件，處理各種異常情況
        """
        try:
            # 確保目錄存在
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # 線程安全的文件寫入
            with file_lock_manager.get_lock(filename):
                # 根據文件擴展名選擇合適的寫入方法
                if os.path.splitext(filename)[1] == '.xlsx':
                    df.to_excel(filename, **params)
                else:
                    df.to_csv(filename, **params)
            
            return True
        except Exception as e:
            logger.error(f"寫入文件 {filename} 失敗: {str(e)}")
            return False
    
    def process(self, df: pd.DataFrame, dimension: str, filename: str, **kwargs) -> bool:
        """
        處理單個維度的聚合和保存
        
        參數:
            df: 輸入的DataFrame
            dimension: 聚合維度 ('store', 'product', 'date' 等)
            filename: 輸出文件名
            **kwargs: 額外的參數，如聚合函數等
        
        返回:
            是否成功處理
        """
        # 初始化線程本地存儲
        if not hasattr(_thread_local, 'current_task'):
            _thread_local.current_task = {
                'dimension': dimension,
                'filename': filename
            }
            
        try:
            logger.info(f"開始處理維度: {dimension}, 輸出至: {filename}")
            
            # 參數驗證
            if not isinstance(df, pd.DataFrame) or df.empty:
                logger.error(f"維度 {dimension}: 輸入數據為空或不是DataFrame")
                return False
                
            # 根據維度選擇分析方式
            # 允許自定義維度
            if 'groupby_cols' in kwargs and 'agg_dict' in kwargs:
                groupby_cols = kwargs['groupby_cols']
                agg_dict = kwargs['agg_dict']
                post_process = kwargs.get('post_process')
                
                # 使用獨立的聚合方法處理
                try:
                    agg_df, record_count = self._perform_aggregation(
                        df, groupby_cols, agg_dict, post_process
                    )
                except Exception as e:
                    logger.error(f"維度 {dimension} 聚合操作失敗: {str(e)}")
                    return False
            else:
                logger.error(f"未知的分析維度: {dimension}, 缺少必要的 groupby_cols 和 agg_dict 參數")
                return False
            
            # 準備寫入參數
            write_params = kwargs.get('write_params', {})
            if 'index' not in write_params:
                write_params['index'] = False
                
            # 安全寫入文件
            if not self._safe_write_file(agg_df, filename, **write_params):
                return False
            
            # 線程安全地更新統計信息
            with self._stats_lock:
                if self.context and hasattr(self.context, 'stats'):
                    self.context.stats.file_processed(filename, record_count)
            
            logger.info(f"完成處理維度: {dimension}, 記錄數: {record_count}")
            return True
            
        except Exception as e:
            error_type = type(e).__name__
            # 線程安全地記錄錯誤
            with self._stats_lock:
                if self.context and hasattr(self.context, 'stats'):
                    self.context.stats.record_error(error_type)
            logger.error(f"處理維度 {dimension} 時發生錯誤: {str(e)}")
            logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
            return False
        finally:
            # 清理線程本地存儲
            if hasattr(_thread_local, 'current_task'):
                delattr(_thread_local, 'current_task')
    
    def process_concurrent(self, 
                           df: pd.DataFrame, 
                           reports: List[Dict[str, Any]], 
                           max_workers: int = 3, 
                           **kwargs) -> Dict[str, bool]:
        """
        並行處理多個報表的生成
        
        參數:
            df: 輸入的DataFrame
            reports: 報表配置列表，每項包含dimension和filename
            max_workers: 最大工作線程數
            **kwargs: 傳遞給單個處理的額外參數
        
        返回:
            各報表處理結果
        """
        if df is None or len(df) == 0:
            logger.error("無法載入: 輸入數據為空")
            return {}
            
        # 輸入驗證
        if not reports:
            logger.warning("沒有報表需要生成")
            return {}
        
        # 計算實際線程數 - 避免線程過多浪費資源
        actual_workers = min(max_workers, len(reports))
        
        start_time = time.time()
        logger.info(f"開始資料加載和報表生成, 將使用 {actual_workers} 個工作線程")
        
        # 安全的結果存儲和計數器
        results = {}
        results_lock = threading.RLock()
        task_counter = {'completed': 0, 'total': len(reports), 'successful': 0}
        counter_lock = threading.RLock()
        
        def process_report_with_tracking(report):
            """帶有跟踪功能的報表處理函數"""
            dimension = report['dimension']
            filename = report.get('filename', f"{self.output_dir}/{dimension}_report.csv")
            
            # 合併報表特定參數和全局參數
            report_params = {**kwargs}
            if 'params' in report:
                report_params.update(report['params'])
                
            try:
                result = self.process(df, dimension, filename, **report_params)
                
                # 線程安全地記錄結果
                with results_lock:
                    results[dimension] = result
                
                # 更新成功計數
                if result:
                    with counter_lock:
                        task_counter['successful'] += 1
                        
                return result
            except Exception as e:
                logger.error(f"處理報表 {dimension} 時發生意外錯誤: {str(e)}")
                logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
                
                # 記錄失敗
                with results_lock:
                    results[dimension] = False
                return False
            finally:
                # 更新進度計數
                with counter_lock:
                    task_counter['completed'] += 1
                    completed = task_counter['completed']
                    total = task_counter['total']
                    success = task_counter['successful']
                    logger.info(f"報表進度: {completed}/{total} ({completed*100/total:.1f}%), 成功: {success}")
        
        # 使用ThreadPoolExecutor生成報表 (I/O密集型)
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # 提交所有報表生成任務
            futures = list(executor.map(process_report_with_tracking, reports))
        
        # 完成總結
        success_count = task_counter['successful']
        total_count = len(reports)
        total_time = time.time() - start_time
        
        logger.info(f"載入階段完成, 成功: {success_count}/{total_count} ({success_count*100/total_count:.1f}%), "
                    f"總耗時: {total_time:.2f}秒, 平均每報表: {total_time/total_count:.2f}秒")
        
        return results