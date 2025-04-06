import os
import concurrent.futures
import time

import pandas as pd
import numpy as np

from utils.logging import get_logger
from core.interfaces import ETLProcessor
from core.context import ETLContext
from config.constants import log_lock


logger = get_logger(__name__)

# 定義一個模組級別的處理函數，用於多進程處理
def _transform_chunk_worker(chunk_data, chunk_index, processing_factor=0.002, **process_kwargs):
    """獨立的數據轉換函數，可被多進程調用"""
    try:
        # 模擬與數據量成正比的處理時間
        processing_time = len(chunk_data) * processing_factor
        time.sleep(processing_time)
        
        # 1. 新增日期欄位
        chunk_data['year'] = pd.to_datetime(chunk_data['date']).dt.year
        chunk_data['month'] = pd.to_datetime(chunk_data['date']).dt.month
        chunk_data['day'] = pd.to_datetime(chunk_data['date']).dt.day
        chunk_data['weekday'] = pd.to_datetime(chunk_data['date']).dt.weekday
        
        # 2. 計算業務指標
        chunk_data['revenue'] = chunk_data['total_price']
        chunk_data['discount_amount'] = chunk_data['quantity'] * chunk_data['unit_price'] * chunk_data['discount']
        chunk_data['profit_margin'] = np.random.uniform(0.15, 0.45, size=len(chunk_data))
        chunk_data['profit'] = chunk_data['revenue'] * chunk_data['profit_margin']
        
        # 3. 分類標籤
        price_bins = process_kwargs.get('price_bins', [0, 1000, 5000, 10000, 50000, float('inf')])
        price_labels = process_kwargs.get('price_labels', ['極低', '低', '中', '高', '極高'])
        chunk_data['price_category'] = pd.cut(chunk_data['unit_price'], bins=price_bins, labels=price_labels)
        
        # 4. 應用自定義轉換函數
        custom_transform = process_kwargs.get('custom_transform')
        if custom_transform and callable(custom_transform):
            chunk_data = custom_transform(chunk_data)
        
        return chunk_data, chunk_index, None  # 返回數據和索引
    except Exception as e:
        return None, chunk_index, str(e)  # 返回錯誤信息


# Transform 階段處理器
class TransformProcessor(ETLProcessor[pd.DataFrame, pd.DataFrame]):
    """
    數據轉換處理器
    負責數據的轉換和處理
    """
    def __init__(self, context: ETLContext = None, processing_factor: float = 0.002):
        """
        初始化轉換處理器
        
        參數:
            context: ETL上下文
            processing_factor: 處理因子，用於模擬處理時間
        """
        super().__init__(context)
        self.processing_factor = processing_factor
    
    def process(self, df_chunk: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        處理單個數據塊的轉換
        
        參數:
            df_chunk: 輸入的DataFrame分塊
            **kwargs: 額外的轉換參數
        
        返回:
            轉換後的DataFrame
        """
        try:
            # 使用全局函數處理，代碼重用
            result, _, error = _transform_chunk_worker(
                df_chunk, 
                0,  # 只是一個佔位符索引
                self.processing_factor, 
                **kwargs
            )
            
            if error:
                raise Exception(error)
                
            return result
        except Exception as e:
            error_type = type(e).__name__
            if self.context and hasattr(self.context, 'stats'):
                self.context.stats.record_error(error_type)
            with log_lock:
                logger.error(f"轉換數據時發生錯誤: {str(e)}")
            raise
    
    def process_concurrent(self, 
                           df: pd.DataFrame, 
                           num_partitions: int = None, 
                           max_workers: int = None, 
                           **kwargs) -> pd.DataFrame:
        """
        並行處理數據轉換
        
        參數:
            df: 輸入的完整DataFrame
            num_partitions: 分割的分區數量（預設使用CPU核心數）
            max_workers: 最大工作進程數（預設等於num_partitions）
            **kwargs: 傳遞給轉換處理的額外參數
        
        返回:
            轉換後的完整DataFrame
        """
        if df is None or len(df) == 0:
            with log_lock:
                logger.error("無法轉換: 輸入數據為空")
            return pd.DataFrame()
        
        start_time = time.time()
        with log_lock:
            logger.info("開始數據轉換")
        
        # 確定分區數和工作進程數
        if num_partitions is None:
            num_partitions = os.cpu_count() or 4
        if max_workers is None:
            max_workers = num_partitions
        
        # 將數據分割成多個塊
        df_split = np.array_split(df, num_partitions)
        
        # 使用ProcessPoolExecutor處理轉換（計算密集型操作適合多進程）
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有分區處理任務，向模組級處理函數傳遞參數
            future_to_chunk = {
                executor.submit(
                    _transform_chunk_worker, 
                    chunk, 
                    i, 
                    self.processing_factor, 
                    **kwargs
                ): i 
                for i, chunk in enumerate(df_split)
            }
            
            # 收集結果
            results = []
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_result, idx, error = future.result()
                    if error is None:
                        results.append(chunk_result)
                        with log_lock:
                            logger.info(f"完成分區 {idx+1}/{num_partitions} 的轉換")
                    else:
                        with log_lock:
                            logger.error(f"處理分區 {idx+1} 時出錯: {error}")
                        # 記錄錯誤但在主進程中處理
                        if self.context and hasattr(self.context, 'stats'):
                            error_type = error.split(':')[0] if ':' in error else 'UnknownError'
                            self.context.stats.record_error(error_type)
                except Exception as e:
                    with log_lock:
                        logger.error(f"處理分區 {chunk_idx+1} 時出錯: {str(e)}")
        
        # 合併轉換後的結果
        if results:
            transformed_data = pd.concat(results, ignore_index=True)
            with log_lock:
                logger.info(f"轉換階段完成, 記錄數: {len(transformed_data)}, 耗時: {time.time() - start_time:.2f}秒")
            return transformed_data
        else:
            with log_lock:
                logger.error("轉換階段失敗: 沒有成功轉換任何數據")
            return pd.DataFrame()