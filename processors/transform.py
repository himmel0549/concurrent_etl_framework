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
            # 模擬與數據量成正比的處理時間
            processing_time = len(df_chunk) * self.processing_factor
            time.sleep(processing_time)
            
            # 1. 新增日期欄位
            df_chunk['year'] = pd.to_datetime(df_chunk['date']).dt.year
            df_chunk['month'] = pd.to_datetime(df_chunk['date']).dt.month
            df_chunk['day'] = pd.to_datetime(df_chunk['date']).dt.day
            df_chunk['weekday'] = pd.to_datetime(df_chunk['date']).dt.weekday
            
            # 2. 計算業務指標
            df_chunk['revenue'] = df_chunk['total_price']
            df_chunk['discount_amount'] = df_chunk['quantity'] * df_chunk['unit_price'] * df_chunk['discount']
            df_chunk['profit_margin'] = np.random.uniform(0.15, 0.45, size=len(df_chunk))  # 模擬利潤率
            df_chunk['profit'] = df_chunk['revenue'] * df_chunk['profit_margin']
            
            # 3. 分類標籤 - 允許自定義區間
            price_bins = kwargs.get('price_bins', [0, 1000, 5000, 10000, 50000, float('inf')])
            price_labels = kwargs.get('price_labels', ['極低', '低', '中', '高', '極高'])
            df_chunk['price_category'] = pd.cut(df_chunk['unit_price'], bins=price_bins, labels=price_labels)
            
            # 4. 應用自定義轉換函數
            custom_transform = kwargs.get('custom_transform')
            if custom_transform and callable(custom_transform):
                df_chunk = custom_transform(df_chunk)
            
            return df_chunk
        except Exception as e:
            error_type = type(e).__name__
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
            # 提交所有分區處理任務
            future_to_chunk = {
                executor.submit(self.process, chunk, **kwargs): i 
                for i, chunk in enumerate(df_split)
            }
            
            # 收集結果
            results = []
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    result = future.result()
                    results.append(result)
                    with log_lock:
                        logger.info(f"完成分區 {chunk_idx+1}/{num_partitions} 的轉換")
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