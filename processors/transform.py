import os
import concurrent.futures
import time
import traceback

import pandas as pd
import numpy as np

from utils.logging import get_logger
from core.interfaces import ETLProcessor
from core.context import ETLContext
from config.constants import log_lock
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Type


logger = get_logger(__name__)

# 轉換策略接口
class TransformStrategy(ABC):
    """轉換策略接口，定義轉換邏輯"""
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """執行轉換邏輯"""
        pass


# 默認銷售資料轉換策略
class DefaultSalesTransformStrategy(TransformStrategy):
    """默認銷售資料轉換策略"""
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """執行銷售資料轉換邏輯"""
        # 1. 新增日期欄位
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['day'] = pd.to_datetime(df['date']).dt.day
        df['weekday'] = pd.to_datetime(df['date']).dt.weekday
        
        # 2. 計算業務指標
        df['revenue'] = df['total_price'] if 'total_price' in df.columns else df['amount']
        if 'quantity' in df.columns and 'unit_price' in df.columns and 'discount' in df.columns:
            df['discount_amount'] = df['quantity'] * df['unit_price'] * df['discount']
            df['profit_margin'] = np.random.uniform(0.15, 0.45, size=len(df))
            df['profit'] = df['revenue'] * df['profit_margin']
        
        # 3. 分類標籤
        if 'unit_price' in df.columns:
            price_bins = kwargs.get('price_bins', [0, 1000, 5000, 10000, 50000, float('inf')])
            price_labels = kwargs.get('price_labels', ['極低', '低', '中', '高', '極高'])
            df['price_category'] = pd.cut(df['unit_price'], bins=price_bins, labels=price_labels)
        
        return df


# 會計資料轉換策略
class AccountingTransformStrategy(TransformStrategy):
    """會計資料轉換策略"""
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """執行會計資料轉換邏輯"""
        # 1. 處理日期欄位
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['day'] = pd.to_datetime(df['date']).dt.day
        df['period'] = df['date'].dt.strftime('%Y-%m')
        
        # 2. 處理會計科目類型
        if 'account_code' in df.columns:
            def get_statement_type(account_code):
                if account_code.startswith(('1', '2', '3')):
                    return '資產負債表'
                elif account_code.startswith(('4', '5')):
                    return '利潤表'
                else:
                    return '其他'
            
            df['statement_type'] = df['account_code'].apply(get_statement_type)
        
        # 3. 計算借貸方餘額
        if 'direction' in df.columns and 'amount' in df.columns:
            df['debit_amount'] = df.apply(lambda x: x['amount'] if x['direction'] == '借' else 0, axis=1)
            df['credit_amount'] = df.apply(lambda x: x['amount'] if x['direction'] == '貸' else 0, axis=1)
        
        # 4. 計算餘額
        if not df.empty and 'debit_amount' in df.columns:
            # 檢查傳票借貸平衡
            voucher_groups = df.groupby('voucher_id')
            balance_check = voucher_groups.apply(
                lambda g: abs(g['debit_amount'].sum() - g['credit_amount'].sum()) < 0.01
            ).reset_index()
            balance_check.columns = ['voucher_id', 'is_balanced']
            df = pd.merge(df, balance_check, on='voucher_id', how='left')
        
        return df


# 模組級別的處理函數，用於多進程處理
def _transform_chunk_worker(chunk_data, chunk_index, strategy_class=None, processing_factor=0.002, **process_kwargs):
    """獨立的數據轉換函數，可被多進程調用"""
    try:
        # 模擬與數據量成正比的處理時間
        processing_time = len(chunk_data) * processing_factor
        time.sleep(processing_time)
        
        # 創建並應用轉換策略
        if strategy_class is None:
            strategy_class = DefaultSalesTransformStrategy
        
        strategy = strategy_class()
        transformed_data = strategy.transform(chunk_data, **process_kwargs)
        
        # 應用自定義轉換函數（如果提供）; 在標準流程後追加操作
        custom_transform = process_kwargs.get('custom_transform')
        if custom_transform and callable(custom_transform):
            transformed_data = custom_transform(transformed_data)
        
        return transformed_data, chunk_index, None  # 返回數據和索引
    except Exception as e:
        # 捕獲詳細的錯誤信息，包括完整的堆疊追蹤
        error_info = {
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc(),
            'chunk_index': chunk_index,
            'chunk_shape': chunk_data.shape if isinstance(chunk_data, pd.DataFrame) else None,
            'chunk_columns': list(chunk_data.columns) if isinstance(chunk_data, pd.DataFrame) else None,
            'strategy': strategy_class.__name__ if strategy_class else None
        }
        return None, chunk_index, error_info  # 返回詳細錯誤信息


# Transform 階段處理器
class TransformProcessor(ETLProcessor[pd.DataFrame, pd.DataFrame]):
    """
    數據轉換處理器
    負責數據的轉換和處理
    """
    def __init__(self, context: ETLContext = None, processing_factor: float = 0.002,
                 strategy_class: Type[TransformStrategy] = None):
        """
        初始化轉換處理器
        
        參數:
            context: ETL上下文
            processing_factor: 處理因子，用於模擬處理時間
            strategy_class: 轉換策略類別
        """
        super().__init__(context)
        self.processing_factor = processing_factor
        self.strategy_class = strategy_class or DefaultSalesTransformStrategy
    
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
            # 使用傳入的策略類別或默認類別
            strategy_class = kwargs.pop('strategy_class', self.strategy_class)
            
            # 使用工作函數處理，代碼重用
            result, _, error = _transform_chunk_worker(
                df_chunk, 
                0,  # 只是一個佔位符索引
                strategy_class,
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
        
        # 從參數中獲取策略類別或使用默認策略
        strategy_class = kwargs.pop('strategy_class', self.strategy_class)
        
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
                    strategy_class,
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
                    chunk_result, idx, error_info = future.result()
                    if error_info is None:
                        results.append(chunk_result)
                        with log_lock:
                            logger.info(f"完成分區 {idx+1}/{num_partitions} 的轉換")
                    else:
                        with log_lock:
                            logger.error(f"處理分區 {idx+1}/{num_partitions} 時出錯:")
                            logger.error(f"錯誤類型: {error_info['error_type']}")
                            logger.error(f"錯誤訊息: {error_info['error_message']}")
                            logger.error(f"堆疊追蹤:\n{error_info['traceback']}")
                            if 'chunk_shape' in error_info and error_info['chunk_shape']:
                                logger.error(f"分區資料形狀: {error_info['chunk_shape']}")
                            if 'chunk_columns' in error_info and error_info['chunk_columns']:
                                logger.error(f"分區資料欄位: {error_info['chunk_columns']}")
                        
                        # 記錄錯誤但在主進程中處理
                        if self.context and hasattr(self.context, 'stats'):
                            self.context.stats.record_error(error_info['error_type'])
                except Exception as e:
                    with log_lock:
                        logger.error(f"處理分區 {chunk_idx+1}/{num_partitions} 時出錯: {str(e)}")
                        logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
        
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