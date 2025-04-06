from typing import List, Dict, Any
import concurrent.futures
import os
import time

import pandas as pd

from utils.logging import get_logger
from core.interfaces import ETLProcessor
from core.context import ETLContext
from config.constants import log_lock, file_lock


logger = get_logger(__name__)

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
        try:
            with log_lock:
                logger.info(f"開始處理維度: {dimension}")
            
            # 根據維度選擇分析方式
            if dimension == 'store':
                # 按門市匯總
                agg_df = df.groupby(['store_id', 'store_name', 'region', 'year', 'month']).agg({
                    'revenue': 'sum',
                    'quantity': 'sum',
                    'profit': 'sum',
                    'discount_amount': 'sum',
                    'transaction_id': 'nunique'  # 交易次數
                }).reset_index()
                agg_df.rename(columns={'transaction_id': 'transaction_count'}, inplace=True)
                
            elif dimension == 'product':
                # 按產品匯總
                agg_df = df.groupby(['product_id', 'product_name', 'category', 'year', 'month']).agg({
                    'revenue': 'sum',
                    'quantity': 'sum',
                    'profit': 'sum',
                    'discount_amount': 'sum'
                }).reset_index()
            
            elif dimension == 'date':
                # 按日期匯總
                agg_df = df.groupby(['year', 'month', 'day', 'weekday']).agg({
                    'revenue': 'sum',
                    'quantity': 'sum',
                    'profit': 'sum',
                    'discount_amount': 'sum',
                    'transaction_id': 'nunique'  # 交易次數
                }).reset_index()
                agg_df.rename(columns={'transaction_id': 'transaction_count'}, inplace=True)
            
            # 允許自定義維度
            elif 'groupby_cols' in kwargs and 'agg_dict' in kwargs:
                groupby_cols = kwargs['groupby_cols']
                agg_dict = kwargs['agg_dict']
                agg_df = df.groupby(groupby_cols).agg(agg_dict).reset_index()
            
            else:
                with log_lock:
                    logger.error(f"未知的分析維度: {dimension}")
                return False
            
            # 應用後處理函數
            post_process = kwargs.get('post_process')
            if post_process and callable(post_process):
                agg_df = post_process(agg_df)
            
            # 線程安全的文件寫入
            with file_lock:
                # 儲存結果
                if 'write_params' in kwargs:
                    # 自定義寫入參數
                    if os.path.splitext(filename)[1] == '.xlsx':
                        agg_df.to_excel(filename, **kwargs['write_params'])
                    else:
                        agg_df.to_csv(filename, **kwargs['write_params'])
                else:
                    if os.path.splitext(filename)[1] == '.xlsx':
                        agg_df.to_excel(filename, index=False)
                    else:
                        agg_df.to_csv(filename, index=False)
            
            with log_lock:
                logger.info(f"完成處理維度: {dimension}, 記錄數: {len(agg_df)}")
            return True
            
        except Exception as e:
            error_type = type(e).__name__
            self.context.stats.record_error(error_type)
            with log_lock:
                logger.error(f"處理維度 {dimension} 時發生錯誤: {str(e)}")
            return False
    
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
            with log_lock:
                logger.error("無法載入: 輸入數據為空")
            return {}
        
        start_time = time.time()
        with log_lock:
            logger.info("開始資料加載和報表生成")
        
        results = {}
        
        # 使用ThreadPoolExecutor生成報表 (I/O密集型)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交報表生成任務
            future_to_report = {}
            for report in reports:
                dimension = report['dimension']
                filename = report.get('filename', f"{self.output_dir}/{dimension}_report.csv")
                
                # 確保目錄存在
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                
                # 合併報表特定參數和全局參數
                report_params = {**kwargs}
                if 'params' in report:
                    report_params.update(report['params'])
                
                future = executor.submit(
                    self.process, 
                    df, 
                    dimension, 
                    filename, 
                    **report_params
                )
                future_to_report[future] = dimension
            
            # 監控任務完成情況
            for future in concurrent.futures.as_completed(future_to_report):
                dimension = future_to_report[future]
                try:
                    result = future.result()
                    results[dimension] = result
                except Exception as e:
                    with log_lock:
                        logger.error(f"生成 {dimension} 報表時出錯: {str(e)}")
                    results[dimension] = False
        
        success_count = sum(1 for success in results.values() if success)
        with log_lock:
            logger.info(f"載入階段完成, 成功生成報表數: {success_count}/{len(reports)}, "
                        f"耗時: {time.time() - start_time:.2f}秒")
        
        return results