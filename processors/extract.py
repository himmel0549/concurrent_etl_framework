from typing import List
import concurrent.futures
import time

import pandas as pd

from utils.logging import get_logger
from utils.file_utils import detect_file_format
from core.interfaces import ETLProcessor
from core.context import ETLContext
from config.constants import log_lock, file_lock


logger = get_logger(__name__)

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
    
    def process(self, file_info: str, **kwargs) -> pd.DataFrame:
        """
        處理單個文件的提取
        
        參數:
            file_info: 可以是字串路徑或包含路徑和格式資訊的字典
            **kwargs: 傳遞給 pd.read_xxx 的額外參數
        
        返回:
            提取的DataFrame
        """
        try:
            if isinstance(file_info, str):
                file_path = file_info
            else:
                file_path = file_info['path']
            with log_lock:
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
            
            # 更新統計信息
            self.context.stats.file_processed(file_path, rows)
            
            with log_lock:
                logger.info(f"完成讀取文件: {file_path}, 記錄數: {len(df)}, 處理時間: {processing_time:.2f}秒")
            
            return df
        except Exception as e:
            error_type = type(e).__name__
            self.context.stats.record_error(error_type)
            with log_lock:
                logger.error(f"讀取文件 {file_path} 時發生錯誤: {str(e)}")
            raise
    
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
        start_time = time.time()
        all_data = []
        
        # 使用ThreadPoolExecutor並行讀取文件（I/O密集型操作適合多線程）
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有文件讀取任務
            future_to_file = {
                executor.submit(self.process, file_path, **kwargs): file_path 
                for file_path in file_paths
            }
            
            # 收集結果 - 使用as_completed可以讓結果盡快處理
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    data = future.result()
                    all_data.append(data)
                except Exception as e:
                    with log_lock:
                        logger.error(f"處理文件 {file_path} 時出錯: {str(e)}")
        
        # 合併所有數據
        if all_data:
            with log_lock:
                logger.info(f"提取階段完成, 成功文件數: {len(all_data)}/{len(file_paths)}, "
                            f"耗時: {time.time() - start_time:.2f}秒")
            return pd.concat(all_data, ignore_index=True)
        else:
            with log_lock:
                logger.error("提取階段失敗: 沒有成功讀取任何文件")
            return pd.DataFrame()  # 返回空DataFrame而不是None，保持一致性