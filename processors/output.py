# processors/output.py
from typing import List, Dict, Any
import concurrent.futures
import os
import time
import pandas as pd

from utils.logging import get_logger
from core.interfaces import ETLProcessor
from core.context import ETLContext
from utils.file_lock_manager import file_lock_manager
from utils.file_utils import detect_file_format  # 引入檔案格式檢測函數


logger = get_logger(__name__)

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
        try:
            filename = output_config.get('filename')
            if not filename:
                raise ValueError("必須提供輸出文件名")
                
            # 確保目錄存在
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            logger.info(f"開始輸出文件: {filename}")
            
            # 使用檔案格式檢測函數
            format_info = detect_file_format(filename)
            writer_method = format_info['writer']
            base_params = format_info['params'].copy()
            
            # 合併參數：先是基本參數，然後是全局參數，最後是特定格式參數
            method_specific_params = kwargs.get(f'{writer_method}_params', {})
            output_specific_params = output_config.get('params', {})
            
            # 組合所有參數
            all_params = {**base_params}
            all_params.update(kwargs.get('common_params', {}))
            all_params.update(method_specific_params)
            all_params.update(output_specific_params)
            
            # 對於Excel和CSV，通常不需要索引
            if writer_method in ['to_csv', 'to_excel'] and 'index' not in all_params:
                all_params['index'] = False
            
            # 線程安全的文件寫入
            with file_lock_manager.get_lock(filename):
                # 動態調用適當的寫入方法
                writer = getattr(df, writer_method)
                writer(filename, **all_params)
            
            logger.info(f"完成輸出文件: {filename}, 記錄數: {len(df)}")
            
            # 更新統計信息
            if self.context and hasattr(self.context, 'stats'):
                self.context.stats.file_processed(filename, len(df))
                
            return True
            
        except Exception as e:
            error_type = type(e).__name__
            if self.context and hasattr(self.context, 'stats'):
                self.context.stats.record_error(error_type)
            logger.error(f"輸出文件失敗 {filename}: {str(e)}")
            return False
    
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
            logger.error("無法輸出: 輸入數據為空")
            return {}
        
        start_time = time.time()
        logger.info(f"開始並行輸出 {len(output_configs)} 個文件")
        
        results = {}
        
        # 使用ThreadPoolExecutor並行輸出 (I/O密集型)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交輸出任務
            future_to_output = {}
            for i, output_config in enumerate(output_configs):
                # 處理默認文件名
                if 'filename' not in output_config:
                    output_config['filename'] = f"{self.output_dir}/output_{i}.csv"
                
                # 確保目錄存在
                os.makedirs(os.path.dirname(output_config['filename']), exist_ok=True)
                
                future = executor.submit(
                    self.process, 
                    df, 
                    output_config, 
                    **kwargs
                )
                future_to_output[future] = output_config['filename']
            
            # 監控任務完成情況
            for future in concurrent.futures.as_completed(future_to_output):
                filename = future_to_output[future]
                try:
                    result = future.result()
                    results[filename] = result
                except Exception as e:
                    logger.error(f"輸出 {filename} 時出錯: {str(e)}")
                    results[filename] = False
        
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"輸出階段完成, 成功輸出文件數: {success_count}/{len(output_configs)}, "
                    f"耗時: {time.time() - start_time:.2f}秒")
        
        return results