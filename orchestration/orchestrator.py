from typing import List, Dict, Any, Union
import time
import os

import pandas as pd
import numpy as np
import traceback

from utils.logging import get_logger
from core.enums import ProcessingMode
from core.context import ETLContext
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor
from processors.load import LoadProcessor
from processors.output import OutputProcessor
from utils.resource_manager import ResourceManager


logger = get_logger(__name__)

# ETL流程協調器
class ETLOrchestrator:
    """
    ETL流程協調器
    負責協調整個ETL流程的執行
    """
    def __init__(self, 
                 context: ETLContext = None,
                 extractor: ExtractProcessor = None,
                 transformer: TransformProcessor = None,
                 loader: LoadProcessor = None):
        """
        初始化ETL協調器
        
        參數:
            context: ETL上下文
            extractor: 數據提取處理器
            transformer: 數據轉換處理器
            loader: 數據加載處理器
        """
        self.context = context or ETLContext()
        self.extractor = extractor or ExtractProcessor(self.context)
        self.transformer = transformer or TransformProcessor(self.context)
        self.loader = loader or LoadProcessor(self.context)
    
    def run(self, 
            data_dir: str = 'data/raw', 
            file_pattern: str = 'sales_*.csv',
            processing_mode: ProcessingMode = ProcessingMode.CONCURRENT,
            extract_params: Dict[str, Any] = None,
            transform_params: Dict[str, Any] = None,
            load_params: Dict[str, Any] = None,
            reports: List[Dict[str, Any]] = None) -> bool:
        """
        執行完整的ETL流程
        
        參數:
            data_dir: 數據目錄
            file_pattern: 文件匹配模式
            processing_mode: 處理模式 (並行或串行)
            extract_params: 提取階段參數
            transform_params: 轉換階段參數
            load_params: 加載階段參數
            reports: 報表配置
        
        返回:
            ETL流程是否成功
        """
        total_start_time = time.time()
        logger.info(f"======= 開始ETL流程 (模式: {processing_mode.value}) =======")
        
        # 添加安全檢查
        if self.context is None or self.context.stats is None:
            logger.error("無法執行ETL流程: context 或 stats 為 None")
            return False
        
        # 重置統計信息
        try:
            self.context.reset_stats()
        except Exception as e:
            logger.error(f"重置統計信息時出錯: {str(e)}")
        
        # 默認參數初始化
        extract_params = extract_params or {}
        transform_params = transform_params or {}
        load_params = load_params or {}
        
        if reports is None:
            reports = [
                {'dimension': 'store', 'filename': 'data/final/store_monthly_report.csv'},
                {'dimension': 'product', 'filename': 'data/final/product_monthly_report.csv'},
                {'dimension': 'date', 'filename': 'data/final/daily_sales_report.csv'}
            ]
        
        try:
            # 1. 提取階段
            logger.info("=== 提取階段開始 ===")
            
            # 獲取所有銷售數據文件
            import glob
            file_paths = glob.glob(f"{data_dir}/{file_pattern}")
            
            if processing_mode == ProcessingMode.CONCURRENT:
                extracted_data = self.extractor.process_concurrent(file_paths, **extract_params)
            else:
                # 串行處理
                all_data = []
                for file_path in file_paths:
                    df = self.extractor.process(file_path, **extract_params)
                    all_data.append(df)
                
                if all_data:
                    extracted_data = pd.concat(all_data, ignore_index=True)
                else:
                    extracted_data = pd.DataFrame()
            
            if len(extracted_data) == 0:
                logger.error("提取階段失敗，終止ETL流程")
                return False
            
            # 2. 轉換階段
            logger.info("=== 轉換階段開始 ===")
            
            if processing_mode == ProcessingMode.CONCURRENT:
                transformed_data = self.transformer.process_concurrent(extracted_data, **transform_params)
            else:
                # 串行處理 - 但仍分塊處理以保持一致性
                num_partitions = transform_params.get('num_partitions', 4)
                df_split = np.array_split(extracted_data, num_partitions)
                
                transformed_chunks = []
                for i, chunk in enumerate(df_split):
                    logger.info(f"處理分區 {i+1}/{num_partitions}")
                    transformed_chunk = self.transformer.process(chunk, **transform_params)
                    transformed_chunks.append(transformed_chunk)
                
                if transformed_chunks:
                    transformed_data = pd.concat(transformed_chunks, ignore_index=True)
                else:
                    transformed_data = pd.DataFrame()
            
            if len(transformed_data) == 0:
                logger.error("轉換階段失敗，終止ETL流程")
                return False
            
            # 保存轉換後的數據
            
            # 3. 載入階段
            logger.info("=== 載入階段開始 ===")
            
            if processing_mode == ProcessingMode.CONCURRENT:
                results = self.loader.process_concurrent(transformed_data, reports, **load_params)
                load_success = any(results.values())
            else:
                # 串行處理
                results = {}
                for report in reports:
                    dimension = report['dimension']
                    filename = report.get('filename', f"data/final/{dimension}_report.csv")
                    
                    # 合併報表特定參數和全局參數
                    report_params = {**load_params}
                    if 'params' in report:
                        report_params.update(report['params'])
                    
                    result = self.loader.process(transformed_data, dimension, filename, **report_params)
                    results[dimension] = result
                
                load_success = any(results.values())
            
            if not load_success:
                logger.error("載入階段失敗")
                return False
            
            total_time = time.time() - total_start_time
            logger.info(f"======= ETL流程成功完成，總耗時: {total_time:.2f}秒 =======")
            return True
            
        except Exception as e:
            logger.error(f"ETL流程執行出錯: {str(e)}")
            return False


class ETLOrchestratorWithOutput(ETLOrchestrator):
    """
    擴展ETL協調器，增加純輸出功能
    不修改原有ETL流程，只增加輸出階段
    """
    def __init__(self, 
                 context: ETLContext = None,
                 extractor: ExtractProcessor = None,
                 transformer: TransformProcessor = None,
                 loader: LoadProcessor = None,
                 outputter: OutputProcessor = None):
        """
        初始化擴展ETL協調器
        
        參數:
            context: ETL上下文
            extractor: 數據提取處理器
            transformer: 數據轉換處理器
            loader: 數據加載處理器
            outputter: 數據輸出處理器（純輸出，無聚合邏輯）
        """
        super().__init__(context, extractor, transformer, loader)
        self.outputter = outputter or OutputProcessor(self.context)
        self.resource_manager = ResourceManager()

    def calculate_optimal_partitions(self, df):
        """根據資料大小動態計算最佳分區數"""
        memory_usage = df.memory_usage(deep=True).sum()
        logger.info(f"根據資料大小({memory_usage / (1024*1024):.2f}MB)動態調整分區數")
        # 每個分區理想大小約50-100MB
        ideal_partition_size = 75 * 1024 * 1024  # 75MB
        optimal_count = max(1, int(memory_usage / ideal_partition_size))
        
        # 不超過CPU核心數且至少為1
        cpu_count = os.cpu_count() or 4
        return max(1, min(cpu_count, optimal_count))

    def optimize_processing_parameters(self, df, processing_mode):
        """根據資料和系統資源優化處理參數"""
        if processing_mode != ProcessingMode.CONCURRENT:
            return {}, {}, {}
            
        # 獲取資源狀況
        resources = self.resource_manager.resources
        
        # 優化提取階段 (IO密集型)
        extract_workers = self.resource_manager.get_adaptive_workers(
            task_type='io', min_workers=2, max_workers=8
        )
        
        # 優化轉換階段 (CPU密集型)
        optimal_partitions = self.calculate_optimal_partitions(df)
        transform_workers = self.resource_manager.get_adaptive_workers(
            task_type='cpu', min_workers=2, max_workers=optimal_partitions
        )
        
        # 優化載入階段 (IO密集型); 跟output共用
        load_workers = self.resource_manager.get_adaptive_workers(
            task_type='io', min_workers=1, max_workers=5
        )
        
        logger.info(f"系統資源狀態: 記憶體使用率 {resources['memory_used_percent']}%, "
                    f"CPU使用率 {resources['average_cpu']}%")
        logger.info(f"最佳參數設定: 提取工作線程數 {extract_workers}, "
                    f"轉換分區數 {optimal_partitions}, 轉換工作進程數 {transform_workers}, "
                    f"載入/輸出工作線程數 {load_workers}")
        
        return {
            'max_workers': extract_workers
        }, {
            'num_partitions': optimal_partitions,
            'max_workers': transform_workers
        }, {
            'max_workers': load_workers
        }
    
    def run(self, 
            data_dir: str = 'data/raw', 
            file_pattern: Union[str, List[str]] = 'sales_*.csv',
            processing_mode: ProcessingMode = ProcessingMode.CONCURRENT,
            extract_params: Dict[str, Any] = None,
            transform_params: Dict[str, Any] = None,
            load_params: Dict[str, Any] = None,
            reports: List[Dict[str, Any]] = None,
            output_configs: List[Dict[str, Any]] = None,
            output_params: Dict[str, Any] = None,
            skip_load: bool = False, 
            skip_transform: bool = False,
            skip_output: bool = False,
            enable_auto_optimization=True) -> bool:
        """
        執行完整的ETL流程，包括純輸出階段
        
        參數:
            data_dir: 數據目錄
            file_pattern: 文件匹配模式
            processing_mode: 處理模式 (並行或串行)
            extract_params: 提取階段參數
            transform_params: 轉換階段參數
            load_params: 加載階段參數
            reports: 報表配置
            output_configs: 輸出配置列表，用於純輸出階段
            output_params: 輸出階段參數
            skip_load: 是否跳過加載階段，直接進行聚合輸出
            skip_transform: 是否跳過轉換階段
            skip_output: 是否跳過輸出階段
        
        返回:
            ETL流程是否成功
        """
        # 如果不需要純輸出，調用父類的run方法
        if output_configs is None and not skip_load:
            return super().run(
                data_dir=data_dir,
                file_pattern=file_pattern,
                processing_mode=processing_mode,
                extract_params=extract_params,
                transform_params=transform_params,
                load_params=load_params,
                reports=reports
            )
        
        # 否則，執行包含輸出階段的完整流程
        total_start_time = time.time()
        logger.info(f"======= 開始擴展ETL流程 (模式: {processing_mode.value}) =======")
        
        # 添加安全檢查
        if self.context is None or self.context.stats is None:
            logger.error("無法執行ETL流程: context 或 stats 為 None")
            return False
        
        # 重置統計信息
        try:
            self.context.reset_stats()
        except Exception as e:
            logger.error(f"重置統計信息時出錯: {str(e)}")
        
        # 默認參數初始化
        extract_params = extract_params or {}
        transform_params = transform_params or {}
        load_params = load_params or {}
        output_params = output_params or {}

        # 根據系統資源和資料情況自動優化參數
        if enable_auto_optimization and processing_mode == ProcessingMode.CONCURRENT:
            try:
                if isinstance(file_pattern, str):
                    # 取得資料大小來優化參數
                    import glob
                    first_file: List[str] = glob.glob(f"{data_dir}/{file_pattern}")[0]
                    sample_df = self.extractor.process(first_file)
                else:
                    # 取得資料大小來優化參數
                    sample_df = self.extractor.process(file_pattern[0])
                
                opt_extract, opt_transform, opt_load = self.optimize_processing_parameters(
                    sample_df, processing_mode
                )
                
                # 合併自動優化參數與使用者提供的參數
                extract_params = {**opt_extract, **(extract_params or {})}
                transform_params = {**opt_transform, **(transform_params or {})}
                load_params = {**opt_load, **(load_params or {})}
                output_params = {**opt_load, **(output_params or {})}
                
            except Exception as e:
                logger.error(f"自動優化處理參數失敗：{str(e)}")
                logger.error(f"堆疊追蹤：{traceback.format_exc()}")
        
        try:
            # 1. 提取階段
            logger.info("=== 提取階段開始 ===")
            
            if isinstance(file_pattern, str):
                # 獲取所有文件
                import glob
                file_paths: List[str] = glob.glob(f"{data_dir}/{file_pattern}")
            elif isinstance(file_pattern, list):
                file_paths: List[str] = file_pattern
            else:
                raise ValueError("file_pattern 必須是字串或字串列表")
            
            transformed_data = None
            
            if processing_mode == ProcessingMode.CONCURRENT:
                extracted_data = self.extractor.process_concurrent(file_paths, **extract_params)
            else:
                # 串行處理
                all_data = []
                for file_path in file_paths:
                    df = self.extractor.process(file_path, **extract_params)
                    all_data.append(df)
                
                if all_data:
                    extracted_data = pd.concat(all_data, ignore_index=True)
                else:
                    extracted_data = pd.DataFrame()
            
            if len(extracted_data) == 0:
                logger.error("提取階段失敗，終止ETL流程")
                return False
            
            # 2. 轉換階段
            if not skip_transform:
                logger.info("=== 轉換階段開始 ===")
                
                if processing_mode == ProcessingMode.CONCURRENT:
                    transformed_data = self.transformer.process_concurrent(extracted_data, **transform_params)
                else:
                    # 串行處理 - 直接處理整個資料集
                    logger.info(f"串行處理轉換階段，資料行數: {len(extracted_data)}")
                    transformed_data = self.transformer.process(extracted_data, **transform_params)
                
                if len(transformed_data) == 0:
                    logger.error("轉換階段失敗，終止ETL流程")
                    return False
            
            # 3. 載入階段 (可跳過)
            load_success = True
            if not skip_load and reports:
                logger.info("=== 載入階段開始 ===")
                
                if processing_mode == ProcessingMode.CONCURRENT:
                    results = self.loader.process_concurrent(transformed_data, reports, **load_params)
                    load_success = any(results.values())
                else:
                    # 串行處理
                    results = {}
                    for report in reports:
                        dimension = report['dimension']
                        filename = report.get('filename', f"data/final/{dimension}_report.csv")
                        
                        # 合併報表特定參數和全局參數
                        report_params = {**load_params}
                        if 'params' in report:
                            report_params.update(report['params'])
                        
                        result = self.loader.process(transformed_data, dimension, filename, **report_params)
                        results[dimension] = result
                    
                    load_success = any(results.values())
                
                if not load_success:
                    logger.warning("載入階段失敗，但仍繼續執行輸出階段")
            
            # 4. 輸出階段 (純輸出，無聚合邏輯)
            output_success = True
            if skip_transform or not isinstance(transformed_data, pd.DataFrame):
                pass
            else:
                transformed_data = extracted_data
            if not skip_output and output_configs:
                if output_configs:
                    logger.info("=== 輸出階段開始 ===")
                    
                    if processing_mode == ProcessingMode.CONCURRENT:
                        output_results = self.outputter.process_concurrent(
                            transformed_data, 
                            output_configs, 
                            **output_params
                        )
                        output_success = any(output_results.values())
                    else:
                        # 串行處理
                        output_results = {}
                        for config in output_configs:
                            result = self.outputter.process(
                                transformed_data,
                                config,
                                **output_params
                            )
                            output_results[config.get('filename', 'unknown')] = result
                        
                        output_success = any(output_results.values())
                    
                    if not output_success:
                        logger.error("輸出階段失敗")
            
            total_time = time.time() - total_start_time
            logger.info(f"======= 擴展ETL流程完成，總耗時: {total_time:.2f}秒 =======")
            
            # 根據執行階段評估整體成功與否
            if skip_load and output_configs:
                return output_success  # 只執行輸出階段時，只考慮輸出結果
            elif output_configs:
                return load_success and output_success  # 同時執行載入和輸出，兩者都需成功
            else:
                return load_success  # 沒有輸出配置時，只考慮載入結果
                
        except Exception as e:
            logger.error(f"擴展ETL流程執行出錯: {str(e)}")
            return False