# 使用指南-銷售版本
# main.py
import os
import sys
# 將項目根目錄添加到 Python 路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import warnings
# 忽略 Pandas 的警告信息
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

# 首先初始化日誌
from utils.logging import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

# 接著初始化核心組件
from core.stats import ETLStats
import config.constants as constants

# 確保 etl_stats 已正確初始化
if constants.etl_stats is None:
    constants.etl_stats = ETLStats()
    logger.info("初始化全局 ETLStats 對象")

# 然後導入其餘模組
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor
from processors.load import LoadProcessor
from orchestration.orchestrator import ETLOrchestrator, ETLOrchestratorWithOutput
from processors.output import OutputProcessor
from orchestration.performance import PerformanceComparator
from generators.sales import SalesDataGenerator


# 設置日誌
setup_logging()
logger = get_logger(__name__)

def main():
    # 創建必要的目錄
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('data/final', exist_ok=True)
    
    # 生成測試數據
    # data_gen = SalesDataGenerator()
    # data_gen.generate(days=90)
    
    # 創建ETL協調器和處理器
    context = ETLContext()
    # 驗證 context.stats 已正確初始化
    if context.stats is None:
        logger.error("ETLContext.stats 為 None，無法繼續")
        return
    else:
        logger.info("ETLContext.stats 正確初始化")
    orchestrator = ETLOrchestratorWithOutput(
        context=context,
        extractor=ExtractProcessor(context),
        transformer=TransformProcessor(context),
        loader=LoadProcessor(context),
        outputter=OutputProcessor(context, output_dir='data/outputs')
    )

    extract_params = {'max_workers': 5}
    transform_params = {
        'num_partitions': 4, 
        'max_workers': 4,
        'save_transformed': True
    }
    load_params = {'max_workers': 3}
    
    # 執行ETL流程
    # orchestrator.run(
    #     processing_mode=ProcessingMode.CONCURRENT,
    #     extract_params=extract_params,
    #     transform_params=transform_params,
    #     load_params=load_params
    # )
    
    # 比較性能
    # comparator = PerformanceComparator(orchestrator)
    # comparator.compare(
    #     extract_params=extract_params,
    #     transform_params=transform_params,
    #     load_params=load_params
    # )
    
    # 展示進階用法
    # ConcurrencyExamples.cancel_example()
    # ConcurrencyExamples.timeout_example()
    # ConcurrencyExamples.callback_example()

    # 定義純輸出配置
    output_configs = [
        {'filename': 'data/outputs/full_data.csv'},
        {'filename': 'data/outputs/full_data.xlsx'},
        {'filename': 'data/outputs/full_data.parquet', 'params': {'compression': 'snappy'}}
    ]
    output_params = {'max_workers': 3}

    # 定義報表配置
    reports = [
        # 按門市彙總報表
        {
            'dimension': 'store', 
            'filename': 'data/final/store_monthly.csv',
            'params': {
                'groupby_cols': ['store_id', 'region', 'year', 'month'],
                'agg_dict': {
                    'revenue': 'sum',
                    'quantity': 'sum',
                    'profit': 'sum'
                },
                'post_process': add_kpi_metrics  # 增加KPI指標
            }
        },
        # 按商品彙總報表
        {
            'dimension': 'product',
            'filename': 'data/final/product_report.xlsx',
            'params': {
                'groupby_cols': ['product_id', 'category', 'year', 'month'],
                'agg_dict': {
                    'revenue': 'sum',
                    'profit_margin': 'mean'
                },
                'write_params': {
                    'sheet_name': '產品業績'
                }
            }
        },
        # 自定義維度報表
        {
            'dimension': 'custom',
            'filename': 'data/final/custom_report.csv',
            'params': {
                'groupby_cols': ['region', 'category', 'year', 'month'],
                'agg_dict': {'revenue': 'sum', 'profit': 'sum'}
            }
        }
    ]

    # 執行帶有輸出階段的ETL流程
    success = orchestrator.run(
        processing_mode=ProcessingMode.CONCURRENT,
        extract_params=extract_params,
        transform_params=transform_params,
        load_params=load_params,
        reports=accounting_reports,
        output_configs=output_configs,
        output_params=output_params
    )

    # 或者只執行提取、轉換和輸出，跳過標準載入階段
    # output_only = orchestrator.run(
    #     processing_mode=ProcessingMode.CONCURRENT,
    #     extract_params=extract_params,
    #     transform_params=transform_params,
    #     skip_load=True,
    #     output_configs=output_configs,
    #     output_params=output_params
    # )
if __name__ == "__main__":
    main()