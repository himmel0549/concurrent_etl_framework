# 使用指南-會計版本
# main_accounting.py
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from utils.logging import setup_logging, get_logger
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor, AccountingTransformStrategy
from processors.load import LoadProcessor
from processors.output import OutputProcessor
from orchestration.orchestrator import ETLOrchestratorWithOutput
from generators.accounting_data import AccountingDataGenerator
from utils.resource_manager import ResourceManager

# 設置日誌
setup_logging()
logger = get_logger(__name__)

def main():
    # 創建必要的目錄
    for dir_path in ['data/accounting/raw', 'data/accounting/processed', 'data/accounting/reports']:
        os.makedirs(dir_path, exist_ok=True)
    
    # 1. 生成會計測試資料
    # logger.info("開始生成會計測試資料...")
    # data_gen = AccountingDataGenerator()
    # data_gen.generate(months=3, transactions_per_day=20)

    # 創建資源管理器
    resource_manager = ResourceManager()
    resource_manager.start_monitoring()
    
    # 2. 創建ETL組件
    context = ETLContext()
    extractor = ExtractProcessor(context, processing_factor=0)  # 實際應用中不延遲
    transformer = TransformProcessor(context, processing_factor=0, 
                                     strategy_class=AccountingTransformStrategy, 
                                     resource_manager=resource_manager)  # 實際應用中不延遲
    loader = LoadProcessor(context, output_dir='data/accounting/reports')
    outputter = OutputProcessor(context, output_dir='data/accounting/processed')
    
    # 3. 創建協調器
    orchestrator = ETLOrchestratorWithOutput(
        context=context,
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        outputter=outputter
    )
    
    # 4. 定義處理參數
    # Extract參數 - 配置xlsx讀取選項
    extract_params = {
        'max_workers': 3,  # 3個並行線程
        'parse_dates': ['date'],  # 自動解析日期
        'dtype': {
            'voucher_id': str,
            'account_code': str
        }
    }
    
    # Transform參數 - 配置會計資料處理選項
    from accounting_logics import accounting_transform
    
    transform_params = {
        'num_partitions': 4,  # 分區數量，預設根據數據量調整
        'max_workers': 4,     # 並行線程，預設根據系統負載調整
        'custom_transform': accounting_transform
    }
    
    # Load參數 - 配置報表生成選項
    load_params = {
        'max_workers': 3
    }
    
    # 5. 定義報表; only for load module
    # 這裡的報表是針對會計資料的彙總(aggregation)報表，並不是ETL的最終輸出
    accounting_reports = [
        # 傳票彙總表
        {
            'dimension': 'voucher_summary',
            'filename': 'data/accounting/reports/voucher_summary.xlsx',
            'params': {
                'groupby_cols': ['company', 'year', 'month', 'type'],
                'agg_dict': {
                    'voucher_id': 'count',
                    'amount': 'sum'
                },
                'post_process': lambda df: df.rename(columns={'voucher_id': 'voucher_count'})
            }
        },
        # 科目餘額表
        # {
        #     'dimension': 'account_balance',
        #     'filename': 'data/accounting/reports/account_balance.xlsx',
        #     'params': {
        #         'groupby_cols': ['company', 'year', 'month', 'account_code', 'account_name', 'account_type'],
        #         'agg_dict': {
        #             'debit_amount': 'sum',  # 使用預計算欄位
        #             'credit_amount': 'sum'  # 使用預計算欄位
        #         },
        #         'post_process': lambda df: df.assign(balance=df['debit_amount'] - df['credit_amount'])
        #     }
        # },
        # # 利潤表
        # {
        #     'dimension': 'income_statement',
        #     'filename': 'data/accounting/reports/income_statement.xlsx',
        #     'params': {
        #         'groupby_cols': ['company', 'year', 'month', 'account_type', 'account_code', 
        #                          'account_name', 'statement_type'],
        #         'agg_dict': {
        #             'debit_amount': 'sum',  # 使用預計算欄位
        #             'credit_amount': 'sum'  # 使用預計算欄位
        #         },
        #         'post_process': lambda df: (df[df['statement_type'] == '利潤表']
        #                                     .assign(balance=df['debit_amount'] - df['credit_amount']))
        #     }
        # }
    ]
    
    # 6. 定義輸出格式
    output_configs = [
        # 保存完整轉換後的數據 - CSV格式
        {
            'filename': 'data/accounting/processed/all_vouchers.csv',
            'params': {
                'encoding': 'utf-8-sig',
                'index': False
            }
        },
        # Excel格式 - 適合查看
        {
            'filename': 'data/accounting/processed/all_vouchers.xlsx',
            'params': {
                'sheet_name': '會計分錄',
                'freeze_panes': (1, 0),
                'index': False
            }
        },
        # Parquet格式 - 壓縮儲存，適合大數據分析
        {
            'filename': 'data/accounting/processed/all_vouchers.parquet',
            'params': {
                'compression': 'snappy'
            }
        }
    ]
    
    output_params = {
        'max_workers': 3,
    }
    
    # 7. 執行ETL流程
    logger.info("開始執行會計ETL流程...")
    success = orchestrator.run(
        data_dir='data/accounting/raw',
        # file_pattern='entries_*.xlsx',  # 只處理會計分錄文件
        file_pattern='vouchers_*.xlsx',  # 只處理傳票文件
        # file_pattern=['data/accounting/raw/vouchers_A.xlsx', 'data/accounting/raw/vouchers_B.xlsx'],
        processing_mode=ProcessingMode.CONCURRENT,
        # processing_mode=ProcessingMode.SEQUENTIAL,
        extract_params=extract_params,
        transform_params=transform_params,
        skip_load=False,
        load_params=load_params,
        reports=accounting_reports,
        output_configs=output_configs,
        output_params=output_params,
        enable_auto_optimization=False
    )
    
    if success:
        logger.info("會計ETL流程成功完成!")
    else:
        logger.error("會計ETL流程執行失敗!")

    # 資源清理
    resource_manager.stop_monitoring()

if __name__ == "__main__":
    main()