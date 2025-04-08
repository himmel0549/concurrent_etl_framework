#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL並發處理模組使用範例
提供不同使用情境的代碼示例，便於依照需求靈活選用模組
"""

import os
import sys
import pandas as pd
import warnings

# 忽略Pandas的警告信息
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

# 將項目根目錄添加到Python路徑
# 這確保能正確導入相關模組，不論從哪個目錄執行
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 首先初始化日誌系統
from utils.logging import setup_logging, get_logger
setup_logging()  # 設置日誌格式與級別
logger = get_logger(__name__)  # 獲取當前模組的日誌記錄器

# 導入核心組件
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor, AccountingTransformStrategy
from processors.load import LoadProcessor
from processors.output import OutputProcessor
from orchestration.orchestrator import ETLOrchestratorWithOutput
from utils.resource_manager import ResourceManager


#####################################################################
# 情境1: 單純只使用讀取檔案並發模組
#####################################################################
def scenario_extract_only():
    """
    情境1: 只使用並發檔案讀取功能，實現多文件並行讀取並合併
    適用於: 
    - 僅需要快速讀取多個文件並合併
    - 不需要複雜轉換和聚合
    - 作為數據探索或前處理的第一步
    """
    logger.info("===== 情境1: 單純使用讀取檔案並發模組 =====")
    
    # 步驟1: 建立ETL上下文 - 提供資源共享與配置管理
    context = ETLContext()  # 創建ETL上下文，用於在各組件間共享狀態和配置
    resource_manager = ResourceManager()  # 創建資源管理器
    resource_manager.start_monitoring()  # 啟動資源監控
    
    # 步驟2: 建立提取處理器 - 負責從文件中讀取數據
    extractor = ExtractProcessor(
        context=context,  # 傳入上下文
        processing_factor=0  # 設為0表示不模擬額外處理時間
    )
    
    # 步驟3: 設置讀取參數 - 控制並發與讀取選項
    extract_params = {
        'max_workers': 4,  # 並發線程數，根據CPU核心與IO負載調整
        'parse_dates': ['date'],  # 自動將指定列解析為日期類型
        'dtype': {  # 明確指定列的數據類型
            'account_code': str,
            'voucher_id': str
        },
        # 'sheet_name': '會計分錄',  # 指定要讀取的工作表名稱
    }
    
    # 步驟4: 獲取文件列表 - 指定要處理的文件
    data_dir = 'data/accounting/raw'  # 原始數據目錄
    file_pattern = 'vouchers_*.xlsx'  # 文件匹配模式，使用通配符
    
    import glob
    file_paths = glob.glob(f"{data_dir}/{file_pattern}")  # 獲取所有匹配的文件路徑
    
    # 步驟5: 並發讀取文件 - 使用線程池加速IO密集型操作
    logger.info(f"開始並發讀取 {len(file_paths)} 個文件...")
    combined_data = extractor.process_concurrent(
        file_paths=file_paths,  # 要處理的文件列表
        **extract_params  # 展開讀取參數
    )
    
    # 步驟6: 簡單數據概覽 - 查看讀取結果
    logger.info(f"成功讀取數據: {len(combined_data)} 行, {len(combined_data.columns)} 列")
    logger.info(f"數據列: {list(combined_data.columns)}")
    
    # 步驟7: (可選) 保存合併後的數據
    output_path = 'data/accounting/combined_raw_data.csv'
    logger.info(f"保存合併數據到 {output_path}")
    combined_data.to_csv(output_path, index=False, encoding='utf-8-sig')

    resource_manager.stop_monitoring()  # 停止資源監控
    
    return combined_data  # 返回合併後的數據以便後續處理


#####################################################################
# 情境2: 使用讀取檔案並發模組和輸出模組逐一輸出
#####################################################################
def scenario_extract_and_output():
    """
    情境2: 使用並發讀取和輸出模組，實現數據提取後直接輸出為多種格式
    適用於:
    - 需要將數據轉換為多種輸出格式(CSV, Excel, Parquet等)
    - 不需要複雜的數據處理
    - 資料格式轉換與匯出
    """
    logger.info("===== 情境2: 使用讀取檔案並發模組和輸出模組 =====")
    
    # 步驟1: 建立ETL上下文與處理器
    context = ETLContext()  # 創建ETL上下文
    resource_manager = ResourceManager()  # 創建資源管理器
    resource_manager.start_monitoring()  # 啟動資源監控
    
    # 步驟2: 建立提取處理器
    extractor = ExtractProcessor(context=context, processing_factor=0)  # 負責文件讀取
    
    # 步驟3: 建立輸出處理器
    outputter = OutputProcessor(
        context=context,  # 共享上下文
        output_dir='data/accounting/outputs'  # 設置輸出目錄
    )
    
    # 步驟4: 建立簡化版協調器 - 只包含提取和輸出組件
    orchestrator = ETLOrchestratorWithOutput(
        context=context,  # 共享上下文
        extractor=extractor,  # 提取處理器
        outputter=outputter  # 輸出處理器
    )
    
    # 步驟5: 設置提取參數
    extract_params = {
        'max_workers': 4,  # 並發線程數
        'parse_dates': ['date'],  # 自動解析日期列
        'dtype': {'voucher_id': str, 'account_code': str}  # 指定列數據類型
    }
    
    # 步驟6: 定義多種輸出配置 - 將同一數據保存為不同格式
    output_configs = [
        # CSV輸出配置 - 適合通用數據交換
        {
            'filename': 'data/accounting/outputs/all_vouchers.csv',  # 輸出文件路徑
            'params': {  # CSV特定參數
                'encoding': 'utf-8-sig',  # 使用UTF-8 BOM編碼(Excel兼容)
                'index': False,  # 不包含索引列
                'date_format': '%Y-%m-%d'  # 日期格式化
            }
        },
        # Excel輸出配置 - 適合人工查看與分析
        {
            'filename': 'data/accounting/outputs/all_vouchers.xlsx',
            'params': {  # Excel特定參數
                'sheet_name': '會計分錄',  # 工作表名稱
                'freeze_panes': (1, 0),  # 凍結首行
                'index': False  # 不包含索引列
            }
        },
        # Parquet輸出配置 - 適合大數據分析與存儲
        {
            'filename': 'data/accounting/outputs/all_vouchers.parquet',
            'params': {
                'compression': 'snappy'  # 使用Snappy壓縮
            }
        }
    ]
    
    # 步驟7: 設置輸出參數
    output_params = {
        'max_workers': 4,  # 輸出並發線程數
    }
    
    # 步驟8: 執行提取和輸出流程
    logger.info("開始執行提取並多格式輸出...")
    success = orchestrator.run(
        data_dir='data/accounting/raw',  # 數據源目錄
        file_pattern='vouchers_*.xlsx',  # 文件匹配模式
        processing_mode=ProcessingMode.CONCURRENT,  # 使用並發模式
        extract_params=extract_params,  # 提取參數
        skip_load=True,  # 跳過Load階段
        skip_transform=True,  # 跳過Transform階段
        output_configs=output_configs,  # 輸出配置
        output_params=output_params,  # 輸出參數
        enable_auto_optimization=False  # 禁用自動優化
    )

    resource_manager.stop_monitoring()  # 停止資源監控
    
    # 步驟9: 檢查運行結果
    if success:
        logger.info("提取與輸出成功完成!")
    else:
        logger.error("提取與輸出過程出錯!")
    
    return success


#####################################################################
# 情境3: 只用讀取檔案並發模組和Transform模組
#####################################################################
def scenario_extract_and_transform():
    """
    情境3: 使用並發讀取和轉換模組，讀取後進行數據處理但不生成報表
    適用於:
    - 數據清洗與預處理
    - 特徵工程
    - 數據標準化/正規化
    - 資料計算與派生欄位創建
    """
    logger.info("===== 情境3: 使用讀取檔案並發模組和Transform模組 =====")
    
    # 步驟1: 建立ETL上下文與資源管理器
    context = ETLContext()  # 創建ETL上下文
    resource_manager = ResourceManager()  # 創建資源管理器，用於監控系統資源
    resource_manager.start_monitoring()  # 啟動資源監控
    
    # 步驟2: 建立提取和轉換處理器
    extractor = ExtractProcessor(context=context, processing_factor=0)  # 讀取處理器
    
    # 步驟3: 建立轉換處理器，使用會計轉換策略
    transformer = TransformProcessor(
        context=context,  # 共享上下文
        processing_factor=0,  # 不添加額外處理延時
        strategy_class=AccountingTransformStrategy,  # 使用會計數據轉換策略
        resource_manager=resource_manager  # 提供資源管理以優化效能
    )
    
    # 步驟4: 建立簡化版協調器 - 只使用提取和轉換組件
    orchestrator = ETLOrchestratorWithOutput(
        context=context,
        extractor=extractor,
        transformer=transformer
    )
    
    # 步驟5: 設置提取參數
    extract_params = {
        'max_workers': 4,  # 提取並發線程數
        'parse_dates': ['date'],  # 解析日期列
        'dtype': {'voucher_id': str, 'account_code': str}  # 指定列數據類型
    }
    
    # 步驟6: 設置轉換參數，包含自定義轉換邏輯
    from accounting_logics import accounting_transform  # 導入自定義會計轉換函數
    
    transform_params = {
        'num_partitions': 4,  # 數據分區數量，適用於並行處理
        'max_workers': 4,  # 轉換並發線程數
        'custom_transform': accounting_transform,  # 自定義轉換函數
        'save_transformed': True,  # 保存轉換後的數據
        'transformed_path': 'data/accounting/transformed_data.csv'  # 轉換結果保存路徑
    }
    
    # 步驟7: 執行提取和轉換流程
    logger.info("開始執行提取和轉換...")
    success = orchestrator.run(
        data_dir='data/accounting/raw',  # 數據源目錄
        file_pattern='vouchers_*.xlsx',  # 文件匹配模式
        processing_mode=ProcessingMode.CONCURRENT,  # 使用並發模式
        extract_params=extract_params,  # 提取參數
        transform_params=transform_params,  # 轉換參數
        skip_load=True,  # 跳過Load階段
        skip_output=True  # 跳過Output階段
    )
    
    # 步驟8: 清理資源和檢查結果
    resource_manager.stop_monitoring()  # 停止資源監控
    
    if success:
        logger.info("提取和轉換成功完成!")
        # 可以讀取保存的轉換結果
        if os.path.exists(transform_params['transformed_path']):
            transformed_data = pd.read_csv(
                transform_params['transformed_path'], 
                parse_dates=['date']
            )
            logger.info(f"轉換後數據: {len(transformed_data)} 行")
    else:
        logger.error("提取和轉換過程出錯!")
    
    return success


#####################################################################
# 情境4: 用讀取檔案並發模組、Transform和輸出模組
#####################################################################
def scenario_extract_transform_output():
    """
    情境4: 使用讀取、轉換和輸出模組，實現完整的ETL流程但不生成彙總報表
    適用於:
    - 資料前處理工作流
    - 需要計算派生欄位但不需要聚合
    - 數據轉換與格式化後直接輸出
    """
    logger.info("===== 情境4: 使用讀取、轉換和輸出模組 =====")
    
    # 步驟1: 建立ETL上下文與資源管理器
    context = ETLContext()  # 創建ETL上下文
    resource_manager = ResourceManager()  # 創建資源管理器
    resource_manager.start_monitoring()  # 啟動資源監控
    
    # 步驟2: 建立所有需要的處理器
    extractor = ExtractProcessor(context=context, processing_factor=0)  # 讀取處理器
    
    transformer = TransformProcessor(
        context=context,
        processing_factor=0,
        strategy_class=AccountingTransformStrategy,  # 使用會計數據轉換策略
        resource_manager=resource_manager
    )
    
    outputter = OutputProcessor(
        context=context,
        output_dir='data/accounting/processed'  # 設置輸出目錄
    )
    
    # 步驟3: 建立協調器，包含提取、轉換和輸出組件
    orchestrator = ETLOrchestratorWithOutput(
        context=context,
        extractor=extractor,
        transformer=transformer,
        outputter=outputter
    )
    
    # 步驟4: 設置提取參數
    extract_params = {
        'max_workers': 4,
        'parse_dates': ['date'],
        'dtype': {'voucher_id': str, 'account_code': str}
    }
    
    # 步驟5: 設置轉換參數
    from accounting_logics import accounting_transform
    
    transform_params = {
        'num_partitions': 4,
        'max_workers': 4,
        'custom_transform': accounting_transform
    }
    
    # 步驟6: 設置輸出配置
    output_configs = [
        # CSV輸出 - 標準開放格式
        {
            'filename': 'data/accounting/processed/transformed_vouchers.csv',
            'params': {
                'encoding': 'utf-8-sig',
                'index': False
            }
        },
        # Excel輸出 - 方便查看和分析
        {
            'filename': 'data/accounting/processed/transformed_vouchers.xlsx',
            'params': {
                'sheet_name': '處理後會計分錄',
                'freeze_panes': (1, 0),
                'index': False
            }
        }
    ]
    
    # 步驟7: 設置輸出參數
    output_params = {
        'max_workers': 4,  # 輸出並發線程數
    }
    
    # 步驟8: 執行提取、轉換和輸出流程
    logger.info("開始執行提取、轉換和輸出流程...")
    success = orchestrator.run(
        data_dir='data/accounting/raw',
        file_pattern='vouchers_*.xlsx',
        processing_mode=ProcessingMode.CONCURRENT,
        extract_params=extract_params,
        transform_params=transform_params,
        skip_load=True,  # 跳過Load階段，不生成聚合報表
        output_configs=output_configs,
        output_params=output_params
    )
    
    # 步驟9: 清理資源和檢查結果
    resource_manager.stop_monitoring()
    
    if success:
        logger.info("提取、轉換和輸出成功完成!")
    else:
        logger.error("提取、轉換和輸出過程出錯!")
    
    return success


#####################################################################
# 情境5: 使用完整的ETL Pipeline
#####################################################################
def scenario_full_etl_pipeline():
    """
    情境5: 使用完整ETL流程，包括提取、轉換、彙總報表生成和輸出
    適用於:
    - 完整的數據處理與分析流程
    - 需要生成多種彙總報表和輸出格式
    - 生產環境數據處理流程
    """
    logger.info("===== 情境5: 使用完整ETL Pipeline =====")
    
    # 步驟1: 建立ETL上下文與資源管理器
    context = ETLContext()  # 創建ETL上下文
    resource_manager = ResourceManager()  # 創建資源管理器
    resource_manager.start_monitoring()  # 啟動資源監控
    
    # 步驟2: 建立所有處理器
    extractor = ExtractProcessor(context=context, processing_factor=0)
    
    transformer = TransformProcessor(
        context=context, processing_factor=0,
        strategy_class=AccountingTransformStrategy,
        resource_manager=resource_manager
    )
    
    loader = LoadProcessor(
        context=context, 
        output_dir='data/accounting/reports'  # 報表輸出目錄
    )
    
    outputter = OutputProcessor(
        context=context,
        output_dir='data/accounting/processed'  # 處理後數據輸出目錄
    )
    
    # 步驟3: 建立完整的協調器
    orchestrator = ETLOrchestratorWithOutput(
        context=context,
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        outputter=outputter
    )
    
    # 步驟4: 設置提取參數
    extract_params = {
        'max_workers': 4,
        'parse_dates': ['date'],
        'dtype': {'voucher_id': str, 'account_code': str}
    }
    
    # 步驟5: 設置轉換參數
    from accounting_logics import accounting_transform
    
    transform_params = {
        'num_partitions': 4,
        'max_workers': 4,
        'custom_transform': accounting_transform
    }
    
    # 步驟6: 設置載入參數
    load_params = {
        'max_workers': 4  # 報表生成並發線程數
    }
    
    # 步驟7: 定義彙總報表配置
    accounting_reports = [
        # 傳票彙總表 - 按公司、年月和類型彙總
        {
            'dimension': 'voucher_summary',  # 報表維度
            'filename': 'data/accounting/reports/voucher_summary.xlsx',  # 輸出文件
            'params': {  # 報表特定參數
                'groupby_cols': ['company', 'year', 'month', 'type'],  # 分組列
                'agg_dict': {  # 聚合字典
                    'voucher_id': 'count',  # 計算傳票數量
                    'amount': 'sum'  # 計算金額總和
                },
                'post_process': lambda df: df.rename(columns={'voucher_id': 'voucher_count'})  # 後處理函數
            }
        },
        # 科目餘額表 - 展示各科目的借貸餘額
        {
            'dimension': 'account_balance',
            'filename': 'data/accounting/reports/account_balance.xlsx',
            'params': {
                'groupby_cols': ['company', 'year', 'month', 'account_code', 'account_name', 'account_type'],
                'agg_dict': {
                    'debit_amount': 'sum',  # 借方總額
                    'credit_amount': 'sum'  # 貸方總額
                },
                'post_process': lambda df: df.assign(balance=df['debit_amount'] - df['credit_amount'])  # 計算餘額
            }
        },
        # 利潤表數據 - 僅包含利潤表相關科目
        {
            'dimension': 'income_statement',
            'filename': 'data/accounting/reports/income_statement.xlsx',
            'params': {
                'groupby_cols': ['company', 'year', 'month', 'account_type', 'account_code', 
                                 'account_name', 'statement_type'],
                'agg_dict': {
                    'debit_amount': 'sum',
                    'credit_amount': 'sum'
                },
                'post_process': lambda df: (df[df['statement_type'] == '利潤表']
                                            .assign(balance=df['debit_amount'] - df['credit_amount']))
            }
        }
    ]
    
    # 步驟8: 設置輸出配置
    output_configs = [
        # 保存完整處理後的數據 - CSV格式
        {
            'filename': 'data/accounting/processed/all_processed_vouchers.csv',
            'params': {
                'encoding': 'utf-8-sig',
                'index': False
            }
        },
        # Excel格式 - 便於檢視
        {
            'filename': 'data/accounting/processed/all_processed_vouchers.xlsx',
            'params': {
                'sheet_name': '處理後會計分錄',
                'freeze_panes': (1, 0),
                'index': False
            }
        }
    ]
    
    # 步驟9: 設置輸出參數
    output_params = {
        'max_workers': 4,
    }
    
    # 步驟10: 執行完整ETL流程
    logger.info("開始執行完整ETL Pipeline...")
    success = orchestrator.run(
        data_dir='data/accounting/raw',
        file_pattern='vouchers_*.xlsx',
        processing_mode=ProcessingMode.CONCURRENT,  # 使用並發模式
        extract_params=extract_params,  # 提取參數
        transform_params=transform_params,  # 轉換參數
        load_params=load_params,  # 載入參數
        reports=accounting_reports,  # 彙總報表配置
        output_configs=output_configs,  # 輸出配置
        output_params=output_params,  # 輸出參數
        enable_auto_optimization=True  # 啟用自動優化
    )
    
    # 步驟11: 清理資源和總結結果
    resource_manager.stop_monitoring()
    
    if success:
        logger.info("完整ETL Pipeline成功執行完畢!")
        # 打印生成的所有輸出文件
        for report in accounting_reports:
            logger.info(f"生成報表: {report['filename']}")
        for output in output_configs:
            logger.info(f"輸出數據: {output['filename']}")
    else:
        logger.error("ETL Pipeline執行過程出錯!")
    
    return success


#####################################################################
# 主函數
#####################################################################
def main():
    """
    主函數: 提供一個簡單的界面來選擇和運行不同的ETL場景
    """
    # 建立必要的目錄結構
    os.makedirs('data/accounting/raw', exist_ok=True)
    os.makedirs('data/accounting/processed', exist_ok=True)
    os.makedirs('data/accounting/reports', exist_ok=True)
    os.makedirs('data/accounting/outputs', exist_ok=True)
    
    print("\n===== ETL並發處理模組使用範例 =====")
    print("1. 單純只使用讀取檔案並發模組")
    print("2. 使用讀取檔案並發模組和輸出模組")
    print("3. 只用讀取檔案並發模組和Transform")
    print("4. 用讀取檔案並發模組、Transform和輸出模組")
    print("5. 使用完整的ETL Pipeline")
    print("0. 退出")
    
    try:
        choice = int(input("\n請選擇要運行的場景 (0-5): "))
        
        if choice == 1:
            scenario_extract_only()
        elif choice == 2:
            scenario_extract_and_output()
        elif choice == 3:
            scenario_extract_and_transform()
        elif choice == 4:
            scenario_extract_transform_output()
        elif choice == 5:
            scenario_full_etl_pipeline()
        elif choice == 0:
            print("退出程序")
        else:
            print("無效的選擇!")
            
    except ValueError:
        print("請輸入有效的數字!")
    except Exception as e:
        print(f"運行出錯: {str(e)}")


if __name__ == "__main__":
    main()