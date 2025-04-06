# ETL併發處理框架

[![Python版本](https://img.shields.io/badge/python-3.8%2B-blue)]()
[![授權](https://img.shields.io/badge/license-MIT-green)]()

高效能、可擴展的ETL（Extract-Transform-Load）併發處理框架，專為處理會計底稿和數據分析而設計。透過並行處理技術，大幅提升數據處理效能，同時保持代碼清晰和可維護性。

## ✨ 特色功能

- **高效併發處理**：利用多線程和多進程技術，實現I/O和CPU密集型任務的最佳性能
- **自適應資源優化**：基於數據大小和系統資源動態調整處理參數
- **靈活的處理策略**：支持可切換的轉換邏輯，適配不同業務場景
- **多格式輸出支持**：內建支持CSV、Excel、Parquet等多種數據格式
- **完整的ETL流程**：從數據提取、轉換到聚合報表生成，一站式解決方案
- **穩健的錯誤處理**：完善的錯誤捕獲與日誌記錄機制
- **全面的性能監控**：自動收集處理性能統計，支持性能比較分析

## 🛠️ 安裝與依賴

### 環境要求
- Python 3.8+
- pandas
- numpy
- openpyxl (用於Excel文件處理)
- pyarrow (用於Parquet文件處理)
- psutil (用於系統資源監控)

### 安裝

1. 複製儲存庫
```bash
git clone https://github.com/yourusername/etl-concurrent-framework.git
cd etl-concurrent-framework
```

2. 使用pipenv設置環境（推薦）
```bash
pipenv install
pipenv shell
```

## 📋 快速開始

### 基本用法

```python
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor
from processors.load import LoadProcessor
from orchestration.orchestrator import ETLOrchestratorWithOutput

# 初始化上下文和處理器
context = ETLContext()
extractor = ExtractProcessor(context) 
transformer = TransformProcessor(context)
loader = LoadProcessor(context)

# 創建協調器
orchestrator = ETLOrchestratorWithOutput(
    context=context,
    extractor=extractor,
    transformer=transformer,
    loader=loader
)

# 執行ETL流程
success = orchestrator.run(
    data_dir='data/raw',  # 數據目錄
    file_pattern='vouchers_*.xlsx',  # 文件匹配模式
    processing_mode=ProcessingMode.CONCURRENT,  # 並行處理模式
    extract_params={'max_workers': 4},  # 提取參數
    transform_params={'max_workers': 4},  # 轉換參數
    load_params={'max_workers': 3},  # 載入參數
    reports=[  # 報表配置
        {
            'dimension': 'account_balance',
            'filename': 'data/reports/balance_sheet.xlsx'
        }
    ]
)
```

### 併發模式選項

```python
# 串行處理模式（適合小數據量或調試）
orchestrator.run(
    processing_mode=ProcessingMode.SEQUENTIAL,
    # 其他參數...
)

# 並行處理模式（適合大數據量生產環境）
orchestrator.run(
    processing_mode=ProcessingMode.CONCURRENT,
    # 其他參數...
)
```

### 使用自動優化

```python
# 啟用自動參數優化（根據數據量和系統資源）
orchestrator.run(
    # 其他參數...
    enable_auto_optimization=True
)
```

## 🏗️ 框架架構

### 核心組件

- **ETLContext**: 管理共享資源和狀態的上下文
- **ExtractProcessor**: 負責從各種來源讀取數據
- **TransformProcessor**: 負責數據轉換和處理
- **LoadProcessor**: 負責聚合和報表生成
- **OutputProcessor**: 負責數據輸出（無聚合邏輯）
- **ETLOrchestrator**: 協調整個ETL流程的執行
- **ResourceManager**: 監控系統資源和優化處理參數

### 處理策略

框架使用策略模式實現可切換的數據轉換邏輯:

- **DefaultSalesTransformStrategy**: 銷售數據轉換邏輯
- **AccountingTransformStrategy**: 會計數據轉換邏輯
- 可自行擴展實現 `TransformStrategy` 接口

```python
# 自定義轉換策略示例
class MyCustomStrategy(TransformStrategy):
    def transform(self, df, **kwargs):
        # 自定義轉換邏輯
        return transformed_df

# 使用自定義策略
transformer = TransformProcessor(
    context=context,
    strategy_class=MyCustomStrategy
)
```

## 🔧 進階用法

### 定制輸出格式

```python
# 定義多種輸出格式
output_configs = [
    # CSV輸出
    {
        'filename': 'data/processed/accounts.csv',
        'params': {
            'encoding': 'utf-8-sig',
            'index': False
        }
    },
    # Excel輸出
    {
        'filename': 'data/processed/accounts.xlsx',
        'params': {
            'sheet_name': '科目表',
            'freeze_panes': (1, 0)
        }
    },
    # Parquet輸出
    {
        'filename': 'data/processed/accounts.parquet',
        'params': {
            'compression': 'snappy'
        }
    }
]

# 在ETL流程中使用
orchestrator.run(
    # 其他參數...
    output_configs=output_configs,
    output_params={'max_workers': 3}
)
```

### 自定義報表設計

```python
# 定義複雜報表
reports = [
    # 科目餘額表
    {
        'dimension': 'account_balance',
        'filename': 'data/reports/account_balance.xlsx',
        'params': {
            'groupby_cols': ['company', 'year', 'month', 'account_code', 'account_name'],
            'agg_dict': {
                'debit_amount': 'sum',
                'credit_amount': 'sum'
            },
            'post_process': lambda df: df.assign(
                balance=df['debit_amount'] - df['credit_amount']
            ),
            'write_params': {
                'sheet_name': '科目餘額表',
                'freeze_panes': (1, 0)
            }
        }
    }
]
```

### 跳過特定階段

```python
# 跳過載入階段，只執行提取、轉換和輸出
orchestrator.run(
    # 其他參數...
    skip_load=True,
    output_configs=output_configs
)

# 跳過轉換階段，直接從提取到輸出
orchestrator.run(
    # 其他參數...
    skip_transform=True,
    skip_load=True,
    output_configs=output_configs
)
```

### 性能比較

```python
from orchestration.performance import PerformanceComparator

# 創建性能比較器
comparator = PerformanceComparator(orchestrator)

# 比較串行和並行模式的性能
comparator.compare(
    extract_params={'max_workers': 4},
    transform_params={'max_workers': 4},
    load_params={'max_workers': 3}
)
```

## 📊 會計數據處理

### 會計底稿處理示例

```python
from accounting_logics import accounting_transform

# 設置會計轉換參數
transform_params = {
    'num_partitions': 4,
    'max_workers': 4,
    'custom_transform': accounting_transform
}

# 定義會計報表
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
            'post_process': lambda df: df.rename(
                columns={'voucher_id': 'voucher_count'}
            )
        }
    },
    # 科目餘額表
    {
        'dimension': 'account_balance',
        'filename': 'data/accounting/reports/account_balance.xlsx',
        'params': {
            'groupby_cols': ['company', 'year', 'month', 'account_code', 'account_name'],
            'agg_dict': {
                'debit_amount': 'sum',
                'credit_amount': 'sum'
            },
            'post_process': lambda df: df.assign(
                balance=df['debit_amount'] - df['credit_amount']
            )
        }
    }
]

# 使用會計轉換策略執行流程
transformer = TransformProcessor(
    context=context,
    strategy_class=AccountingTransformStrategy
)

orchestrator = ETLOrchestratorWithOutput(
    context=context,
    extractor=extractor,
    transformer=transformer,
    loader=loader
)

orchestrator.run(
    data_dir='data/accounting/raw',
    file_pattern='vouchers_*.xlsx',
    processing_mode=ProcessingMode.CONCURRENT,
    transform_params=transform_params,
    reports=accounting_reports
)
```

### 各種使用情境

框架支持多種使用情境：

1. **單純檔案讀取**：快速並行讀取多個文件並合併
2. **檔案讀取與輸出**：讀取後直接以多種格式輸出
3. **數據清洗與轉換**：讀取後進行數據處理
4. **完整ETL流程**：包括讀取、轉換、報表生成和格式輸出的完整流程

完整的使用範例請見 `etl_usage_examples.py` 檔案。

## 🔍 性能優化建議

1. **I/O密集型處理**：
   - 對於提取和輸出階段，增加 `max_workers` 參數值以提高並行度
   - 示例：`extract_params={'max_workers': 8}`

2. **CPU密集型處理**：
   - 對於轉換階段，設置適當的分區數和工作進程數，通常不超過CPU核心數
   - 示例：`transform_params={'num_partitions': 4, 'max_workers': 4}`

3. **資源限制情境**：
   - 在資源受限的環境中，減少 `max_workers` 和 `num_partitions`
   - 啟用 `enable_auto_optimization=True` 讓框架動態調整

4. **大型數據集**：
   - 增加分區數以減少每個工作進程的記憶體使用
   - 示例：`transform_params={'num_partitions': 8}`

## 📄 授權

本專案採用 MIT 授權條款。
