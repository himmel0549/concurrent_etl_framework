# ETL併發處理框架

[![Python版本](https://img.shields.io/badge/python-3.8%2B-blue)]()
[![授權](https://img.shields.io/badge/license-MIT-green)]()

高效能、可擴展的ETL（Extract-Transform-Load）併發處理框架，專為處理會計底稿和數據分析而設計。利用多線程和多進程技術，大幅提升數據處理效能，同時保持代碼清晰和可維護性。

## ✨ 特色功能

- **高效併發處理**：針對I/O和CPU密集型任務分別採用多線程和多進程，實現最佳性能
- **自適應資源優化**：基於數據大小和系統資源動態調整並發參數
- **增強錯誤恢復**：完整的錯誤追蹤與異常處理，確保ETL流程穩定性
- **線程與進程安全**：細粒度鎖機制和線程本地存儲保證併發安全
- **高級日誌系統**：非阻塞式日誌處理和自動堆疊追蹤功能
- **靈活的處理策略**：可切換的轉換邏輯，支持不同業務場景
- **多格式輸出支持**：內建支持CSV、Excel、Parquet等多種數據格式
- **全面的性能監控**：自動收集資源使用與處理性能統計

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

2. 安裝依賴套件
```bash
pip install -r requirements.txt
```

## 📋 快速開始

### 基本用法

```python
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor
from processors.load import LoadProcessor
from orchestration.orchestrator import ETLOrchestratorWithOutput
from utils.resource_manager import ResourceManager

# 初始化資源管理器
resource_manager = ResourceManager()
resource_manager.start_monitoring()

# 初始化上下文與處理器
context = ETLContext()
extractor = ExtractProcessor(context) 
transformer = TransformProcessor(
    context=context,
    resource_manager=resource_manager
)
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
            'filename': 'data/reports/balance_sheet.xlsx',
            'params': {
                'groupby_cols': ['company', 'year', 'month', 'account_code'],
                'agg_dict': {
                    'debit_amount': 'sum',
                    'credit_amount': 'sum'
                },
                'post_process': lambda df: df.assign(
                    balance=df['debit_amount'] - df['credit_amount']
                )
            }
        }
    ],
    enable_auto_optimization=True  # 啟用自動優化
)

# 清理資源
resource_manager.stop_monitoring()
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

## 🏗️ 框架架構

### 核心組件

- **ETLContext**: 管理共享資源和狀態的上下文
- **ExtractProcessor**: 負責從各種來源讀取數據（使用多線程）
- **TransformProcessor**: 負責數據轉換和處理（使用多進程）
- **LoadProcessor**: 負責聚合和報表生成（使用多線程）
- **OutputProcessor**: 負責數據輸出（無聚合邏輯，使用多線程）
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

## 🧠 並發安全與錯誤處理

### 併發安全機制

框架採用多層次的併發安全機制:

```python
# 文件鎖 - 防止文件競爭
with file_lock_manager.get_lock(filename):
    df.to_excel(filename, **params)

# 統計信息更新鎖 - 保護共享狀態
with self._stats_lock:
    self.context.stats.file_processed(file_path, rows)

# 線程本地存儲 - 隔離線程數據
_thread_local.current_task = {'config': output_config}
```

### 錯誤恢復機制

完整的錯誤捕獲與記錄:

```python
try:
    # 處理代碼
except Exception as e:
    logger.error(f"處理失敗: {str(e)}")
    logger.error(f"堆疊追蹤:\n{traceback.format_exc()}")
    # 更新錯誤統計
    self.context.stats.record_error(type(e).__name__)
```

## 🔧 進階用法

### 多格式輸出配置

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

### 選擇性處理階段

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

## 📈 實用範例情境

框架支持多種使用情境：

### 情境1: 單純讀取文件

```python
from core import ETLContext
from processors.extract import ExtractProcessor

context = ETLContext()
extractor = ExtractProcessor(context)

# 並行讀取文件
combined_data = extractor.process_concurrent(
    file_paths=['data/file1.xlsx', 'data/file2.xlsx'],
    max_workers=4,
    parse_dates=['date']
)
```

### 情境2: 讀取與轉換

```python
from core import ETLContext, ProcessingMode
from processors.extract import ExtractProcessor
from processors.transform import TransformProcessor
from orchestration.orchestrator import ETLOrchestratorWithOutput

context = ETLContext()
extractor = ExtractProcessor(context)
transformer = TransformProcessor(context)

orchestrator = ETLOrchestratorWithOutput(
    context=context,
    extractor=extractor,
    transformer=transformer
)

orchestrator.run(
    data_dir='data/raw',
    file_pattern='*.csv',
    skip_load=True,
    skip_output=True,
    transform_params={
        'num_partitions': 4,
        'max_workers': 4,
        'save_transformed': True,
        'transformed_path': 'data/transformed.csv'
    }
)
```

### 情境3: 完整ETL處理

完整的ETL流程見[快速開始](#快速開始)部分和[會計數據處理](#會計數據處理)部分。

## 🔍 性能優化建議

1. **I/O密集型處理**（提取和輸出階段）:
   - 增加 `max_workers` 參數值以提高並行度
   - 範例: `extract_params={'max_workers': 8}`
   - 使用多線程而非多進程，因為I/O受限操作不受GIL影響

2. **CPU密集型處理**（轉換階段）:
   - 設置合適的分區數和工作進程數，通常不超過CPU核心數
   - 範例: `transform_params={'num_partitions': 4, 'max_workers': 4}`
   - 減少分區數量可以降低合併開銷

3. **自動優化**:
   - 啟用 `enable_auto_optimization=True` 參數
   - 框架會基於系統資源和數據大小動態調整參數
   - 動態監控CPU和記憶體使用率

4. **大型數據集**:
   - 增加分區數以減少每個工作進程的記憶體使用
   - 範例: `transform_params={'num_partitions': 8}`
   - 考慮使用Parquet輸出以減少磁盤使用和提高後續讀取速度

## 🔒 併發安全性設計

框架設計上考慮多層次的併發安全性:

1. **文件級鎖定**: 使用`FileLockManager`提供細粒度文件鎖，防止併發寫入衝突
2. **共享資源保護**: 所有共享狀態操作都使用適當的鎖保護
3. **線程隔離**: 使用`threading.local()`實現線程特定數據隔離
4. **計數器安全**: 使用專用鎖保護進度和統計計數器
5. **跨進程安全**: 針對多進程場景設計的資源分配和初始化機制

## 💾 日誌系統特點

框架包含高級日誌系統:

1. **非阻塞寫入**: 使用隊列處理器將日誌寫入操作移至背景線程
2. **自動堆疊追蹤**: 增強型`TracebackLogger`自動添加詳細錯誤信息
3. **全局異常捕獲**: 捕獲並記錄所有未處理的異常，包括子線程異常
4. **進程感知**: 適當處理多進程環境中的日誌初始化和配置
5. **按級別鎖定**: 不同日誌級別使用獨立鎖，減少寫入競爭

## 📄 授權

本專案採用 MIT 授權條款。

## 🙏 致謝

感謝所有貢獻者和測試者。如有問題或建議，請提交 Issue 或 Pull Request。