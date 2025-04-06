# 導入關鍵組件
from core import (
    ProcessingMode, ETLContext, ETLStats, 
    ETLProcessor, DataGenerator
)
from processors import (
    ExtractProcessor, TransformProcessor, LoadProcessor
)
from orchestration import ETLOrchestrator, PerformanceComparator
from generators import SalesDataGenerator
from config import etl_stats, file_lock, log_lock

# 版本信息
__version__ = '0.1.0'

__all__ = [
    'ProcessingMode', 'ETLContext', 'ETLStats', 'ETLProcessor', 'DataGenerator',
    'ExtractProcessor', 'TransformProcessor', 'LoadProcessor',
    'ETLOrchestrator', 'PerformanceComparator',
    'SalesDataGenerator',
    'etl_stats', 'file_lock', 'log_lock'
]