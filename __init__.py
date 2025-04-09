"""
ETL併發處理框架
高效能、可擴展的ETL併發處理框架，專為處理會計底稿和數據分析而設計
"""

from .core import (
    ProcessingMode, ETLContext, ETLStats, 
    ETLProcessor, DataGenerator
)
from .processors import (
    ExtractProcessor, TransformProcessor, LoadProcessor, OutputProcessor
)
from .orchestration import ETLOrchestrator, PerformanceComparator, ETLOrchestratorWithOutput

from .generators import SalesDataGenerator, AccountingDataGenerator

# 版本信息
__version__ = '0.1.0'

__all__ = [
    'ProcessingMode', 'ETLContext', 'ETLStats', 'ETLProcessor', 'DataGenerator',
    'ExtractProcessor', 'TransformProcessor', 'LoadProcessor', 'OutputProcessor',
    'ETLOrchestrator', 'PerformanceComparator', 'ETLOrchestratorWithOutput',
    'SalesDataGenerator', 'AccountingDataGenerator'
]