from enum import Enum


class ProcessingMode(Enum):
    """處理模式枚舉"""
    SEQUENTIAL = "sequential"  # 串行處理
    CONCURRENT = "concurrent"  # 並行處理