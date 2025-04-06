from typing import List, Generic, TypeVar
from abc import ABC, abstractmethod
from core.context import ETLContext
from .types import T, R

# ETL 處理器接口
class ETLProcessor(ABC, Generic[T, R]):
    """ETL 處理器抽象基類"""
    def __init__(self, context: ETLContext = None):
        self.context = context or ETLContext()
    
    @abstractmethod
    def process(self, input_data: T, **kwargs) -> R:
        """處理數據"""
        pass
    
    def process_concurrent(self, input_data: List[T], **kwargs) -> List[R]:
        """並行處理數據"""
        pass

# 數據生成器接口
class DataGenerator(ABC):
    """數據生成器抽象基類"""
    @abstractmethod
    def generate(self, **kwargs) -> None:
        """生成數據"""
        pass