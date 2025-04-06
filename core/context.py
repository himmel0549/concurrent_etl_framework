from typing import Any
from utils.logging import get_logger


logger = get_logger(__name__)

class ETLContext:
    """ETL 上下文，存儲全局配置和資源"""
    def __init__(self, stats=None):
        """
        初始化上下文
        如果未提供 stats，將使用全局變數
        """
        # 延遲導入，避免循環依賴
        if stats is None:
            from config.constants import etl_stats
            self.stats = etl_stats
        else:
            self.stats = stats
        
        self.config = {}
        self.resources = {}
    
    def set_config(self, key: str, value: Any) -> None:
        """設置配置項"""
        self.config[key] = value
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """獲取配置項"""
        return self.config.get(key, default)
    
    def set_resource(self, key: str, value: Any) -> None:
        """設置資源"""
        self.resources[key] = value
    
    def get_resource(self, key: str, default: Any = None) -> Any:
        """獲取資源"""
        return self.resources.get(key, default)
    
    def reset_stats(self) -> None:
        """重置統計信息"""
        self.stats.reset()