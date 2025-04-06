# utils/logging.py
# utils/logging.py
import logging
import inspect
import sys

# 已經設置過日誌的標記
_logging_setup_done = False

def setup_logging(level=logging.INFO):
    """設置全局日誌配置"""
    global _logging_setup_done
    if not _logging_setup_done:
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        _logging_setup_done = True

def get_logger(name=None):
    """
    獲取指定名稱的日誌記錄器
    如果未提供名稱，自動獲取調用者的模組名
    """
    setup_logging()  # 確保日誌已設置
    
    if name is None:
        # 獲取調用者的模組名
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'root')
    
    return logging.getLogger(name)