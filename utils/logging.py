# utils/logging.py
import logging
import inspect
from utils.enhanced_logging import setup_enhanced_logging
from utils.traceback_logger import get_traceback_logger, setup_global_exception_handler

# 已經設置過日誌的標記
_logging_setup_done = False

def setup_logging(level=logging.INFO):
    """設置全局日誌配置
    _logging_setup_done重複設置保護:
        - 使用全局變量 _logging_setup_done 作為標誌
        - 首次調用時執行配置並設置標誌為 True
        - 後續調用檢測到標誌為 True 則跳過配置
    避免:
        - 資源浪費：每次調用都創建新的處理器、隊列和線程
        - 文件句柄泄漏：重複打開同一文件可能耗盡系統資源
        - 日誌重複：如果配置重複執行，可能導致日誌被多次寫入
        - 背景線程堆積：每次調用都會創建新的守護線程但不會終止舊線程
    """
    global _logging_setup_done
    if not _logging_setup_done:
        setup_enhanced_logging()
        setup_global_exception_handler()  # 設置全局未捕獲例外處理程序
        _logging_setup_done = True

def get_logger(name=None):
    """
    獲取指定名稱的日誌記錄器，使用增強型 TracebackLogger
    如果未提供名稱，自動獲取調用者的模組名
    
    返回:
        自動添加 traceback 信息的 logger 實例
    """
    setup_logging()  # 確保日誌已設置
    
    if name is None:
        # 獲取調用者的模組名
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'root')
    
    # 使用增強型 logger，自動添加 traceback 信息
    return get_traceback_logger(name)
