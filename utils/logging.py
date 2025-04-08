# utils/logging.py
import logging
import inspect
import threading
import os
from utils.enhanced_logging import setup_enhanced_logging
from utils.traceback_logger import get_traceback_logger, setup_global_exception_handler, init_worker

# 已經設置過日誌的標記
_logging_setup_done = False
_logging_setup_lock = threading.RLock()
_process_initialized = set()
_process_lock = threading.RLock()

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
    
    # 進程級別的初始化跟踪（確保每個進程只初始化一次）
    current_pid = os.getpid()
    
    with _process_lock:
        # 如果當前進程已初始化，則返回
        if current_pid in _process_initialized:
            return
    
    # 使用鎖保護初始化過程，確保線程安全
    with _logging_setup_lock:
        if not _logging_setup_done:
            try:
                setup_enhanced_logging()
                setup_global_exception_handler()  # 設置全局未捕獲例外處理程序
                _logging_setup_done = True
                
                # 記錄初始化完成
                with _process_lock:
                    _process_initialized.add(current_pid)
                
                # 初始化成功日誌
                logger = get_traceback_logger('logging_setup')
                logger.info(f"日誌系統已在進程 {current_pid} 中初始化")
                
            except Exception as e:
                # 如果初始化過程發生錯誤，記錄錯誤但不中斷程序
                # 使用標準日誌記錄器，因為增強型可能尚未設置
                fallback_logger = logging.getLogger('logging_setup')
                fallback_logger.error(f"初始化日誌系統時發生錯誤: {str(e)}")
                # 確保至少基本日誌功能可用
                logging.basicConfig(level=level)

def get_logger(name=None):
    """
    獲取指定名稱的日誌記錄器，使用增強型 TracebackLogger
    如果未提供名稱，自動獲取調用者的模組名
    
    返回:
        自動添加 traceback 信息的 logger 實例
    """
    # 確保日誌已設置
    setup_logging()
    
    if name is None:
        # 獲取調用者的模組名，使用更安全的方式
        try:
            frame = inspect.currentframe().f_back
            name = frame.f_globals.get('__name__', 'root')
        except (AttributeError, ValueError):
            name = 'root'
    
    # 使用增強型 logger，自動添加 traceback 信息
    return get_traceback_logger(name)

# 暴露 init_worker 函數，以便在多進程環境中使用
__all__ = ['setup_logging', 'get_logger', 'init_worker']
