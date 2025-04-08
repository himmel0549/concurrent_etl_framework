# utils/traceback_logger.py
import logging
import traceback
import inspect
import sys
import threading
import atexit
import os
import multiprocessing

# 確保線程/進程安全的初始化鎖
_logger_class_lock = threading.RLock()
_exception_handler_lock = threading.RLock()

# 追蹤已初始化的進程
_initialized_processes = set()
_process_lock = threading.RLock()

# 防止子線程異常處理器重複安裝
_threading_excepthook_installed = False
_threading_lock = threading.RLock()

class TracebackLogger(logging.Logger):
    """
    增強型 Logger 類，自動在 error 和 critical 級別記錄中添加 traceback 信息
    """
    
    def error(self, msg, *args, exc_info=None, stack_info=False, extra=None, **kwargs):
        """
        重寫 error 方法，自動添加 traceback 信息
        
        如果 exc_info 已經提供，則使用提供的值
        如果 exc_info 為 None 且當前處於例外上下文中，自動添加例外信息
        """
        # 如果 exc_info 未提供且當前處於例外上下文中
        if exc_info is None and sys.exc_info()[0] is not None:
            exc_info = True
        
        # 如果 msg 不是字串，轉換為字串
        if not isinstance(msg, str):
            msg = str(msg)
            
        # 即使沒有異常，也添加當前的調用堆疊（但僅在未明確設置 exc_info 或 stack_info 時）
        if not exc_info and not stack_info:
            try:
                # 獲取堆疊但排除當前框架和 logging 框架
                stack_frames = traceback.extract_stack()
                # 找到最後一個非 logging 框架
                non_logging_frames = []
                for frame in stack_frames:
                    if 'logging' not in frame.filename and 'traceback_logger.py' not in frame.filename:
                        non_logging_frames.append(frame)
                
                # 生成更簡潔的堆疊追蹤（只顯示關鍵信息）
                stack_trace = "堆疊追蹤 (簡化版):\n"
                for frame in non_logging_frames[-5:]:  # 只取最近的5幀
                    stack_trace += f"  文件 \"{frame.filename}\", 行 {frame.lineno}, 在 {frame.name}\n"
                
                msg = f"{msg}\n{stack_trace}"
            except Exception:
                # 如果堆疊提取失敗，不影響原本的日誌記錄
                pass
        
        # 調用父類的 error 方法
        super().error(msg, *args, exc_info=exc_info, stack_info=stack_info, extra=extra, **kwargs)
    
    def critical(self, msg, *args, exc_info=None, stack_info=False, extra=None, **kwargs):
        """
        重寫 critical 方法，自動添加 traceback 信息，邏輯與 error 方法相同
        """
        # 如果 exc_info 未提供且當前處於例外上下文中
        if exc_info is None and sys.exc_info()[0] is not None:
            exc_info = True
            
        # 如果 msg 不是字串，轉換為字串
        if not isinstance(msg, str):
            msg = str(msg)
            
        # 即使沒有異常，也添加當前的調用堆疊（但僅在未明確設置 exc_info 或 stack_info 時）
        if not exc_info and not stack_info:
            try:
                # 獲取堆疊但排除當前框架和 logging 框架
                stack_frames = traceback.extract_stack()
                # 找到最後一個非 logging 框架
                non_logging_frames = []
                for frame in stack_frames:
                    if 'logging' not in frame.filename and 'traceback_logger.py' not in frame.filename:
                        non_logging_frames.append(frame)
                
                # 生成更簡潔的堆疊追蹤（只顯示關鍵信息）
                stack_trace = "堆疊追蹤 (簡化版):\n"
                for frame in non_logging_frames[-5:]:  # 只取最近的5幀
                    stack_trace += f"  文件 \"{frame.filename}\", 行 {frame.lineno}, 在 {frame.name}\n"
                
                msg = f"{msg}\n{stack_trace}"
            except Exception:
                # 如果堆疊提取失敗，不影響原本的日誌記錄
                pass
        
        # 調用父類的 critical 方法
        super().critical(msg, *args, exc_info=exc_info, stack_info=stack_info, extra=extra, **kwargs)


def get_traceback_logger(name=None):
    """
    獲取帶有自動 traceback 功能的 logger
    
    如果未提供 name，自動獲取調用者的模組名
    """
    if name is None:
        # 獲取調用者的模組名，更安全的方式（防止 inspect 失敗）
        try:
            frame = inspect.currentframe().f_back
            name = frame.f_globals.get('__name__', 'root')
        except (AttributeError, ValueError):
            name = 'root'
    
    # 使用鎖來確保線程安全地註冊 Logger 類
    with _logger_class_lock:
        # 向 logging 系統註冊我們的 Logger 類（如果尚未註冊）
        if not hasattr(logging, '_traceback_logger_initialized'):
            original_logger_class = logging.getLoggerClass()
            logging.setLoggerClass(TracebackLogger)
            setattr(logging, '_traceback_logger_initialized', True)
            setattr(logging, '_original_logger_class', original_logger_class)
    
    # 獲取 logger 實例
    return logging.getLogger(name)

# 用於捕獲線程未處理異常的處理器
def _thread_exception_handler(args):
    """處理線程未捕獲的異常"""
    # 獲取線程名稱，如果可用
    thread_name = args.thread.name if hasattr(args, 'thread') and hasattr(args.thread, 'name') else 'unknown'
    # 使用專門的 logger
    thread_logger = get_traceback_logger(f'thread-{thread_name}')
    # 記錄異常
    thread_logger.critical(
        f"子線程 '{thread_name}' 未捕獲的例外",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
    )

# 安裝用於處理未捕獲例外的處理程序
def setup_global_exception_handler(logger=None):
    """
    設置全局未捕獲例外處理程序，將未捕獲的例外記錄到指定的 logger
    
    這個函數是線程安全的，可以被多次調用，但只會設置一次
    """
    global _threading_excepthook_installed
    
    # 記錄進程 ID，確保每個進程只初始化一次
    current_pid = os.getpid()
    
    with _process_lock:
        if current_pid in _initialized_processes:
            return
        _initialized_processes.add(current_pid)
    
    if logger is None:
        logger = get_traceback_logger('uncaught')
    
    # 主線程異常處理
    def handle_exception(exc_type, exc_value, exc_traceback):
        """處理主線程未捕獲的例外"""
        if issubclass(exc_type, KeyboardInterrupt):
            # 正常退出方式，不記錄堆疊
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # 記錄未捕獲的例外
        logger.critical(
            f"進程 {os.getpid()} 主線程未捕獲的例外", 
            exc_info=(exc_type, exc_value, exc_traceback)
        )
    
    # 使用鎖確保線程安全
    with _exception_handler_lock:
        # 設置全局例外處理程序
        original_excepthook = sys.excepthook
        # 確保我們不會覆蓋用戶自定義的 excepthook
        if original_excepthook is not sys.__excepthook__ and original_excepthook.__module__ != __name__:
            # 保存用戶的 excepthook
            def combined_excepthook(exc_type, exc_value, exc_traceback):
                handle_exception(exc_type, exc_value, exc_traceback)
                original_excepthook(exc_type, exc_value, exc_traceback)
            sys.excepthook = combined_excepthook
        else:
            sys.excepthook = handle_exception
    
    # 設置線程異常處理 (Python 3.8+)
    if hasattr(threading, 'excepthook'):
        with _threading_lock:
            if not _threading_excepthook_installed:
                # 保存原始 excepthook（如果存在）
                original_threading_excepthook = threading.excepthook
                
                # 如果原始 excepthook 已被設置且不是默認的
                if hasattr(original_threading_excepthook, '__module__') and \
                   original_threading_excepthook.__module__ != 'threading':
                    def combined_thread_excepthook(args):
                        _thread_exception_handler(args)
                        original_threading_excepthook(args)
                    threading.excepthook = combined_thread_excepthook
                else:
                    threading.excepthook = _thread_exception_handler
                
                _threading_excepthook_installed = True


# 為多進程環境設置初始化函數
def init_worker():
    """
    子進程初始化函數，設置異常處理器
    在使用 ProcessPoolExecutor 時作為 initializer 參數傳入
    """
    from utils.logging import setup_logging
    
    # 在子進程中設置日誌和異常處理
    setup_logging()
    setup_global_exception_handler()
    
    # 記錄進程啟動信息
    logger = get_traceback_logger('process')
    logger.info(f"進程 {os.getpid()} 已初始化異常處理")


# 在主程序退出時清理資源
def _cleanup():
    """清理資源，防止資源洩漏"""
    # 這裡可以添加任何需要的清理代碼
    pass

# 註冊退出處理
atexit.register(_cleanup)
