# utils/traceback_logger.py
import logging
import traceback
import inspect
import sys

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
            
        # 即使沒有異常，也添加當前的調用堆疊
        if not exc_info and not stack_info:
            stack_trace = ''.join(traceback.format_stack()[:-1])  # 排除當前幀
            msg = f"{msg}\n堆疊追蹤:\n{stack_trace}"
        
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
            
        # 即使沒有異常，也添加當前的調用堆疊
        if not exc_info and not stack_info:
            stack_trace = ''.join(traceback.format_stack()[:-1])  # 排除當前幀
            msg = f"{msg}\n堆疊追蹤:\n{stack_trace}"
        
        # 調用父類的 critical 方法
        super().critical(msg, *args, exc_info=exc_info, stack_info=stack_info, extra=extra, **kwargs)


def get_traceback_logger(name=None):
    """
    獲取帶有自動 traceback 功能的 logger
    
    如果未提供 name，自動獲取調用者的模組名
    """
    if name is None:
        # 獲取調用者的模組名
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'root')
    
    # 向 logging 系統註冊我們的 Logger 類
    if not hasattr(logging, '_traceback_logger_initialized'):
        logging.setLoggerClass(TracebackLogger)
        setattr(logging, '_traceback_logger_initialized', True)
    
    # 獲取 logger 實例
    return logging.getLogger(name)

# 安裝用於處理未捕獲例外的處理程序
def setup_global_exception_handler(logger=None):
    """
    設置全局未捕獲例外處理程序，將未捕獲的例外記錄到指定的 logger
    """
    if logger is None:
        logger = get_traceback_logger('uncaught')
    
    def handle_exception(exc_type, exc_value, exc_traceback):
        """處理未捕獲的例外"""
        if issubclass(exc_type, KeyboardInterrupt):
            # 正常退出方式，不記錄堆疊
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # 記錄未捕獲的例外
        logger.critical("未捕獲的例外", exc_info=(exc_type, exc_value, exc_traceback))
    
    # 設置全局例外處理程序
    sys.excepthook = handle_exception
