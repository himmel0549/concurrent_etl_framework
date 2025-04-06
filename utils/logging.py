# utils/logging.py
import logging
import inspect
import os
import sys

# 已經設置過日誌的標記
_logging_setup_done = False

def setup_logging(level=logging.INFO):
    """設置全局日誌配置"""
    global _logging_setup_done
    if not _logging_setup_done:
        """配置日誌系統"""
        # 獲取根記錄器
        root_logger = logging.getLogger('')
        
        # 清除現有處理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        """配置日誌系統"""
        log_level = 'INFO'
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # 設置日誌級別
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level = level_map.get(log_level, logging.INFO)
        
        # 配置日誌路徑
        log_path = './logs'
        if not os.path.exists(log_path):
            os.makedirs(log_path, exist_ok=True)
        
        # 生成日誌文件名
        from datetime import datetime
        day = datetime.strftime(datetime.now(), '%Y-%m-%d')
        log_name = os.path.join(log_path, f'ETL_{day}.log')
        
        # 配置日誌
        logging.basicConfig(
            level=level,
            format=log_format,
            filename=log_name,
            filemode='a',
            encoding='utf-8',
        )
        
        # 添加控制台輸出
        console = logging.StreamHandler()
        console.setLevel(level)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        root_logger.addHandler(console)

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