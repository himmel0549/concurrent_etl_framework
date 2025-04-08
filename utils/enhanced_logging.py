# utils/enhanced_logging.py
import logging
import queue
import threading
import os
from datetime import datetime

class QueueHandler(logging.Handler):
    """基於隊列的日誌處理器，非阻塞寫入"""
    
    def __init__(self):
        super().__init__()
        self.queue = queue.Queue()
        self.worker = threading.Thread(target=self._process_queue, daemon=True)
        self.worker.start()
        
    def emit(self, record):
        """將日誌放入隊列，避免阻塞主線程"""
        self.queue.put(record)
        
    def _process_queue(self):
        """背景處理日誌隊列"""
        while True:
            try:
                record = self.queue.get()
                self.format(record)
                self._write_to_handlers(record)
                self.queue.task_done()
            except Exception:
                import traceback
                traceback.print_exc()
    
    def _write_to_handlers(self, record):
        """將日誌分發到實際的處理器"""
        for handler in self.handlers:
            handler.emit(record)
            
    def addHandler(self, handler):
        """添加實際的日誌處理器"""
        if not hasattr(self, 'handlers'):
            self.handlers = []
        self.handlers.append(handler)

class LevelLockHandler(logging.Handler):
    """按日誌級別使用不同鎖的處理器"""
    
    def __init__(self, handler):
        super().__init__()
        self.handler = handler
        # 為不同級別使用不同鎖
        self.locks = {
            logging.DEBUG: threading.Lock(),
            logging.INFO: threading.Lock(),
            logging.WARNING: threading.Lock(),
            logging.ERROR: threading.Lock(),
            logging.CRITICAL: threading.Lock()
        }
        
    def emit(self, record):
        """使用級別對應的鎖處理日誌"""
        lock = self.locks.get(record.levelno, self.locks[logging.DEBUG])
        with lock:
            self.handler.emit(record)

def setup_enhanced_logging():
    """設置增強的日誌系統"""
    root_logger = logging.getLogger()
    
    # 清除現有處理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 設置格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 創建文件處理器
    log_path = './logs'
    os.makedirs(log_path, exist_ok=True)
    day = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(log_path, f'ETL_{day}.log')
    
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setFormatter(formatter)
    
    # 使用隊列處理器
    queue_handler = QueueHandler()
    queue_handler.setLevel(logging.INFO)
    
    # 添加級別鎖處理器來寫文件
    level_lock_handler = LevelLockHandler(file_handler)
    queue_handler.addHandler(level_lock_handler)
    
    # 控制台輸出
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    
    # 添加處理器
    root_logger.addHandler(queue_handler)
    root_logger.addHandler(console)
    root_logger.setLevel(logging.INFO)