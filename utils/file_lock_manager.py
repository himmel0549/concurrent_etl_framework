# utils/file_lock_manager.py
import threading
import os

class FileLockManager:
    """文件鎖管理器，使用路徑感知的文件鎖，為不同文件提供獨立的鎖"""
    
    def __init__(self):
        self._locks = {}
        self._manager_lock = threading.Lock()
    
    def get_lock(self, file_path):
        """獲取指定文件的鎖，如果不存在則創建"""
        # 標準化文件路徑
        norm_path = os.path.normpath(os.path.abspath(file_path))
        
        with self._manager_lock:
            if norm_path not in self._locks:
                self._locks[norm_path] = threading.Lock()
            return self._locks[norm_path]

# 全局實例
file_lock_manager = FileLockManager()