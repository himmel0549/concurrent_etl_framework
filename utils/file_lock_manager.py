# utils/file_lock_manager.py
import threading
import os
import time
import weakref
import logging

# 獲取模組的日誌記錄器
logger = logging.getLogger(__name__)

class FileLockManager:
    """
    文件鎖管理器，使用路徑感知的文件鎖，為不同文件提供獨立的鎖
    提供增強的線程安全性和資源管理
    """
    
    def __init__(self, auto_cleanup_interval=300):
        """
        初始化文件鎖管理器
        
        參數:
            auto_cleanup_interval: 自動清理未使用鎖的間隔（秒）
        """
        # 使用字典存儲文件路徑到鎖的映射
        self._locks = {}
        # 使用字典記錄每個鎖的最後使用時間
        self._last_used = {}
        # 管理器的元鎖，保護內部狀態
        self._manager_lock = threading.RLock()
        # 自動清理間隔
        self._auto_cleanup_interval = auto_cleanup_interval
        # 最後一次清理時間
        self._last_cleanup = time.time()
        # 是否啟用調試日誌
        self._debug = False
        
    def get_lock(self, file_path):
        """
        獲取指定文件的鎖，如果不存在則創建
        
        參數:
            file_path: 文件路徑
            
        返回:
            文件對應的鎖對象
        """
        # 標準化文件路徑以確保一致性
        try:
            norm_path = os.path.normpath(os.path.abspath(file_path))
        except (TypeError, ValueError) as e:
            logger.error(f"無法標準化文件路徑 '{file_path}': {str(e)}")
            # 回退到原始路徑
            norm_path = str(file_path)
        
        # 嘗試自動清理
        self._try_auto_cleanup()
        
        # 使用管理器鎖保護內部狀態
        with self._manager_lock:
            # 檢查路徑是否已有對應的鎖
            if norm_path not in self._locks:
                # 創建新鎖
                self._locks[norm_path] = threading.RLock()
                # 在調試模式下記錄鎖創建信息
                if self._debug:
                    logger.debug(f"為文件 '{norm_path}' 創建新的鎖")
            
            # 更新最後使用時間
            self._last_used[norm_path] = time.time()
            return self._locks[norm_path]
    
    def _try_auto_cleanup(self):
        """
        嘗試自動清理長時間未使用的鎖
        """
        # 檢查是否需要清理
        current_time = time.time()
        if current_time - self._last_cleanup < self._auto_cleanup_interval:
            return
            
        # 使用管理器鎖保護清理過程
        with self._manager_lock:
            # 更新最後清理時間
            self._last_cleanup = current_time
            
            # 查找長時間未使用的鎖
            expired_paths = []
            for path, last_time in self._last_used.items():
                # 如果鎖超過10分鐘未使用
                if current_time - last_time > 600:  # 10分鐘
                    expired_paths.append(path)
            
            # 移除過期的鎖
            for path in expired_paths:
                if path in self._locks:
                    del self._locks[path]
                if path in self._last_used:
                    del self._last_used[path]
            
            # 記錄清理結果
            if expired_paths and self._debug:
                logger.debug(f"清理了 {len(expired_paths)} 個未使用的鎖")
    
    def release_all(self):
        """
        釋放所有鎖 (通常在程序終止時調用)
        """
        with self._manager_lock:
            self._locks.clear()
            self._last_used.clear()
    
    def set_debug(self, debug=True):
        """
        設置是否啟用調試日誌
        """
        self._debug = debug
    
    def get_lock_status(self):
        """
        獲取當前鎖的狀態信息 (用於診斷)
        
        返回:
            包含鎖狀態信息的字典
        """
        with self._manager_lock:
            return {
                'active_locks': len(self._locks),
                'locks': [{'path': path, 'last_used': self._last_used.get(path, 0)}
                          for path in self._locks]
            }

# 全局實例
file_lock_manager = FileLockManager()

# 註冊進程終止時的清理函數
import atexit
atexit.register(file_lock_manager.release_all)
