from typing import Dict, Any, Set
from dataclasses import dataclass, field
import time
import threading


@dataclass
class ETLStats:
    """ETL 處理統計信息"""
    # 處理狀態計數
    processed_files: int = 0
    total_records: int = 0
    error_count: int = 0
    # 詳細信息
    error_types: Dict[str, int] = field(default_factory=dict)
    processed_file_paths: Set[str] = field(default_factory=set)
    # 計時
    start_time: float = field(default_factory=time.time)
    # 並發安全的鎖
    lock: threading.Lock = field(default_factory=threading.Lock)
    
    def file_processed(self, file_path: str, records_count: int) -> None:
        """記錄文件處理完成"""
        with self.lock:
            self.processed_files += 1
            self.total_records += records_count
            self.processed_file_paths.add(file_path)
    
    def record_error(self, error_type: str) -> None:
        """記錄錯誤信息"""
        with self.lock:
            self.error_count += 1
            if error_type in self.error_types:
                self.error_types[error_type] += 1
            else:
                self.error_types[error_type] = 1
    
    def get_progress(self, total_files: int) -> Dict[str, Any]:
        """取得進度信息"""
        with self.lock:
            elapsed = time.time() - self.start_time
            return {
                'processed_files': self.processed_files,
                'total_files': total_files,
                'percent_complete': (self.processed_files / total_files) * 100 if total_files > 0 else 0,
                'records_processed': self.total_records,
                'errors': self.error_count,
                'elapsed_seconds': elapsed
            }
    
    def reset(self) -> None:
        """重置統計信息"""
        with self.lock:
            self.processed_files = 0
            self.total_records = 0
            self.error_count = 0
            self.error_types.clear()
            self.processed_file_paths.clear()
            self.start_time = time.time()