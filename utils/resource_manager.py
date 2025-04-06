# utils/resource_manager.py
import psutil
import os
import time
import threading

class ResourceManager:
    """系統資源管理器，監控並優化資源分配"""
    
    def __init__(self, polling_interval=10):
        self.polling_interval = polling_interval  # 秒
        self.resources = self.monitor_system_resources()
        self._stop_flag = False
        self._monitor_thread = None
        
    def start_monitoring(self):
        """開始背景監控"""
        self._stop_flag = False
        self._monitor_thread = threading.Thread(
            target=self._background_monitor,
            daemon=True
        )
        self._monitor_thread.start()
        
    def stop_monitoring(self):
        """停止監控"""
        self._stop_flag = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1)
    
    def _background_monitor(self):
        """背景監控執行緒"""
        while not self._stop_flag:
            self.resources = self.monitor_system_resources()
            time.sleep(self.polling_interval)
    
    def monitor_system_resources(self):
        """監控系統資源使用情況"""
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1, percpu=True)
        
        # 收集 IO 等待時間，判斷是否 IO 密集型
        disk_io = psutil.disk_io_counters()
        
        return {
            'memory_used_percent': memory.percent,
            'available_memory_gb': memory.available / (1024 ** 3),
            'cpu_usage': cpu,
            'average_cpu': sum(cpu) / len(cpu),
            'disk_io': disk_io,
            'timestamp': time.time()
        }
    
    def get_adaptive_workers(self, task_type='cpu', min_workers=2, max_workers=None):
        """根據系統負載動態調整工作進程數
        
        參數:
            task_type: 'cpu' 或 'io' - 任務類型
            min_workers: 最小工作進程數
            max_workers: 最大工作進程數 (默認為CPU核心數)
        """
        if max_workers is None:
            max_workers = os.cpu_count() or 4
            
        # 更新資源狀態
        resources = self.resources or self.monitor_system_resources()
        
        # 如果系統負載過高，返回最小工作進程數
        if resources['memory_used_percent'] > 85:
            return min_workers
            
        if resources['average_cpu'] > 90:
            return min_workers
            
        # 根據任務類型和可用資源計算安全的工作進程數
        if task_type == 'io':
            # IO密集型任務可以使用較多線程
            io_factor = 1.5
            worker_count = int(max_workers * io_factor)
        else:  # CPU密集型
            # 基於CPU使用率
            cpu_factor = 1 - (resources['average_cpu'] / 100)  # cpu未使用率
            cpu_based = int(max_workers * cpu_factor)
            
            # 基於記憶體使用率
            memory_factor = 1 - (resources['memory_used_percent'] / 100)  # 記憶體未使用率
            memory_based = int(max_workers * memory_factor)
            
            worker_count = min(cpu_based, memory_based)
        
        # 確保在有效範圍內
        return max(min_workers, min(worker_count, max_workers))