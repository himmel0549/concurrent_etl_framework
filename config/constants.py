
import threading


# 資源鎖
file_lock = threading.Lock()  # 文件操作鎖
log_lock = threading.Lock()   # 日誌操作鎖