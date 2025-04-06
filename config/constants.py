
# import logging

import threading

# from concurrency_test.core import ETLStats
# 注意：不要在這裡導入 ETLStats
# 這會導致循環導入問題
# 我們需要在適當的時候初始化全局變數
from core.stats import ETLStats

# 初始化一個全局 stats 對象
etl_stats = ETLStats()


# 資源鎖
file_lock = threading.Lock()  # 文件操作鎖
log_lock = threading.Lock()   # 日誌操作鎖

# # 設置日誌格式
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)


# 全局統計對象 # 這些會在 main.py 中初始化
# etl_stats = None