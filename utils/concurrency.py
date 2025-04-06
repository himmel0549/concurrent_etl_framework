import time
import concurrent.futures
from utils.logging import get_logger
logger = get_logger(__name__)

# 示例工具類
class ConcurrencyExamples:
    """
    並發處理示例
    展示 concurrent.futures 的高級用法
    """
    @staticmethod
    def cancel_example():
        """演示任務取消"""
        def long_running_task(n):
            logger.info(f"任務 {n} 開始執行")
            try:
                for i in range(10):
                    time.sleep(0.5)
                    logger.info(f"任務 {n} 進度: {i+1}/10")
            except Exception as e:
                logger.info(f"任務 {n} 被取消或出錯: {str(e)}")
                raise
            return f"任務 {n} 完成"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # 提交5個任務
            futures = [executor.submit(long_running_task, i) for i in range(5)]
            
            # 等待1秒後取消部分任務
            time.sleep(1)
            for i, future in enumerate(futures):
                if i % 2 == 0:  # 取消偶數任務
                    cancelled = future.cancel()
                    logger.info(f"嘗試取消任務 {i}: {'成功' if cancelled else '失敗'}")
            
            # 獲取結果
            for i, future in enumerate(futures):
                try:
                    result = future.result() if not future.cancelled() else "已取消"
                    logger.info(f"任務 {i} 結果: {result}")
                except concurrent.futures.CancelledError:
                    logger.info(f"任務 {i} 被取消")
                except Exception as e:
                    logger.info(f"任務 {i} 出錯: {str(e)}")
    
    @staticmethod
    def timeout_example():
        """演示任務超時處理"""
        def slow_task(n):
            """模擬一個執行時間不確定的任務"""
            sleep_time = n * 2  # 任務執行時間與輸入成正比
            logger.info(f"任務 {n} 開始執行, 預計需要 {sleep_time} 秒")
            time.sleep(sleep_time)
            return f"任務 {n} 完成，耗時 {sleep_time} 秒"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # 提交多個任務
            futures = {executor.submit(slow_task, i): i for i in range(1, 5)}
            
            # 使用不同的超時設置獲取結果
            for future in concurrent.futures.as_completed(futures):
                task_id = futures[future]
                try:
                    # 對不同任務使用不同的超時時間
                    timeout = 3 if task_id <= 2 else None
                    result = future.result(timeout=timeout)
                    logger.info(f"獲取結果: {result}")
                except concurrent.futures.TimeoutError:
                    logger.info(f"任務 {task_id} 超時")
                except Exception as e:
                    logger.info(f"任務 {task_id} 出錯: {str(e)}")
    
    @staticmethod
    def callback_example():
        """演示使用回調函數處理任務完成"""
        def process_data(data):
            """處理數據，有可能成功或失敗"""
            logger.info(f"處理數據: {data}")
            if data % 3 == 0:
                raise ValueError(f"不能處理被3整除的數據: {data}")
            time.sleep(1)  # 模擬處理時間
            return data * 2
        
        def on_completed(future):
            """任務完成時的回調"""
            try:
                result = future.result()
                logger.info(f"任務成功完成，結果: {result}")
            except Exception as e:
                logger.info(f"任務失敗: {str(e)}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # 提交多個任務，並附加回調
            for i in range(1, 10):
                future = executor.submit(process_data, i)
                future.add_done_callback(on_completed)
            
            logger.info("所有任務已提交，等待完成...")