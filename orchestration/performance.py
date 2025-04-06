from typing import Dict, Any
import time
from orchestration.orchestrator import ETLOrchestrator
from config.constants import log_lock
from utils.logging import get_logger
from core.enums import ProcessingMode


logger = get_logger(__name__)

# 效能比較器
class PerformanceComparator:
    """
    效能比較器
    用於比較不同處理模式的效能差異
    """
    def __init__(self, orchestrator: ETLOrchestrator):
        """
        初始化效能比較器
        
        參數:
            orchestrator: ETL協調器
        """
        self.orchestrator = orchestrator
    
    def compare(self, 
                extract_params: Dict[str, Any] = None,
                transform_params: Dict[str, Any] = None,
                load_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        比較串行和並行處理的效能差異
        
        參數:
            extract_params: 提取階段參數
            transform_params: 轉換階段參數
            load_params: 加載階段參數
        
        返回:
            比較結果
        """
        with log_lock:
            logger.info("\n\n========== 效能比較：串行 vs 並行 ==========")
        
        # 克隆參數，避免修改原始對象
        seq_extract_params = extract_params.copy() if extract_params else {}
        seq_transform_params = transform_params.copy() if transform_params else {}
        seq_load_params = load_params.copy() if load_params else {}
        
        par_extract_params = extract_params.copy() if extract_params else {}
        par_transform_params = transform_params.copy() if transform_params else {}
        par_load_params = load_params.copy() if load_params else {}
        
        # 設置不同的輸出文件名，避免衝突
        seq_reports = [
            {'dimension': 'store', 'filename': 'data/final/store_monthly_report_seq.csv'},
            {'dimension': 'product', 'filename': 'data/final/product_monthly_report_seq.csv'},
            {'dimension': 'date', 'filename': 'data/final/daily_sales_report_seq.csv'}
        ]
        
        par_reports = [
            {'dimension': 'store', 'filename': 'data/final/store_monthly_report_par.csv'},
            {'dimension': 'product', 'filename': 'data/final/product_monthly_report_par.csv'},
            {'dimension': 'date', 'filename': 'data/final/daily_sales_report_par.csv'}
        ]
        
        # 執行串行處理
        with log_lock:
            logger.info("執行串行ETL流程")
        
        self.orchestrator.context.reset_stats()
        seq_start = time.time()
        self.orchestrator.run(
            processing_mode=ProcessingMode.SEQUENTIAL,
            extract_params=seq_extract_params,
            transform_params=seq_transform_params,
            load_params=seq_load_params,
            reports=seq_reports
        )
        seq_total = time.time() - seq_start
        
        # 執行並行處理
        with log_lock:
            logger.info("執行並行ETL流程")
        
        self.orchestrator.context.reset_stats()
        par_start = time.time()
        self.orchestrator.run(
            processing_mode=ProcessingMode.CONCURRENT,
            extract_params=par_extract_params,
            transform_params=par_transform_params,
            load_params=par_load_params,
            reports=par_reports
        )
        par_total = time.time() - par_start
        
        # 打印比較結果
        speedup = (seq_total / par_total - 1) * 100 if par_total > 0 else 0
        
        with log_lock:
            logger.info("\n=== 效能比較結果 ===")
            logger.info(f"串行處理總時間: {seq_total:.2f}秒")
            logger.info(f"並行處理總時間: {par_total:.2f}秒")
            logger.info(f"效能提升: {speedup:.2f}%")
            logger.info("======================\n")
        
        return {
            'sequential_time': seq_total,
            'parallel_time': par_total,
            'speedup_percentage': speedup
        }