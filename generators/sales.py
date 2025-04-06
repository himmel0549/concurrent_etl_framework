from core.interfaces import DataGenerator
from utils.logging import get_logger
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json


logger = get_logger(__name__)

class SalesDataGenerator(DataGenerator):
    """銷售數據生成器"""
    def __init__(self, output_dir: str = 'data/raw'):
        self.output_dir = output_dir
        # 創建輸出目錄
        os.makedirs(output_dir, exist_ok=True)
    
    def generate(self, days: int = 90, **kwargs) -> None:
        """
        生成銷售測試數據
        
        參數:
            days: 生成的歷史數據天數
            **kwargs: 其他參數
        """
        # 產品資訊
        products = [
            {"id": 1, "name": "筆記型電腦", "category": "電子產品", "base_price": 30000},
            {"id": 2, "name": "智慧型手機", "category": "電子產品", "base_price": 15000},
            {"id": 3, "name": "藍牙耳機", "category": "配件", "base_price": 2000},
            {"id": 4, "name": "滑鼠", "category": "配件", "base_price": 800},
            {"id": 5, "name": "鍵盤", "category": "配件", "base_price": 1200},
            {"id": 6, "name": "顯示器", "category": "電子產品", "base_price": 8000},
            {"id": 7, "name": "充電器", "category": "配件", "base_price": 500},
            {"id": 8, "name": "平板電腦", "category": "電子產品", "base_price": 12000},
        ]
        
        # 門市資訊
        stores = [
            {"id": 101, "name": "台北旗艦店", "region": "北部"},
            {"id": 102, "name": "新北門市", "region": "北部"},
            {"id": 201, "name": "台中門市", "region": "中部"},
            {"id": 301, "name": "高雄門市", "region": "南部"},
            {"id": 302, "name": "屏東門市", "region": "南部"},
        ]
        
        # 生成歷史銷售數據
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 為每個門市生成數據
        for store in stores:
            records = []
            for date in date_range:
                # 每天隨機銷售2-10件產品
                sales_count = np.random.randint(2, 11)
                for _ in range(sales_count):
                    product = np.random.choice(products)
                    quantity = np.random.randint(1, 6)
                    discount = np.random.choice([0, 0.05, 0.1, 0.15, 0.2], p=[0.7, 0.1, 0.1, 0.05, 0.05])
                    price = product["base_price"] * (1 - discount)
                    
                    record = {
                        "transaction_id": f"{store['id']}-{date.strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}",
                        "date": date.strftime('%Y-%m-%d'),
                        "store_id": store['id'],
                        "store_name": store['name'],
                        "region": store['region'],
                        "product_id": product['id'],
                        "product_name": product['name'],
                        "category": product['category'],
                        "quantity": quantity,
                        "unit_price": price,
                        "total_price": price * quantity,
                        "discount": discount
                    }
                    records.append(record)
            
            # 保存為CSV
            df = pd.DataFrame(records)
            filename = f"{self.output_dir}/sales_{store['id']}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"生成銷售資料: {filename}, 記錄數: {len(df)}")
        
        # 生成元數據
        with open(f"{self.output_dir}/products.json", "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.output_dir}/stores.json", "w", encoding="utf-8") as f:
            json.dump(stores, f, ensure_ascii=False, indent=2)
        
        logger.info(f"測試資料生成完成，共 {len(stores)} 個門市，{len(date_range)} 天數據")