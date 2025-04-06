# generators/accounting_data.py
from core.interfaces import DataGenerator
from utils.logging import get_logger
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

logger = get_logger(__name__)

class AccountingDataGenerator(DataGenerator):
    """會計資料產生器"""
    def __init__(self, output_dir: str = 'data/accounting/raw'):
        self.output_dir = output_dir
        # 創建輸出目錄
        os.makedirs(output_dir, exist_ok=True)
    
    def generate(self, months: int = 3, transactions_per_day: int = 20, **kwargs) -> None:
        """
        生成會計測試資料
        
        參數:
            months: 生成的歷史數據月數
            transactions_per_day: 每天平均交易數
            **kwargs: 其他參數
        """
        logger.info(f"開始生成會計測試資料，月數：{months}，每日平均交易量：{transactions_per_day}")
        
        # 1. 生成科目列表
        accounts = self._generate_chart_of_accounts()
        
        # 2. 生成傳票與分錄
        end_date = datetime.now().date().replace(day=1) - timedelta(days=1)  # 上月最後一天
        start_date = end_date.replace(day=1) - timedelta(days=(months - 1) * 30)  # 往前推n個月
        
        # 按公司生成資料
        companies = ['A公司', 'B公司', 'C公司']
        
        for company in companies:
            logger.info(f"開始生成 {company} 的會計資料")
            
            # 收集所有月份的傳票
            all_vouchers = []
            all_entries = []
            
            # 按月生成傳票
            current_date = start_date
            while current_date <= end_date:
                year = current_date.year
                month = current_date.month
                
                # 計算該月天數
                if month == 12:
                    next_month = datetime(year + 1, 1, 1)
                else:
                    next_month = datetime(year, month + 1, 1)
                days_in_month = (next_month.date() - current_date).days
                
                # 生成每月的傳票與分錄
                monthly_vouchers, monthly_entries = self._generate_monthly_vouchers(
                    company, year, month, days_in_month, 
                    transactions_per_day, accounts
                )
                
                all_vouchers.extend(monthly_vouchers)
                all_entries.extend(monthly_entries)
                
                # 移至下一月
                if month == 12:
                    current_date = datetime(year + 1, 1, 1).date()
                else:
                    current_date = datetime(year, month + 1, 1).date()
            
            # 保存為xlsx
            vouchers_df = pd.DataFrame(all_vouchers)
            entries_df = pd.DataFrame(all_entries)
            
            company_code = company[0]  # 使用公司名稱第一個字做為代碼
            vouchers_file = f"{self.output_dir}/vouchers_{company_code}.xlsx"
            entries_file = f"{self.output_dir}/entries_{company_code}.xlsx"
            
            vouchers_df.to_excel(vouchers_file, index=False)
            entries_df.to_excel(entries_file, index=False)
            
            logger.info(f"已生成 {company} 的會計資料: {len(all_vouchers)} 筆傳票, {len(all_entries)} 筆分錄")
        
        # 保存科目表
        accounts_df = pd.DataFrame(accounts)
        accounts_df.to_excel(f"{self.output_dir}/chart_of_accounts.xlsx", index=False)
        
        logger.info(f"會計測試資料生成完成，共 {len(companies)} 家公司，{months} 個月的資料")
    
    def _generate_chart_of_accounts(self):
        """生成會計科目表"""
        accounts = [
            # 資產類科目 (1開頭)
            {"code": "1001", "name": "現金", "type": "資產", "sub_type": "流動資產"},
            {"code": "1002", "name": "銀行存款", "type": "資產", "sub_type": "流動資產"},
            {"code": "1122", "name": "應收帳款", "type": "資產", "sub_type": "流動資產"},
            {"code": "1201", "name": "原料", "type": "資產", "sub_type": "流動資產"},
            {"code": "1301", "name": "預付費用", "type": "資產", "sub_type": "流動資產"},
            {"code": "1601", "name": "房屋建築", "type": "資產", "sub_type": "固定資產"},
            {"code": "1602", "name": "機器設備", "type": "資產", "sub_type": "固定資產"},
            {"code": "1603", "name": "辦公設備", "type": "資產", "sub_type": "固定資產"},
            
            # 負債類科目 (2開頭)
            {"code": "2001", "name": "短期借款", "type": "負債", "sub_type": "流動負債"},
            {"code": "2201", "name": "應付帳款", "type": "負債", "sub_type": "流動負債"},
            {"code": "2202", "name": "應付費用", "type": "負債", "sub_type": "流動負債"},
            {"code": "2203", "name": "應付稅金", "type": "負債", "sub_type": "流動負債"},
            {"code": "2501", "name": "長期借款", "type": "負債", "sub_type": "長期負債"},
            
            # 權益類科目 (3開頭)
            {"code": "3001", "name": "實收資本", "type": "權益", "sub_type": "資本"},
            {"code": "3101", "name": "資本公積", "type": "權益", "sub_type": "資本公積"},
            {"code": "3401", "name": "未分配盈餘", "type": "權益", "sub_type": "盈餘"},
            
            # 收入類科目 (4開頭)
            {"code": "4001", "name": "主營業務收入", "type": "收入", "sub_type": "營業收入"},
            {"code": "4101", "name": "其他業務收入", "type": "收入", "sub_type": "營業收入"},
            {"code": "4201", "name": "投資收益", "type": "收入", "sub_type": "營業外收入"},
            {"code": "4301", "name": "利息收入", "type": "收入", "sub_type": "營業外收入"},
            
            # 費用類科目 (5開頭)
            {"code": "5001", "name": "主營業務成本", "type": "費用", "sub_type": "營業成本"},
            {"code": "5101", "name": "銷售費用", "type": "費用", "sub_type": "營業費用"},
            {"code": "5201", "name": "管理費用", "type": "費用", "sub_type": "營業費用"},
            {"code": "5301", "name": "財務費用", "type": "費用", "sub_type": "營業費用"},
            {"code": "5401", "name": "研發費用", "type": "費用", "sub_type": "營業費用"},
        ]
        return accounts
    
    def _generate_monthly_vouchers(self, company, year, month, days_in_month, 
                                   transactions_per_day, accounts):
        """生成每月的傳票與分錄"""
        vouchers = []
        entries = []
        
        # 每天生成若干筆傳票
        for day in range(1, days_in_month + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            
            # 隨機生成當天傳票數量，模擬工作日交易較多的情況
            if day % 7 in [0, 6]:  # 週末
                daily_transactions = int(transactions_per_day * 0.5)
            else:  # 工作日
                daily_transactions = int(transactions_per_day * (0.8 + 0.4 * random.random()))
            
            # 生成當天的傳票
            for t in range(daily_transactions):
                # 生成傳票號
                voucher_id = f"{company[0]}{year}{month:02d}{day:02d}{t+1:03d}"
                
                # 決定傳票類型
                voucher_types = ["收", "付", "轉"]
                weights = [0.3, 0.3, 0.4]  # 轉帳傳票通常較多
                voucher_type = random.choices(voucher_types, weights=weights)[0]
                
                # 生成分錄數量 (通常2-10個分錄)
                entry_count = random.randint(2, 10) if voucher_type == "轉" else random.randint(2, 4)
                
                # 生成傳票基本信息
                voucher = {
                    "voucher_id": voucher_id,
                    "company": company,
                    "date": date_str,
                    "year": year,
                    "month": month,
                    "day": day,
                    "type": voucher_type,
                    "status": "已過帳",
                    "description": self._generate_description(voucher_type),
                    "creator": f"User{random.randint(1, 10)}",
                    "create_time": f"{date_str} {random.randint(8, 17):02d}:{random.randint(0, 59):02d}:00"
                }
                vouchers.append(voucher)
                
                # 生成對應的分錄
                new_entries, total_amount = self._generate_entries(voucher_id, entry_count, 
                                                                   voucher_type, accounts, date_str)
                entries.extend(new_entries)
                
                # 更新傳票金額
                voucher["amount"] = total_amount
                
        return vouchers, entries
    
    def _generate_entries(self, voucher_id, entry_count, voucher_type, accounts, date_str):
        """生成傳票分錄"""
        entries = []
        
        # 按傳票類型選擇相應的科目
        account_code = ["1122", "2201"]
        if voucher_type == "收":  # 收款傳票
            debit_accounts = [acc for acc in accounts if acc["code"] in ["1001", "1002"]]  # 現金/銀行
            credit_accounts = [acc for acc in accounts if acc["code"].startswith("4") or acc["code"] in account_code]
        elif voucher_type == "付":  # 付款傳票
            debit_accounts = [acc for acc in accounts if acc["code"].startswith("5") or acc["code"] in account_code]
            credit_accounts = [acc for acc in accounts if acc["code"] in ["1001", "1002"]]  # 現金/銀行
        else:  # 轉賬傳票
            debit_accounts = accounts.copy()
            credit_accounts = accounts.copy()
        
        # 決定總金額範圍 (根據傳票類型調整金額範圍)
        if voucher_type == "轉":
            total_amount = random.randint(10000, 1000000) / 100  # 100.00 - 10,000.00
        else:
            total_amount = random.randint(1000, 100000) / 100  # 10.00 - 1,000.00
        
        # 生成借方分錄
        debit_entries = entry_count // 2  # 大約一半是借方
        debit_amounts = self._split_amount(total_amount, debit_entries)
        
        for i in range(debit_entries):
            acc = random.choice(debit_accounts)
            entry = {
                "voucher_id": voucher_id,
                "entry_id": f"{voucher_id}-{i+1:02d}",
                "date": date_str,  # 新增日期欄位
                "account_code": acc["code"],
                "account_name": acc["name"],
                "account_type": acc["type"],
                "direction": "借",
                "amount": debit_amounts[i],
                "description": self._generate_entry_description(acc["code"]),
            }
            entries.append(entry)
        
        # 生成貸方分錄
        credit_entries = entry_count - debit_entries
        credit_amounts = self._split_amount(total_amount, credit_entries)
        
        for i in range(credit_entries):
            acc = random.choice(credit_accounts)
            entry = {
                "voucher_id": voucher_id,
                "entry_id": f"{voucher_id}-{debit_entries+i+1:02d}",
                "date": date_str,  # 新增日期欄位
                "account_code": acc["code"],
                "account_name": acc["name"],
                "account_type": acc["type"],
                "direction": "貸",
                "amount": credit_amounts[i],
                "description": self._generate_entry_description(acc["code"]),
            }
            entries.append(entry)
        
        return entries, total_amount
    
    def _generate_description(self, voucher_type):
        """生成傳票摘要"""
        if voucher_type == "收":
            descriptions = [
                "收取貨款", "收取服務費", "客戶預付款", "收取租金", 
                "收回借款", "投資收益", "利息收入"
            ]
        elif voucher_type == "付":
            descriptions = [
                "支付貨款", "支付服務費", "預付供應商款項", "支付租金", 
                "支付工資", "支付水電費", "支付差旅費", "支付廣告費"
            ]
        else:  # 轉賬
            descriptions = [
                "計提折舊", "結轉成本", "轉銷應收帳款", "轉銷應付帳款", 
                "提取公積金", "資產報廢", "費用報銷", "內部轉賬"
            ]
        
        return random.choice(descriptions)
    
    def _generate_entry_description(self, account_code):
        """根據科目生成分錄說明"""
        # 可以根據科目代碼判斷生成對應的說明
        prefix = account_code[:2]
        
        if prefix == "10":  # 現金類
            return random.choice(["現金收入", "現金支出", "備用金調整"])
        elif prefix == "11":  # 應收類
            return random.choice(["應收貨款", "客戶結算", "銷售商品"])
        elif prefix == "16":  # 固定資產
            return random.choice(["購置設備", "資產折舊", "資產維修"])
        elif prefix == "22":  # 應付類
            return random.choice(["應付貨款", "供應商結算", "採購商品"])
        elif prefix == "40":  # 收入類
            return random.choice(["銷售收入", "服務收入", "產品銷售"])
        elif prefix == "50":  # 成本類
            return random.choice(["產品成本", "銷售成本", "原料成本"])
        elif prefix == "51" or prefix == "52":  # 費用類
            return random.choice(["辦公費用", "差旅費用", "水電費用", "廣告費用"])
        else:
            return "其他業務"
    
    def _split_amount(self, total, parts):
        """將總金額分成若干個部分，保證每個部分都是兩位小數"""
        if parts <= 0:
            return []
        
        if parts == 1:
            return [round(total, 2)]
        
        # 生成隨機比例
        ratios = [random.random() for _ in range(parts)]
        sum_ratio = sum(ratios)
        ratios = [r / sum_ratio for r in ratios]
        
        # 按比例分配金額
        amounts = [round(total * r, 2) for r in ratios]
        
        # 由於四捨五入可能導致總和不一致，調整最後一個金額
        diff = round(total - sum(amounts[:-1]), 2)
        amounts[-1] = diff
        
        return amounts