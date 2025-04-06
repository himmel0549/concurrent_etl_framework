# utils/file_utils.py
import os
import pandas as pd

def detect_file_format(file_path):
    """檢測檔案格式並返回適當的讀取與寫入參數
    
    參數:
        file_path: 檔案路徑
        
    返回:
        包含reader, writer和params的字典
    """
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext in ['.csv', '.txt']:
        return {
            'reader': pd.read_csv,
            'writer': 'to_csv',
            'params': {}
        }
    elif ext in ['.xlsx', '.xls']:
        return {
            'reader': pd.read_excel,
            'writer': 'to_excel',
            'params': {'engine': 'openpyxl'}
        }
    elif ext == '.parquet':
        return {
            'reader': pd.read_parquet,
            'writer': 'to_parquet',
            'params': {}
        }
    elif ext == '.pkl' or ext == '.pickle':
        return {
            'reader': pd.read_pickle,
            'writer': 'to_pickle',
            'params': {}
        }
    elif ext == '.json':
        return {
            'reader': pd.read_json,
            'writer': 'to_json',
            'params': {'orient': 'records'}
        }
    elif ext == '.feather':
        return {
            'reader': pd.read_feather,
            'writer': 'to_feather',
            'params': {}
        }
    else:
        raise ValueError(f"不支援的檔案格式: {ext}")