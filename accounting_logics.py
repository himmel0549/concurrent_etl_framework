
def accounting_transform(df):
    """會計處理函數 - 增加必要的會計計算"""
    # 計算期間彙總資料
    df['period'] = df['date'].dt.strftime('%Y-%m')
    df['post_step'] = 'test'  # 測試用的步驟欄位
    
    return df