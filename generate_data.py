# generate_data.py
import os
import json
import pandas as pd
from datetime import datetime

# --- 配置 ---
CSV_DIR = "flight_data_combined"
OUTPUT_JSON = "data.json"

def generate_chart_data():
    """读取最新的 CSV 文件，生成用于图表的 JSON 数据"""
    try:
        csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not csv_files:
            print("错误：在 'flight_data_combined' 目录中未找到任何 CSV 文件。")
            return None
        
        latest_csv = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        print(f"正在处理最新的文件: {latest_csv}")

        df = pd.read_csv(csv_path)
        flight_counts = df['registration'].value_counts().reset_index()
        flight_counts.columns = ['registration', 'count']
        
        chart_data = {
            "xAxis_data": flight_counts['registration'].tolist(),
            "series_data": flight_counts['count'].tolist(),
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": latest_csv
        }
        return chart_data
    except Exception as e:
        print(f"处理数据时出错: {e}")
        return None

if __name__ == "__main__":
    data = generate_chart_data()
    if data:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"数据已成功生成到: {OUTPUT_JSON}")
