# generate_data.py
import os
import json
import pandas as pd
from datetime import datetime

# --- 配置 ---
CSV_DIR = "flight_data_combined"
OUTPUT_JSON = "data.json"

def generate_chart_data():
    """
    读取最新的 CSV 文件，生成用于图表的 JSON 数据。
    如果找不到 CSV 文件，则返回一个包含默认空数据的字典。
    """
    try:
        # 检查目录是否存在
        if not os.path.isdir(CSV_DIR):
            print(f"警告：目录 '{CSV_DIR}' 不存在。")
            return None

        csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"警告：在 '{CSV_DIR}' 目录中未找到任何 CSV 文件。将生成空数据。")
            return None
        
        latest_csv = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        print(f"正在处理最新的文件: {latest_csv}")

        df = pd.read_csv(csv_path)
        flight_counts = df['registration'].value_counts().reset_index()
        flight_counts.columns = ['registration', 'count']
        
        return {
            "xAxis_data": flight_counts['registration'].tolist(),
            "series_data": flight_counts['count'].tolist(),
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": latest_csv
        }

    except FileNotFoundError:
        print(f"错误：找不到文件或目录。")
        return None
    except pd.errors.EmptyDataError:
        print(f"错误：CSV 文件为空。")
        return None
    except Exception as e:
        print(f"处理数据时发生未知错误: {e}")
        return None

if __name__ == "__main__":
    # 尝试生成数据
    chart_data = generate_chart_data()

    # 如果成功生成数据，则使用它；否则，使用默认的空数据
    if chart_data is not None:
        final_data = chart_data
    else:
        final_data = {
            "xAxis_data": [],
            "series_data": [],
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": "No data available"
        }

    # 确保无论如何都生成一个 JSON 文件
    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"数据已成功生成到: {OUTPUT_JSON}")
    except Exception as e:
        print(f"写入 JSON 文件时出错: {e}")
        # 如果连 JSON 文件都写不了，那就真的失败了，以错误码退出
        exit(1)

    # 如果一切顺利，以成功码 0 退出
    exit(0)
