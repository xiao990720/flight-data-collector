import os
import json
import pandas as pd
from datetime import datetime

# --- 配置 ---
# CSV 文件所在的目录
CSV_DIR = "flight_data_combined"
# 输出文件的目录 (GitHub Pages 会从 'docs' 目录发布)
OUTPUT_DIR = "docs"
# 图表 HTML 模板文件
TEMPLATE_FILE = "chart_template.html"
# 输出的 JSON 文件名
JSON_FILE = "daily_summary.json"
# 输出的最终 HTML 文件名
HTML_FILE = "index.html"

def generate_chart_data():
    """
    读取最新的 CSV 文件，生成用于图表的数据
    """
    # 1. 找到最新的 CSV 文件
    try:
        csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not csv_files:
            print("错误：在 'flight_data_combined' 目录中未找到任何 CSV 文件。")
            return None
        
        latest_csv = max(csv_files, key=lambda x: os.path.getmtime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        print(f"正在处理最新的文件: {latest_csv}")
    except Exception as e:
        print(f"查找 CSV 文件时出错: {e}")
        return None

    # 2. 读取 CSV 并分析数据
    try:
        df = pd.read_csv(csv_path)
        
        # 按飞机注册号分组，统计每个飞机的航班次数
        flight_counts = df['registration'].value_counts().reset_index()
        flight_counts.columns = ['registration', 'count']
        
        # 准备图表所需的数据格式
        chart_data = {
            "xAxis_data": flight_counts['registration'].tolist(),
            "series_data": flight_counts['count'].tolist(),
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": latest_csv
        }
        return chart_data
    except Exception as e:
        print(f"处理 CSV 文件时出错: {e}")
        return None

def create_html_page(chart_data):
    """
    使用模板和数据生成最终的 HTML 页面
    """
    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 将数据写入 JSON 文件
    json_path = os.path.join(OUTPUT_DIR, JSON_FILE)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)
    print(f"数据已保存到: {json_path}")

    # 读取 HTML 模板
    try:
        with open(TEMPLATE_FILE, 'r', encoding='utf-8') as f:
            html_template = f.read()
    except FileNotFoundError:
        print(f"错误：模板文件 '{TEMPLATE_FILE}' 未找到。")
        return

    # 将 JSON 数据直接嵌入到 HTML 中，避免跨域问题
    final_html = html_template.replace('/*__JSON_DATA_PLACEHOLDER__*/', json.dumps(chart_data, ensure_ascii=False))
    
    # 写入最终的 HTML 文件
    html_path = os.path.join(OUTPUT_DIR, HTML_FILE)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"可视化页面已生成: {html_path}")

if __name__ == "__main__":
    data = generate_chart_data()
    if data:
        create_html_page(data)
