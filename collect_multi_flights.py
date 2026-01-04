# -*- coding: utf-8 -*-
"""
多飞机航班信息采集脚本 (合并输出到单个CSV文件)
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
import schedule
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# --- 1. 配置区 ---
# 将所有需要采集的飞机注册号放在这个列表中
TARGET_AIRCRAFT_LIST = [
    # 原始列表
    #"B-919A", "B-919C", "B-919D", "B-919E", "B-919F", "B-919G", "B-919H",
    "B-657S", "B-657T",
    #"B-658E", "B-658U", "B-658V",
   # "B-659Z",
   # "B-65A0",
    # 新增列表
    #"B-919X", "B-919Y", "B-919Z", "B-919J",
    #"B-657J", "B-657X",
    #"B-658H", "B-658J", "B-658Q", "B-658R", "B-658N", "B-658P", "B-658X", "B-658W", "B-658Y",
    #"B-6593", "B-659K"
]

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("flight_collector.log", 'a', 'utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 主输出目录
BASE_OUTPUT_DIR = "flight_data_combined"
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
# --- 配置结束 ---

def collect_single_aircraft(aircraft_reg, date_str):
    """
    采集单个飞机在指定日期的航班数据
    """
    logger.info(f"--- 开始从 FlightAware 采集 {aircraft_reg} 在 {date_str} 的航班 ---")
    
    flightaware_id = aircraft_reg.replace('-', '')
    url = f"https://www.flightaware.com/live/flight/{flightaware_id}"
    
    flights = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=50)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            logger.info(f"正在加载页面: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=180000) 
            
            flight_row_selector = "div.flightPageDataRowTall"
            logger.info(f"等待航班记录行加载...")
            page.wait_for_selector(flight_row_selector, timeout=60000)

            time.sleep(3)

            content = page.content()
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            all_rows = soup.select(flight_row_selector)
            logger.info(f"页面上共找到 {len(all_rows)} 条航班记录。")

            for row in all_rows:
                date_div = row.select_one('div.flightPageActivityLogDate')
                if not date_div:
                    continue
                
                date_text = date_div.get_text(strip=True)[-11:]
                
                info_divs = row.select('div.flightPageActivityLogData')[1:3]
                if len(info_divs) < 2:
                    continue
                    
                dep_div, arr_div = info_divs
                
                dep_airport = dep_div.select_one('a').text.strip() if dep_div.select_one('a') else 'N/A'
                dep_time = dep_div.select_one('span.noWrapTime').text.strip() if dep_div.select_one('span.noWrapTime') else 'N/A'
                arr_airport = arr_div.select_one('a').text.strip() if arr_div.select_one('a') else 'N/A'
                arr_time = arr_div.select_one('span.noWrapTime').text.strip() if arr_div.select_one('span.noWrapTime') else 'N/A'
                
                flight_info = {
                    'registration': aircraft_reg,
                    'date': date_text,
                    'departure_airport': dep_airport,
                    'departure_time': dep_time,
                    'arrival_airport': arr_airport,
                    'arrival_time': arr_time,
                    'status': 'Completed'
                }
                flights.append(flight_info)
            
            logger.info(f"数据提取完成，共提取 {len(flights)} 条记录。")

        except PlaywrightTimeoutError:
            logger.error(f"采集 {aircraft_reg} 时页面加载或元素等待超时。")
        except Exception as e:
            if "net::ERR_CONNECTION_CLOSED" in str(e) or "Page.goto" in str(e):
                logger.error(f"采集 {aircraft_reg} 时发生网络连接错误: {e}. 服务器可能中断了连接。")
            else:
                logger.error(f"采集 {aircraft_reg} 时发生未知错误: {e}", exc_info=True)
        finally:
            browser.close()

    filtered_flights = []
    try:
        target_date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        target_date_str_dmy = target_date_obj.strftime('%d-%b-%Y')
        target_date_str_dmy_no_zero = target_date_obj.strftime('%-d-%b-%Y')
        
        filtered_flights = [f for f in flights if f['date'] in (target_date_str_dmy, target_date_str_dmy_no_zero)]
        logger.info(f"从历史记录中筛选出 {date_str} 的航班: {len(filtered_flights)} 条。")
    except ValueError:
        logger.error(f"日期格式错误: {date_str}")

    return filtered_flights

def main(date_str=None):
    """
    主函数，循环处理飞机列表，将所有结果合并到一个文件中
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"===== 开始批量采集 {date_str} 的航班信息 (合并输出) =====")
    
    all_flights_data = []
    
    for i, aircraft_reg in enumerate(TARGET_AIRCRAFT_LIST):
        results = collect_single_aircraft(aircraft_reg, date_str)
        
        if results:
            logger.info(f"将 {aircraft_reg} 的 {len(results)} 条航班数据添加到总列表中...")
            all_flights_data.extend(results)
        else:
            logger.warning(f"{aircraft_reg} 在 {date_str} 未采集到任何航班数据。")
            
        if i < len(TARGET_AIRCRAFT_LIST) - 1:
            delay_time = random.randint(15, 30)
            logger.info(f"--- 等待 {delay_time} 秒后，继续采集下一个飞机... ---")
            time.sleep(delay_time)
            
    if all_flights_data:
        logger.info(f"===== 所有飞机采集完成，共汇总 {len(all_flights_data)} 条航班数据。准备写入文件... =====")
        df = pd.DataFrame(all_flights_data)
        filename = os.path.join(BASE_OUTPUT_DIR, f"all_flights_{date_str}.csv")
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"所有数据已成功合并并保存至: {filename}")
    else:
        logger.warning("===== 所有飞机采集完成，但未采集到任何航班数据。不生成文件。=====")
        
    logger.info(f"===== 所有飞机采集任务结束 =====")
    print("-" * 50)

if __name__ == "__main__":
    # --- 运行方式 1: 单次运行 ---
    main() # 采集今天
    # main("2026-01-04") # 采集指定日期

    # --- 运行方式 2: 定时任务 ---
    # def scheduled_job():
    #     yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    #     main(yesterday)

   #  schedule.every().day.at("01:00").do(scheduled_job)
    # logger.info(f"定时任务已启动，将在每天凌晨1点自动采集所有飞机前一天的航班数据...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)
