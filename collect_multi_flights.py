# -*- coding: utf-8 -*-
"""
多飞机航班信息采集脚本 (合并输出到单个CSV文件)
修复点：
1. 移除无限定时循环，适配GitHub Actions一次性执行特性
2. 修复PlaywrightTimeoutError异常引用
3. 复用浏览器实例缩短执行时间
4. 强制进程退出避免残留
5. 增加atexit清理浏览器进程
"""

import os
import sys
import time
import random
import logging
import atexit
from datetime import datetime, timedelta

import pandas as pd
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

# --- 1. 配置区 ---
# 目标飞机注册号列表
TARGET_AIRCRAFT_LIST = [
   # "B-919A", "B-919C", "B-919D", "B-919E", "B-919F", "B-919G", "B-919H",
   # "B-657S", "B-657T",
   # "B-658E", "B-658U", "B-658V",
    #"B-659Z",
    #"B-65A0",
    "B-919X", "B-919Y", "B-919Z", "B-919J",
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

# 全局浏览器实例（用于复用）
browser = None
context = None
page = None

# --- 2. 清理函数（确保进程退出） ---
def cleanup_resources():
    """程序退出时清理浏览器资源"""
    global browser, context, page
    logger.info("开始清理浏览器资源...")
    try:
        if page:
            page.close()
        if context:
            context.close()
        if browser:
            browser.close()
        logger.info("浏览器资源清理完成")
    except Exception as e:
        logger.error(f"清理资源时出错: {e}", exc_info=True)

# 注册退出清理函数
atexit.register(cleanup_resources)

# --- 3. 核心采集函数 ---
def collect_single_aircraft(aircraft_reg, date_str):
    """
    采集单个飞机在指定日期的航班数据（复用全局浏览器实例）
    """
    global page
    logger.info(f"--- 开始采集 {aircraft_reg} 在 {date_str} 的航班 ---")
    
    flightaware_id = aircraft_reg.replace('-', '')
    url = f"https://www.flightaware.com/live/flight/{flightaware_id}"
    
    flights = []
    
    try:
        logger.info(f"加载页面: {url}")
        # 页面跳转（设置120秒超时，避免卡壳）
        page.goto(url, wait_until="domcontentloaded", timeout=120000)
        
        # 等待航班数据行加载
        flight_row_selector = "div.flightPageDataRowTall"
        logger.info("等待航班记录行加载...")
        page.wait_for_selector(flight_row_selector, timeout=60000)
        
        # 短暂等待动态数据加载
        time.sleep(3)
        
        # 解析页面数据
        content = page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        
        all_rows = soup.select(flight_row_selector)
        logger.info(f"找到 {len(all_rows)} 条航班记录")
        
        # 提取每条航班信息
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
            
            flights.append({
                'registration': aircraft_reg,
                'date': date_text,
                'departure_airport': dep_airport,
                'departure_time': dep_time,
                'arrival_airport': arr_airport,
                'arrival_time': arr_time,
                'status': 'Completed'
            })
        
        logger.info(f"提取 {len(flights)} 条原始记录")

    except PlaywrightTimeoutError:
        logger.error(f"{aircraft_reg} 页面加载/元素等待超时")
    except Exception as e:
        logger.error(f"{aircraft_reg} 采集失败: {e}", exc_info=True)
    
    # 筛选目标日期的航班
    filtered_flights = []
    try:
        target_date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        target_date_str_dmy = target_date_obj.strftime('%d-%b-%Y')
        target_date_str_dmy_no_zero = target_date_obj.strftime('%-d-%b-%Y')
        filtered_flights = [f for f in flights if f['date'] in (target_date_str_dmy, target_date_str_dmy_no_zero)]
        logger.info(f"{aircraft_reg} 筛选出 {len(filtered_flights)} 条 {date_str} 的航班")
    except ValueError:
        logger.error(f"日期格式错误: {date_str}")
    
    return filtered_flights

# --- 4. 主函数 ---
def main(date_str=None):
    """
    主执行函数：初始化浏览器 + 批量采集 + 生成CSV
    """
    global browser, context, page
    
    # 默认采集前一天数据（适配定时执行）
    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"===== 开始批量采集 {date_str} 航班数据 =====")
    all_flights_data = []
    
    # 初始化Playwright浏览器（全局复用）
    with sync_playwright() as p:
        logger.info("启动Chromium浏览器...")
        browser = p.chromium.launch(headless=True, slow_mo=50)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        # 遍历所有飞机采集数据
        for i, aircraft_reg in enumerate(TARGET_AIRCRAFT_LIST):
            # 采集单架飞机数据
            results = collect_single_aircraft(aircraft_reg, date_str)
            
            if results:
                logger.info(f"添加 {aircraft_reg} 的 {len(results)} 条数据到总列表")
                all_flights_data.extend(results)
            else:
                logger.warning(f"{aircraft_reg} 未采集到 {date_str} 的数据")
            
            # 非最后一架飞机，添加随机延时（降低请求频率）
            if i < len(TARGET_AIRCRAFT_LIST) - 1:
                delay_time = random.randint(5, 10)  # 缩短延时至5-10秒
                logger.info(f"等待 {delay_time} 秒后采集下一架飞机...")
                time.sleep(delay_time)
    
    # 生成合并CSV文件
    if all_flights_data:
        logger.info(f"===== 采集完成，共 {len(all_flights_data)} 条数据 =====")
        df = pd.DataFrame(all_flights_data)
        csv_path = os.path.join(BASE_OUTPUT_DIR, f"all_flights_{date_str}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"数据已保存至: {csv_path}")
    else:
        logger.warning("===== 未采集到任何航班数据，不生成文件 =====")
    
    logger.info(f"===== 所有采集任务结束 =====")
    # 强制退出进程，避免GitHub Actions超时
    sys.exit(0)

# --- 5. 执行入口 ---
if __name__ == "__main__":
    # 仅执行单次采集（无无限循环）
    main()
