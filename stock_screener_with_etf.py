# stock_screener_with_etf.py
# 小资金A股股票+ETF筛选器：基于MACD/RSI/突破/放量，低价股/ETF（2-20元），放松MACD到3日过滤
# 适合1.5万本金、低耐心用户，中频短线/波段策略
# 作者：Grok（基于用户需求生成）
# 更新：加入ETF筛选，多线程并行加速，并加入连接重试机制（tenacity）
# 使用：python stock_screener_with_etf.py
# 输出：符合条件的股票/ETF列表

import akshare as ak
import pandas as pd
import pandas_ta as ta
import datetime
import numpy as np
import requests
import concurrent.futures # 🚀 用于多线程加速
from tenacity import retry, stop_after_attempt, wait_exponential

# =========================================================
# 配置
# =========================================================
MIN_PRICE = 2.0  # 最低价（ETF多低价）
MAX_PRICE = 20.0  # 最高价
DAYS = 120  # 历史数据天数
MACD_DAYS = 3  # MACD过滤：连续3天DIF>0
VOLUME_MULT = 2.5  # 放量倍数
BREAKOUT_MULT = 1.01  # 突破前高倍数
DAILY_RETURN_MIN = 0.015  # 当日涨幅>1.5%
RSI_MIN = 65  # RSI>65（强势）
AVOID_BOARDS = ['688', '300']  # 避开科创/创业板股票（ETF不受影响）
MAX_WORKERS = 32 # 🚀 多线程数，用于加速数据获取，可根据需要调整 (32-64)

# =========================================================
# 数据获取 (含重试)
# =========================================================

# 使用 @retry 装饰器处理网络连接错误
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=(requests.exceptions.ConnectionError) 
)
def get_stock_list():
    """获取A股所有现货列表（包括股票+ETF），并自动重试处理网络错误"""
    print("尝试获取股票/ETF列表...")
    stock_list = ak.stock_zh_a_spot_em()
    return stock_list[['代码', '名称']]

def get_stock_data(code, days=DAYS):
    """获取单只股票/ETF历史数据"""
    try:
        # 只请求所需数据日期范围，减少网络负载
        start_date_str = (datetime.date.today() - datetime.timedelta(days=days*2)).strftime("%Y%m%d")
        end_date_str = datetime.date.today().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(
            symbol=code, 
            period="daily", 
            start_date=start_date_str, 
            end_date=end_date_str, 
            adjust="qfq"
        )
        df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量']]
        df.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume']
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values('Date', inplace=True)
        return df.iloc[-DAYS:] # 只保留最近 DAYS 天的数据用于计算
    except Exception as e:
        # print(f"获取 {code} 数据失败: {e}")
        return pd.DataFrame()

# =========================================================
# 指标计算与筛选逻辑
# =========================================================

def calculate_indicators(df):
    """计算指标：MA、MACD、RSI、Volume MA"""
    if len(df) < 60:
        return df
    
    # 尽可能使用 pandas_ta 的内置功能来提高性能
    df.ta.macd(append=True)
    df.rename(columns={'MACD_12_26_9': 'DIF'}, inplace=True) # 使用 DIF 命名保持一致性
    df.ta.rsi(append=True)
    df.rename(columns={'RSI_14': 'RSI'}, inplace=True) 
    
    df['MA5V'] = ta.sma(df['Volume'], length=5)
    # 前20日最高价，并使用 shift(1) 确保不包含当日高点
    df['Max_High_20'] = df['High'].rolling(20).max().shift(1) 
    return df

def check_conditions(df, code, name):
    """检查筛选条件，返回信号类型或None"""
    
    # 确保有足够的指标数据，至少 DAYS 天 + 26 天指标计算窗口
    if df.empty or len(df) < 60:
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None

    # 检查计算结果是否是 NaN (可能是数据量不足导致的)
    if pd.isna(latest['Close']) or pd.isna(latest['RSI']) or pd.isna(latest['DIF']):
        return None
        
    # 价格过滤：2-20元，非科创/创业股票
    is_etf = 'ETF' in name.upper() or code.startswith(('51', '15', '56')) # 增加ETF代码前缀识别
    if not (MIN_PRICE <= latest['Close'] <= MAX_PRICE):
        return None
        
    # 避开科创/创业板股票 (ETF不受影响)
    if not is_etf and any(code.startswith(board) for board in AVOID_BOARDS):
        return None

    # 当日涨幅>1.5%
    if prev is None or (latest['Close'] - prev['Close']) / prev['Close'] < DAILY_RETURN_MIN:
        return None

    # RSI强势>65
    if latest['RSI'] < RSI_MIN:
        return None

    # MACD 3日>0轴
    # 检查 MACD_DAYS 天的 DIF 都是正数
    if len(df) < MACD_DAYS or not all(df['DIF'].tail(MACD_DAYS) > 0):
        return None

    # 放量：Volume > MA5V * 2.5
    if pd.isna(latest['MA5V']) or latest['Volume'] <= latest['MA5V'] * VOLUME_MULT:
        return None

    # 突破前高：Close > Max_High_20 * 1.01
    if pd.isna(latest['Max_High_20']) or latest['Close'] <= latest['Max_High_20'] * BREAKOUT_MULT:
        return None

    # 额外：检查是否有近期大涨 (ETF用>5%模拟涨停)
    had_big_rise = False
    limit_threshold = 0.05
    # 检查前6个交易日（不含今日）
    for i in range(-6, -1):
        if len(df) + i >= 0 and len(df) + i - 1 >= 0:
            day = df.iloc[i]
            prev_day = df.iloc[i-1]
            if not pd.isna(prev_day['Close']) and (day['Close'] - prev_day['Close']) / prev_day['Close'] >= limit_threshold:
                had_big_rise = True
                break

    signal = "突破放量强势股" if not is_etf else "突破放量强势ETF"
    if had_big_rise:
        signal = ("龙头二次启动" if not is_etf else "ETF二次启动")

    return {
        '代码': code,
        '名称': name,
        '当前价': round(latest['Close'], 2),
        '信号类型': signal,
        'RSI': round(latest['RSI'], 2),
        'DIF': round(latest['DIF'], 3),
        '涨幅%': round((latest['Close'] - prev['Close']) / prev['Close'] * 100, 2) if prev is not None else 0,
        '类型': 'ETF' if is_etf else '股票'
    }

def process_stock(code, name):
    """用于多线程处理单个股票/ETF，返回筛选结果或 None"""
    df = get_stock_data(code)
    # calculate_indicators 已在 check_conditions 内部调用 (或者在 check_conditions 前调用，取决于您的选择)
    # 为了避免在多线程中多次计算，这里先计算指标
    if not df.empty and len(df) >= 60:
         df = calculate_indicators(df)
         return check_conditions(df, code, name)
    return None

# =========================================================
# 主函数 (多线程并行)
# =========================================================

def main():
    print("🚀 开始筛选A股股票+ETF (多线程加速中)...")
    
    # 1. 获取股票列表 (含重试)
    try:
        stock_list = get_stock_list()
    except requests.exceptions.ConnectionError as e:
        print(f"❌ 严重错误：经过多次重试，仍无法获取股票列表。请检查网络或数据源。错误信息: {e}")
        return
        
    results = []
    total_stocks = len(stock_list)
    print(f"待处理股票/ETF总数: {total_stocks}")

    # 2. 使用多线程加速数据获取和筛选
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 将所有股票/ETF的任务提交给线程池
        futures = {
            executor.submit(process_stock, row['代码'], row['名称']): row['代码'] 
            for _, row in stock_list.iterrows()
        }
        
        # 实时获取和处理结果
        processed_count = 0
        for future in concurrent.futures.as_completed(futures):
            processed_count += 1
            result = future.result()
            if result:
                results.append(result)
                
            # 每处理 500 个打印一次进度
            if processed_count % 500 == 0:
                print(f"🔄 已处理进度: {processed_count}/{total_stocks}...")
    
    print(f"✅ 筛选完成。共处理 {total_stocks} 只股票/ETF。")

    # 3. 输出结果
    if results:
        results_df = pd.DataFrame(results)
        results_df.sort_values('涨幅%', ascending=False, inplace=True)
        # 重新排序并选择最终需要的列，保证输出整洁
        results_df = results_df[['代码', '名称', '类型', '信号类型', '当前价', '涨幅%', 'RSI', 'DIF']]
        
        print("\n🎉 符合条件的股票/ETF：")
        print(results_df.to_string(index=False))
        results_df.to_csv('screened_stocks_etf.csv', index=False, encoding='utf-8-sig')
        print("\n💾 已保存到 screened_stocks_etf.csv")
    else:
        print("今日无符合条件的股票/ETF。市场弱势？明日再试！")

if __name__ == "__main__":
    main()
