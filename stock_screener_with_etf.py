# stock_screener_with_etf.py
# 小资金A股股票+ETF筛选器：基于MACD/RSI/突破/放量，低价股/ETF（2-20元），放松MACD到3日过滤
# 适合1.5万本金、低耐心用户，中频短线/波段策略
# 作者：Grok（基于用户需求生成）
# 更新：加入ETF筛选（价格2-20元，名称含ETF），并加入连接重试机制（tenacity）
# 使用：python stock_screener_with_etf.py
# 输出：符合条件的股票/ETF列表

import akshare as ak
import pandas as pd
import pandas_ta as ta
import datetime
import numpy as np
# 引入 tenacity 用于处理网络连接重试
from tenacity import retry, stop_after_attempt, wait_exponential
import requests # 确保 requests 可用，tenacity可能依赖它来捕获ConnectionError

# 配置（调整价格范围适合ETF）
MIN_PRICE = 2.0  # 最低价（ETF多低价）
MAX_PRICE = 20.0  # 最高价
DAYS = 120  # 历史数据天数
MACD_DAYS = 3  # MACD过滤：连续3天DIF>0
VOLUME_MULT = 2.5  # 放量倍数
BREAKOUT_MULT = 1.01  # 突破前高倍数
DAILY_RETURN_MIN = 0.015  # 当日涨幅>1.5%
RSI_MIN = 65  # RSI>65（强势）
AVOID_BOARDS = ['688', '300']  # 避开科创/创业板股票（ETF不受影响）

# 使用 @retry 装饰器处理网络连接错误
@retry(
    # 尝试 5 次
    stop=stop_after_attempt(5),
    # 使用指数退避等待时间：2s, 4s, 8s, 16s...
    wait=wait_exponential(multiplier=1, min=2, max=30),
    # 捕获requests库的ConnectionError（包括底层的ProtocolError）进行重试
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
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=(datetime.date.today() - datetime.timedelta(days=days*2)).strftime("%Y%m%d"), end_date=datetime.date.today().strftime("%Y%m%d"), adjust="qfq")
        df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量']]
        df.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume']
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values('Date', inplace=True)
        return df
    except:
        return pd.DataFrame()

def calculate_indicators(df):
    """计算指标：MA、MACD、RSI、Volume MA"""
    if len(df) < 60:
        return df
    df['MA5'] = ta.sma(df['Close'], length=5)
    df['MA20'] = ta.sma(df['Close'], length=20)
    macd = ta.macd(df['Close'], fast=12, slow=26, signal=9)
    df['DIF'] = macd['MACD_12_26_9']
    df['RSI'] = ta.rsi(df['Close'], length=14)
    df['MA5V'] = ta.sma(df['Volume'], length=5)
    df['Max_High_20'] = df['High'].rolling(20).max().shift(1)  # 前20日最高
    return df

def check_conditions(df, code, name):
    """检查筛选条件，返回信号类型或None"""
    if df.empty or len(df) < DAYS:
        return None
    df = calculate_indicators(df) # 确保计算了指标
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None

    # 价格过滤：2-20元，非科创/创业股票（ETF自动包括）
    # 注意：这里需要确保 latest['Close'] 和 Max_High_20 有值
    if pd.isna(latest['Close']) or pd.isna(latest['RSI']) or pd.isna(latest['DIF']):
        return None
    
    if not (MIN_PRICE <= latest['Close'] <= MAX_PRICE) or any(code.startswith(board) for board in AVOID_BOARDS if not ('ETF' in name.upper())):
        return None

    # 当日涨幅>1.5%
    if prev is None or (latest['Close'] - prev['Close']) / prev['Close'] < DAILY_RETURN_MIN:
        return None

    # RSI强势>65
    if latest['RSI'] < RSI_MIN:
        return None

    # MACD 3日>0轴
    if len(df) < MACD_DAYS or not all(df['DIF'].tail(MACD_DAYS) > 0):
        return None

    # 放量：Volume > MA5V * 2.5
    if pd.isna(latest['MA5V']) or latest['Volume'] <= latest['MA5V'] * VOLUME_MULT:
        return None

    # 突破前高：Close > Max_High_20 * 1.01
    if pd.isna(latest['Max_High_20']) or latest['Close'] <= latest['Max_High_20'] * BREAKOUT_MULT:
        return None

    # 额外：检查是否有近期大涨（ETF用>5%模拟涨停）
    had_big_rise = False
    limit_threshold = 0.05  # ETF少涨停，用5%+
    # 检查前6个交易日（不含今日）
    for i in range(-6, -1):
        if len(df) + i >= 0 and len(df) + i - 1 >= 0:
            day = df.iloc[i]
            prev_day = df.iloc[i-1]
            if not pd.isna(prev_day['Close']) and (day['Close'] - prev_day['Close']) / prev_day['Close'] >= limit_threshold:
                had_big_rise = True
                break

    # 判断是股票还是ETF
    is_etf = 'ETF' in name.upper() or '易方达' in name or '华夏' in name or '南方' in name # 增强ETF识别
    signal = "突破放量强势股" if not is_etf else "突破放量强势ETF"
    if had_big_rise:
        signal = ("龙头二次启动" if not is_etf else "ETF二次启动")

    return {
        '代码': code,
        '名称': name,
        '当前价': latest['Close'],
        '信号类型': signal,
        'RSI': round(latest['RSI'], 2),
        'DIF': round(latest['DIF'], 3),
        '涨幅%': round((latest['Close'] - prev['Close']) / prev['Close'] * 100, 2) if prev is not None else 0,
        '类型': 'ETF' if is_etf else '股票'
    }

def main():
    print("开始筛选A股股票+ETF...")
    try:
        stock_list = get_stock_list()
    except requests.exceptions.ConnectionError as e:
        print(f"❌ 严重错误：经过多次重试，仍无法获取股票列表。请检查网络或数据源。错误信息: {e}")
        # 如果重试后仍失败，主程序退出
        return
        
    results = []
    total_stocks = len(stock_list)

    # 遍历所有（包括ETF）
    for idx, row in stock_list.iterrows():
        code = row['代码']
        name = row['名称']
        
        # 跳过不在价格范围内的股票，提前过滤大部分不符合条件的
        # (这只是粗略过滤，精确过滤在 check_conditions 中)
        # if not ('ETF' in name.upper() or code.startswith('51') or code.startswith('15')):
        #     if code.startswith('688') or code.startswith('300'):
        #         continue
                
        df = get_stock_data(code)
        
        # 仅在数据获取成功且足够时才进行计算和检查
        if not df.empty and len(df) >= DAYS:
            df = calculate_indicators(df)
            result = check_conditions(df, code, name)
            if result:
                results.append(result)
                
        if (idx + 1) % 500 == 0:
            print(f"已处理 {idx + 1}/{total_stocks} 只股票/ETF...")

    print(f"✅ 筛选完成。共处理 {total_stocks} 只股票/ETF。")

    if results:
        results_df = pd.DataFrame(results)
        results_df.sort_values('涨幅%', ascending=False, inplace=True)
        print("\n🎉 符合条件的股票/ETF：")
        print(results_df.to_string(index=False))
        results_df.to_csv('screened_stocks_etf.csv', index=False, encoding='utf-8-sig')
        print("\n💾 已保存到 screened_stocks_etf.csv")
    else:
        print("今日无符合条件的股票/ETF。市场弱势？明日再试！")

if __name__ == "__main__":
    main()
