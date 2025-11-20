# stock_screener_with_etf.py
# 小资金A股股票+ETF筛选器：基于MACD/RSI/突破/放量，低价股/ETF（2-20元），放松MACD到3日过滤
# 适合1.5万本金、低耐心用户，中频短线/波段策略
# 作者：Grok（基于用户需求生成）
# 更新：加入ETF筛选（价格2-20元，名称含ETF）
# 使用：python stock_screener_with_etf.py
# 输出：符合条件的股票/ETF列表

import akshare as ak
import pandas as pd
import pandas_ta as ta
import datetime
import numpy as np

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

def get_stock_list():
    """获取A股所有现货列表（包括股票+ETF）"""
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
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None

    # 价格过滤：2-20元，非科创/创业股票（ETF自动包括）
    if not (MIN_PRICE <= latest['Close'] <= MAX_PRICE) or any(code.startswith(board) for board in AVOID_BOARDS):
        return None

    # 当日涨幅>1.5%
    if prev and (latest['Close'] - prev['Close']) / prev['Close'] < DAILY_RETURN_MIN:
        return None

    # RSI强势>65
    if latest['RSI'] < RSI_MIN:
        return None

    # MACD 3日>0轴
    if not all(df['DIF'].tail(MACD_DAYS) > 0):
        return None

    # 放量：Volume > MA5V * 2.5
    if latest['Volume'] <= latest['MA5V'] * VOLUME_MULT:
        return None

    # 突破前高：Close > Max_High_20 * 1.01
    if latest['Close'] <= latest['Max_High_20'] * BREAKOUT_MULT:
        return None

    # 额外：检查是否有近期大涨（ETF用>5%模拟涨停）
    had_big_rise = False
    limit_threshold = 0.05  # ETF少涨停，用5%+
    for i in range(-6, -1):
        if i + len(df) >= 0:
            day = df.iloc[i]
            prev_day = df.iloc[i-1] if i-1 + len(df) >= 0 else None
            if prev_day and (day['Close'] - prev_day['Close']) / prev_day['Close'] >= limit_threshold:
                had_big_rise = True
                break

    # 判断是股票还是ETF
    is_etf = 'ETF' in name.upper()
    signal = "突破放量强势股" if not is_etf else "突破放量强势ETF"
    if had_big_rise:
        signal = ("龙头二次启动" if not is_etf else "ETF二次启动")

    return {
        '代码': code,
        '名称': name,
        '当前价': latest['Close'],
        '信号类型': signal,
        'RSI': latest['RSI'],
        'DIF': latest['DIF'],
        '涨幅%': round((latest['Close'] - prev['Close']) / prev['Close'] * 100, 2) if prev else 0,
        '类型': 'ETF' if is_etf else '股票'
    }

def main():
    print("开始筛选A股股票+ETF...")
    stock_list = get_stock_list()
    results = []

    # 遍历所有（包括ETF）
    for idx, row in stock_list.iterrows():
        code = row['代码']
        name = row['名称']
        df = get_stock_data(code)
        result = check_conditions(df, code, name)
        if result:
            results.append(result)
        if idx % 100 == 0:
            print(f"已处理 {idx} 只股票/ETF...")

    if results:
        results_df = pd.DataFrame(results)
        results_df.sort_values('涨幅%', ascending=False, inplace=True)
        print("\n符合条件的股票/ETF：")
        print(results_df.to_string(index=False))
        results_df.to_csv('screened_stocks_etf.csv', index=False, encoding='utf-8-sig')
        print("\n已保存到 screened_stocks_etf.csv")
    else:
        print("今日无符合条件的股票/ETF。市场弱势？明日再试！")

if __name__ == "__main__":
    main()
