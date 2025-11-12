# stock_analysis_ak.py

import akshare as ak
import pandas as pd
from datetime import datetime
import os
import pytz
from concurrent.futures import ThreadPoolExecutor

# 设置时区为上海
shanghai_tz = pytz.timezone('Asia/Shanghai')

# --- 常量定义 ---

# 定义所有主要 A 股指数列表
INDEX_LIST = {
    # 综合指数
    '000001': '上证指数',
    '399001': '深证成指',
    '399006': '创业板指',
    '000003': 'B股指数',
    # 规模指数
    '000016': '上证50',
    '000300': '沪深300',
    '000905': '中证500',
    '000852': '中证1000',
    '399300': '沪深300(深)',
    '399005': '中小板指(已退市，部分数据可能缺失)',
    # 科创板/特定主题
    '000688': '科创50',
    '399673': '创业板50',
    '000991': '中证全指',
}

# --- 数据获取和分析核心函数 (与之前基本一致) ---

def calculate_technical_indicators(df):
    """计算每日/周/月/年的技术指标"""
    df.columns = [col.lower() for col in df.columns]

    if 'close' in df.columns:
        # 简单移动平均线 (SMA)
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()

        # 相对强弱指数 (RSI) - 14日
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        RS = gain / loss
        df['rsi14'] = 100 - (100 / (1 + RS))
    
    return df.drop(columns=['amount', 'volume', '涨跌额', '涨跌幅', '换手率'], errors='ignore')

def get_and_analyze_data_ak(symbol, start_date='20000101'):
    """使用 akshare 获取指定指数的历史数据并进行分析"""
    
    # 获取数据并清洗
    try:
        df_raw = ak.index_zh_a_hist(
            symbol=symbol, 
            period="daily", 
            start_date=start_date, 
            end_date=datetime.now().strftime('%Y%m%d')
        )
        # ... (数据处理、指标计算、周期聚合等逻辑与上个版本保持一致)
        if df_raw.empty:
            return None

        df_raw['日期'] = pd.to_datetime(df_raw['日期'])
        df_raw.set_index('日期', inplace=True)
        
        # 1. 日线分析
        df_daily = calculate_technical_indicators(df_raw.copy())
        df_daily.columns = [f'{col}_d' if col not in ['open', 'high', 'low', 'close'] else col for col in df_daily.columns]

        # 2. 聚合到周/月/年，并计算相应指标
        def aggregate_and_analyze(df, freq, prefix):
            agg_df = df.resample(freq).agg({
                '开盘': 'first',
                '最高': 'max',
                '最低': 'min',
                '收盘': 'last',
            }).dropna()
            
            if not agg_df.empty:
                 agg_df.columns = ['open', 'high', 'low', 'close']
                 agg_df = calculate_technical_indicators(agg_df)
                 cols_to_keep = [col for col in agg_df.columns if col not in ['open', 'high', 'low', 'close']]
                 agg_df = agg_df[cols_to_keep]
                 agg_df.columns = [f'{col}_{prefix}' for col in agg_df.columns]
                 
            return agg_df

        df_weekly = aggregate_and_analyze(df_raw, 'W', 'w')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'm')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'y')

        # 3. 合并所有结果
        results = df_daily.copy()
        results = results.join(df_weekly, how='left')
        results = results.join(df_monthly, how='left')
        results = results.join(df_yearly, how='left')
        results.index.name = 'date'

        return results

    except Exception as e:
        print(f"   - 错误：处理指数 {symbol} 时发生错误: {e}")
        return None

# --- 单个指数处理和保存函数 ---

def process_single_index(code, name, output_dir, timestamp):
    """处理单个指数并保存结果"""
    print(f"-> 正在处理指数: {code} ({name})")
    
    results_df = get_and_analyze_data_ak(code)
    
    if results_df is not None:
        safe_code = code.replace('.', '_')
        file_name = f"{safe_code}_{timestamp}.csv"
        output_path = os.path.join(output_dir, file_name)
        
        # 保存到 CSV
        results_df.to_csv(output_path, encoding='utf-8')
        print(f"   - ✅ {code} 成功保存到: {output_path}")
        return True
    else:
        print(f"   - ⚠️ {code} 未生成有效数据，跳过保存。")
        return False

# --- 主执行逻辑 (使用多线程) ---
def main():
    
    # 1. 设置输出路径和文件名
    now_shanghai = datetime.now(shanghai_tz)
    
    # 结果推送到仓库中的 '年/月' 目录
    output_dir = now_shanghai.strftime("%Y/%m")
    
    # 文件名公共时间戳
    timestamp = now_shanghai.strftime("%Y%m%d%H%M%S")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    print(f"-> 结果将保存到目录: {output_dir}")
    print(f"-> 准备并行处理 {len(INDEX_LIST)} 个主要指数...")
    print("---")
    
    # 2. 使用 ThreadPoolExecutor 进行并行处理
    # max_workers 可以根据需要调整，通常设置为 CPU 核心数的几倍或更高
    MAX_WORKERS = 10 
    
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for code, name in INDEX_LIST.items():
            # 提交任务到线程池
            future = executor.submit(process_single_index, code, name, output_dir, timestamp)
            futures.append(future)

    # 3. 收集结果
    successful_count = sum(f.result() for f in futures if f.result() is not None)

    print("---")
    print(f"✅ 所有指数数据处理完成。成功生成 {successful_count} 个文件。")

if __name__ == "__main__":
    main()
