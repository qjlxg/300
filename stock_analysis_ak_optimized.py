# stock_analysis_ak_optimized.py

import akshare as ak
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import os
import pytz
from concurrent.futures import ThreadPoolExecutor

# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
OUTPUT_DIR = "index_data" 
DEFAULT_START_DATE = '20000101'
# 必须保证下载的数据块足够长，以计算所有指标（如MACD和MA20需要26天）。
# 我们向前推30个日历日作为安全裕度。
INDICATOR_LOOKBACK_DAYS = 30 

# 定义所有主要 A 股指数列表
INDEX_LIST = {
    '000001': '上证指数', '399001': '深证成指', '399006': '创业板指',
    '000016': '上证50', '000300': '沪深300', '000905': '中证500',
    '000852': '中证1000', '000688': '科创50', '399300': '沪深300(深)',
    '000991': '中证全指',
}

# --- 指标计算函数 ---

def calculate_full_technical_indicators(df):
    """计算完整的技术指标集：MA, RSI, KDJ, MACD"""
    if df.empty:
        return df
    
    # 确保 pandas-ta 的输入是日期索引的 DataFrame
    df = df.set_index('date')

    # 1. 移动平均线 (MA)
    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))

    # 2. 相对强弱指数 (RSI)
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))

    # 3. 随机指标 (KDJ) - 使用默认参数 (9, 3, 3)
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True) 
    df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D', 'STOCHj_9_3_3': 'J'})

    # 4. 指数平滑移动平均线 (MACD) - 使用默认参数 (12, 26, 9)
    df.ta.macd(append=True)
    df = df.rename(columns={'MACD_12_26_9': 'MACD', 'MACDh_12_26_9': 'MACDh', 'MACDs_12_26_9': 'MACDs'})

    return df.reset_index()

def aggregate_and_analyze(df_raw_slice, freq, prefix):
    """按频率聚合数据并计算指标"""
    if df_raw_slice.empty:
        return pd.DataFrame()
        
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'turnover_rate': 'mean'
    }).dropna()
    
    if not agg_df.empty:
         # 将索引转为列，以便计算指标
         agg_df = agg_df.reset_index().rename(columns={'index': 'date'})
         agg_df = calculate_full_technical_indicators(agg_df)
         
         # 仅保留指标列
         cols_to_keep = agg_df.columns.drop(['date', 'open', 'high', 'low', 'close', 'volume', 'turnover_rate'])
         agg_df = agg_df[['date'] + cols_to_keep.tolist()]
         agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
         agg_df.set_index('date', inplace=True)
         
    return agg_df

# --- 增量数据获取与分析核心函数 ---

def get_and_analyze_data_slice(symbol, start_date):
    """
    使用 akshare 获取指定指数从 start_date 开始到最新的数据，并进行完整分析。
    """
    print(f"   - 正在获取 {symbol} 从 {start_date} 开始的数据...")
    
    try:
        end_date_str = datetime.now(shanghai_tz).strftime('%Y%m%d')
        
        # 1. 获取日线数据切片
        df_raw = ak.index_zh_a_hist(
            symbol=symbol, 
            period="daily", 
            start_date=start_date, 
            end_date=end_date_str
        )
            
        if df_raw.empty:
            print(f"   - {symbol} 未获取到数据。")
            return None 

        # 2. 数据清洗和标准化列名
        df_raw.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'change_abs', 'change_pct', 'turnover_rate']
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        
        # 提取关键原始数据
        df_raw_processed = df_raw[['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate']].copy()
        
        # 3. 计算日线指标
        df_daily = calculate_full_technical_indicators(df_raw_processed.copy())
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        
        # 4. 聚合和计算周/月/年指标
        df_raw.set_index('date', inplace=True)
        df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')

        # 5. 合并所有结果
        df_daily.set_index('date', inplace=True)
        results = df_daily.copy()
        
        results = results.join(df_weekly, how='left')
        results = results.join(df_monthly, how='left')
        results = results.join(df_yearly, how='left')
        
        results.index.name = 'date'
        
        print(f"   - {symbol} 成功分析 {len(results)} 行数据切片。")
        return results.sort_index()

    except Exception as e:
        print(f"   - 错误：处理指数 {symbol} 时发生错误: {e}")
        return None

# --- 单个指数处理和保存函数 (核心逻辑) ---

def process_single_index(code, name):
    """处理单个指数，实现增量下载、计算和覆盖保存"""
    print(f"-> 正在处理指数: {code} ({name})")
    
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = os.path.join(OUTPUT_DIR, file_name)
    
    start_date_to_request = DEFAULT_START_DATE
    df_old = pd.DataFrame()
    
    # 1. 确定本次下载的起始日期 (考虑指标计算所需的历史数据)
    if os.path.exists(output_path):
        try:
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest_date_in_repo = df_old.index.max()
                
                # 关键：往前推 INDICATOR_LOOKBACK_DAYS 天作为下载的起始日期
                # 这样即使遇到长假，也能保证下载到足够的历史数据来计算最新的指标。
                start_date_for_calc = latest_date_in_repo - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_request = start_date_for_calc.strftime('%Y%m%d')
                
                # 确保起始日期不早于DEFAULT_START_DATE
                if start_date_for_calc.strftime('%Y%m%d') < DEFAULT_START_DATE:
                     start_date_to_request = DEFAULT_START_DATE
                
                print(f"   - 检测到旧数据，最新日期为 {latest_date_in_repo.strftime('%Y-%m-%d')}。从 {start_date_to_request} 开始下载增量数据块（含重叠）。")
            else:
                print(f"   - 旧文件为空，从 {DEFAULT_START_DATE} 开始下载所有历史数据。")
        except Exception as e:
            print(f"   - 警告：读取旧文件 {output_path} 失败 ({e})，将从 {DEFAULT_START_DATE} 重新下载。")
            
    else:
        print(f"   - 文件不存在，从 {DEFAULT_START_DATE} 开始下载所有历史数据。")


    # 2. 获取最新数据和指标 (只获取增量数据块)
    df_new_analyzed = get_and_analyze_data_slice(code, start_date_to_request)
    
    if df_new_analyzed is None:
        # 如果获取失败，检查旧数据是否是今天的。
        if not df_old.empty and df_old.index.max().date() == datetime.now(shanghai_tz).date():
             print(f"   - {code} 数据已是今天最新，跳过保存。")
        else:
             print(f"   - {code} 未获取到新数据，保持原文件。")
        return False

    # 3. 整合新旧数据 (使用 deduplication logic)
    
    # 合并旧数据和新数据块 (新数据块包含了计算所需的历史重叠)
    df_combined = pd.concat([df_old, df_new_analyzed])
    
    # 移除索引重复的行，保留最新的分析结果 (keep='last')
    # 这样新数据块中重新计算的指标会覆盖旧数据中对应日期的指标。
    results_to_save = df_combined[~df_combined.index.duplicated(keep='last')]
    
    # 确保最终数据按日期升序
    results_to_save = results_to_save.sort_index()

    print(f"   - ✅ {code} 成功更新。总行数: {len(results_to_save)}")
    
    # 4. 保存到 CSV (覆盖旧文件)
    results_to_save.to_csv(output_path, encoding='utf-8')
    return True

# --- 主执行逻辑 (使用多线程) ---
def main():
    # 1. 设置输出路径
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"-> 结果将保存到专用目录: {OUTPUT_DIR}")
    print(f"-> 准备并行处理 {len(INDEX_LIST)} 个主要指数...")
    print("---")
    
    # 2. 使用 ThreadPoolExecutor 进行并行处理
    MAX_WORKERS = 10 
    
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for code, name in INDEX_LIST.items():
            future = executor.submit(process_single_index, code, name)
            futures.append(future)

    # 3. 收集结果
    successful_count = sum(f.result() for f in futures if f.result() is not None)

    print("---")
    print(f"✅ 所有指数数据处理完成。成功更新 {successful_count} 个文件。")

if __name__ == "__main__":
    main()
