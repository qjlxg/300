# stock_analysis_ak_v2.py

import akshare as ak
import pandas as pd
import pandas_ta as ta  # 用于计算KDJ, MACD等指标
from datetime import datetime
import os
import pytz
from concurrent.futures import ThreadPoolExecutor

# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')

# 定义所有主要 A 股指数列表 (保持不变)
INDEX_LIST = {
    '000001': '上证指数', '399001': '深证成指', '399006': '创业板指',
    '000016': '上证50', '000300': '沪深300', '000905': '中证500',
    '000852': '中证1000', '000688': '科创50', '399673': '创业板50',
    '000003': 'B股指数', '399300': '沪深300(深)', '000991': '中证全指',
    # 请注意: '399005' 中小板指已退市，若数据获取报错，建议移除
}

# --- 指标计算函数 ---

def calculate_full_technical_indicators(df):
    """
    计算完整的技术指标集：MA, RSI, KDJ, MACD。
    假设 df 包含 'open', 'high', 'low', 'close'
    """
    
    # 1. 移动平均线 (MA)
    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))

    # 2. 相对强弱指数 (RSI)
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))

    # 3. 随机指标 (KDJ) - 使用默认参数 (9, 3, 3)
    # KDJ 返回三列：K, D, J
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True) 
    df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D', 'STOCHj_9_3_3': 'J'})

    # 4. 指数平滑移动平均线 (MACD) - 使用默认参数 (12, 26, 9)
    # MACD 返回三列：MACD, MACDh (Histogram), MACDs (Signal)
    df.ta.macd(append=True)
    df = df.rename(columns={
        'MACD_12_26_9': 'MACD', 
        'MACDh_12_26_9': 'MACDh', 
        'MACDs_12_26_9': 'MACDs'
    })

    return df.dropna(subset=['close', 'MA20']).reset_index()


def get_and_analyze_data_ak(symbol, start_date='20000101'):
    """使用 akshare 获取数据并进行分析"""
    
    try:
        # 使用 ak.index_zh_a_hist 通用接口获取数据
        df_raw = ak.index_zh_a_hist(
            symbol=symbol, 
            period="daily", 
            start_date=start_date, 
            end_date=datetime.now().strftime('%Y%m%d')
        )
            
        if df_raw.empty:
            return None

        # 数据清洗和标准化列名
        df_raw.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'change_abs', 'change_pct', 'turnover_rate']
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        df_raw.set_index('date', inplace=True)
        
        # 提取关键原始数据
        df = df_raw[['open', 'close', 'high', 'low', 'volume', 'turnover_rate']].copy()
        
        # 1. 计算日线指标
        df_daily = calculate_full_technical_indicators(df.copy())
        
        # 重命名日线指标列，添加 _D 后缀
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        
        # 2. 聚合到周/月/年，并计算相应指标
        def aggregate_and_analyze(df, freq, prefix):
            """按频率聚合数据并计算指标"""
            agg_df = df.resample(freq).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'turnover_rate': 'mean'
            }).dropna()
            
            # 计算指标
            if not agg_df.empty:
                 agg_df = calculate_full_technical_indicators(agg_df)
                 
                 # 删除原始 OCHL 列
                 cols_to_keep = agg_df.columns.drop(['date', 'open', 'high', 'low', 'close', 'volume', 'turnover_rate'])
                 agg_df = agg_df[['date'] + cols_to_keep.tolist()]
                 
                 # 重命名指标列，加上周期前缀
                 agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
                 
            return agg_df

        df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')

        # 3. 合并所有结果
        # 将 date 设为索引以便合并
        df_daily.set_index('date', inplace=True)
        df_weekly.set_index('date', inplace=True)
        df_monthly.set_index('date', inplace=True)
        df_yearly.set_index('date', inplace=True)

        results = df_daily.copy()
        
        # 合并周/月/年指标到日线数据中
        results = results.join(df_weekly, how='left')
        results = results.join(df_monthly, how='left')
        results = results.join(df_yearly, how='left')
        
        results.index.name = 'date'
        
        # 确保数据按日期升序
        return results.sort_index()

    except Exception as e:
        print(f"   - 错误：处理指数 {symbol} 时发生错误: {e}")
        return None

# --- 单个指数处理和保存函数 (覆盖式更新) ---

def process_single_index(code, name, output_dir):
    """处理单个指数，并以覆盖/追加方式保存到非时间戳文件名"""
    print(f"-> 正在处理指数: {code} ({name})")
    
    # 1. 获取最新数据和指标
    results_df = get_and_analyze_data_ak(code)
    
    if results_df is None:
        print(f"   - ⚠️ {code} 未生成有效数据，跳过保存。")
        return False

    # 2. 定义最终文件路径
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = os.path.join(output_dir, file_name)
    
    # 3. 加载旧数据并进行追加/覆盖
    if os.path.exists(output_path):
        try:
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            
            # 将新数据与旧数据合并。如果索引（日期）相同，新数据覆盖旧数据。
            # pandas.DataFrame.combine_first() 用于合并，缺失值用另一个DataFrame填充。
            # 但这里我们希望新数据完全覆盖旧数据中重复的日期，所以使用 append/concat 后 drop_duplicates 更加直观。
            
            df_combined = pd.concat([df_old, results_df])
            # 保持最新的数据 (keep='last')
            df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
            
            # 最终按日期排序
            results_to_save = df_combined.sort_index()
            print(f"   - ✅ {code} 成功追加 {len(results_df)} 行新数据。总行数: {len(results_to_save)}")
            
        except Exception as e:
            print(f"   - 警告：读取旧文件 {output_path} 失败 ({e})，将使用新数据覆盖。")
            results_to_save = results_df
    else:
        results_to_save = results_df
        print(f"   - ✅ {code} 文件不存在，创建新文件。")

    # 4. 保存到 CSV (覆盖旧文件)
    results_to_save.to_csv(output_path, encoding='utf-8')
    return True

# --- 主执行逻辑 (使用多线程) ---
def main():
    
    # 1. 设置输出路径 (目标目录是 'data/年/月'，方便 Git 追踪和分类)
    now_shanghai = datetime.now(shanghai_tz)
    
    # 为了实现覆盖式更新，我们将所有文件统一放在一个固定的目录下
    output_dir = "index_data" 
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    print(f"-> 结果将保存到目录: {output_dir}")
    print(f"-> 准备并行处理 {len(INDEX_LIST)} 个主要指数...")
    print("---")
    
    # 2. 使用 ThreadPoolExecutor 进行并行处理
    MAX_WORKERS = 10 
    
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for code, name in INDEX_LIST.items():
            # 提交任务到线程池
            future = executor.submit(process_single_index, code, name, output_dir)
            futures.append(future)

    # 3. 收集结果
    successful_count = sum(f.result() for f in futures if f.result() is not None)

    print("---")
    print(f"✅ 所有指数数据处理完成。成功更新 {successful_count} 个文件。")

if __name__ == "__main__":
    main()
