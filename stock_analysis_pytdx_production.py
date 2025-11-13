# stock_analysis_akshare_production.py - V9 终极稳定版 (强化超时与延迟)

import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
import time 

# --- AkShare 依赖 ---
import akshare as ak 

# --- 顶部新增导入 ---
import logging
from pathlib import Path
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)


# --- AkShare 全局配置 (V9 核心改进 1: 设置全局超时) ---
# 强制设置全局请求超时时间为 30 秒
try:
    ak.set_time_out(30)
except Exception as e:
    # 兼容性处理
    print(f"警告：设置 AkShare 全局超时失败：{e}")


# --- 常量和配置 (V9 核心改进 2: 提高基础延迟) ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
OUTPUT_DIR = "index_data" 
DEFAULT_START_DATE = '2000-01-01' 
INDICATOR_LOOKBACK_DAYS = 30 
LOCK_FILE = "stock_analysis.lock" 

MAX_WORKERS = 1 # 保持为1，确保串行
MAX_RETRIES = 5 # 最大的重试次数
BASE_DELAY = 10  # 基础延迟提高到 10 秒

# --- 指数列表及代码结构 (保持不变) ---

INDEX_LIST = {
    '000001': {'name': '上证指数', 'market': 1}, 
    '399001': {'name': '深证成指', 'market': 0}, 
    '399006': {'name': '创业板指', 'market': 0},
    '000016': {'name': '上证50', 'market': 1}, 
    '000300': {'name': '沪深300', 'market': 1}, 
    '000905': {'name': '中证500', 'market': 1},
    '000852': {'name': '中证1000', 'market': 1}, 
    '000688': {'name': '科创50', 'market': 1}, 
    '399300': {'name': '沪深300(深)', 'market': 0},
    '000991': {'name': '中证全指', 'market': 1},
    '000906': {'name': '中证800', 'market': 1}, 
    '399005': {'name': '中小板指', 'market': 0}, 
    '399330': {'name': '深证100', 'market': 0},
    '000010': {'name': '上证180', 'market': 1}, 
    '000015': {'name': '红利指数', 'market': 1},
    '000011': {'name': '上证基金指数', 'market': 1}, 
    '399305': {'name': '深证基金指数', 'market': 0}, 
    '399306': {'name': '深证ETF指数', 'market': 0},
}

# 申万行业指数 (中信/万得暂时禁用，AkShare申万接口相对稳定，但仍需谨慎)
SW_INDUSTRY_DICT = {'801010':'农林牧渔','801020':'采掘','801030':'化工','801040':'钢铁','801050':'有色金属','801080':'电子','801110':'家用电器','801120':'食品饮料','801130':'纺织服装','801140':'轻工制造','801150':'医药生物','801160':'公用事业','801170':'交通运输','801180':'房地产','801200':'商业贸易','801210':'休闲服务','801230':'综合','801710':'建筑材料','801720':'建筑装饰','801730':'电气设备','801740':'国防军工','801750':'计算机','801760':'传媒','801770':'通信','801780':'银行','801790':'非银金融','801880':'汽车','801890':'机械设备','801060':'建筑建材','801070':'机械设备','801090':'交运设备','801190':'金融服务','801100':'信息设备','801220':'信息服务'}
CS_INDUSTRY_DICT = {} 
WIND_INDUSTRY_DICT = {} 

def get_pytdx_market(code): 
    code = str(code)
    if code.startswith('00') or code.startswith('88') or code.startswith('801') or code.startswith('CI005'):
        return 1  
    elif code.startswith('399'):
        return 0 
    return 1 

def merge_industry_indexes(index_list, industry_dict, prefix=""):
    for code, name in industry_dict.items():
        pytdx_code = code.split('.')[0] 
        if pytdx_code not in index_list:
            index_list[pytdx_code] = {
                'name': f'{prefix}{name}',
                'market': get_pytdx_market(pytdx_code)
            }
    return index_list

INDEX_LIST = merge_industry_indexes(INDEX_LIST, SW_INDUSTRY_DICT, prefix="申万一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, CS_INDUSTRY_DICT, prefix="中信一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, WIND_INDUSTRY_DICT, prefix="万得一级_")

# --- 配置日志系统 (保持不变) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("stock_analysis.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- 指标计算函数 (保持不变) ---

def calculate_full_technical_indicators(df):
    """计算完整的技术指标集：MA, RSI, KDJ, MACD, BBANDS, ATR, CCI, OBV"""
    if df.empty:
        return df
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    price_cols = ['open', 'close', 'high', 'low', 'volume']
    for col in price_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True) 
    df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D', 'STOCHj_9_3_3': 'J'})
    df.ta.macd(append=True)
    df = df.rename(columns={'MACD_12_26_9': 'MACD', 'MACDh_12_26_9': 'MACDh', 'MACDs_12_26_9': 'MACDs'})
    df.ta.bbands(length=20, std=2, append=True)
    df = df.rename(columns={'BBL_20_2.0': 'BB_lower', 'BBM_20_2.0': 'BB_middle', 'BBU_20_2.0': 'BB_upper', 'BBB_20_2.0': 'BB_bandwidth', 'BBP_20_2.0': 'BB_percent'})
    df.ta.atr(length=14, append=True)
    df = df.rename(columns={'ATRr_14': 'ATR14'})
    df.ta.cci(length=20, append=True)
    df = df.rename(columns={'CCI_20_0.015': 'CCI20'})
    df.ta.obv(append=True)
    
    return df.reset_index()


def aggregate_and_analyze(df_raw_slice, freq, prefix):
    """按频率聚合数据并计算指标"""
    if df_raw_slice.empty:
        return pd.DataFrame()
        
    df_raw_slice['turnover_rate'] = float('nan') 
    
    df_raw_slice.index = pd.to_datetime(df_raw_slice.index)
    
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'turnover_rate': 'mean'
    }).dropna(subset=['close'])
    
    if not agg_df.empty:
        agg_df = agg_df.reset_index().rename(columns={'index': 'date'})
        
        agg_df['date'] = agg_df['date'].dt.date 
        agg_df = calculate_full_technical_indicators(agg_df)
        
        cols_to_keep = agg_df.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
        agg_df.set_index('date', inplace=True)
        
    return agg_df

# --- 增量数据获取与分析核心函数 (使用 AkShare V9 - 强化重试/延迟) ---

def get_full_history_data(code, start_date_str):
    """
    使用 AkShare 获取完整的历史 K 线数据，并加入重试和延迟机制。
    """
    logger.info(f"    - 正在通过 AkShare 获取 {code} (从 {start_date_str} 开始)...")
    
    for attempt in range(MAX_RETRIES):
        try:
            if code.startswith('801'):
                logger.warning(f"    - 警告：AkShare 行业指数 {code} 接口复杂或不稳定，跳过。")
                return pd.DataFrame()
            else:
                # AkShare A股指数接口
                df = ak.index_zh_a_hist(
                    symbol=code, 
                    period="daily", 
                    start_date=start_date_str.replace('-', ''), # AkShare要求无连字符的日期
                    end_date=datetime.now().strftime('%Y%m%d')
                )

            if df.empty:
                raise ValueError("AkShare returned an empty DataFrame.")
            
            # 字段清洗与重命名
            df.rename(columns={
                '日期': 'date', 
                '开盘': 'open', 
                '收盘': 'close', 
                '最高': 'high', 
                '最低': 'low', 
                '成交量': 'volume'
            }, inplace=True)
            
            df = df[['date', 'open', 'close', 'high', 'low', 'volume']].copy()

            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            for col in ['open', 'close', 'high', 'low', 'volume']:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

            df.dropna(subset=['close'], inplace=True)
            
            df.index = df.index.date
            df.sort_index(inplace=True)

            logger.info(f"    - ✅ {code} 数据获取成功 (尝试次数: {attempt + 1})。")
            return df
        
        except Exception as e:
            # 捕获连接中断、超时、空数据等所有错误
            logger.warning(f"    - AkShare 获取 {code} 失败 (尝试 {attempt + 1}/{MAX_RETRIES})。错误: {e}")
            if attempt < MAX_RETRIES - 1:
                # 增加等待时间，并乘以尝试次数，实现指数退避
                # V9 等待时间：10s, 12s, 14s, 16s, 18s
                wait_time = BASE_DELAY + attempt * 2 
                logger.info(f"    - 正在等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error(f"    - AkShare 获取 {code} 最终失败，放弃。")
                return pd.DataFrame()


def get_and_analyze_data_slice(code, start_date):
    """获取数据切片，包括全量获取、本地筛选和指标计算。"""
    
    try:
        df_full = get_full_history_data(code, start_date)

        if df_full.empty:
            logger.warning(f"    - {code} 未获取到有效数据。")
            return None
            
        df_raw = df_full.copy()
        
        df_raw_processed = df_raw.reset_index().rename(columns={'index': 'date'})
        df_raw_processed['date'] = pd.to_datetime(df_raw_processed['date']) 
        
        df_daily = calculate_full_technical_indicators(df_raw_processed.copy())
        
        df_raw.index = pd.to_datetime(df_raw.index)
        
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        df_daily.set_index('date', inplace=True)
        
        df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')

        results = df_daily.copy()
        results = results.join(df_weekly, how='left').join(df_monthly, how='left').join(df_yearly, how='left')
        results.index.name = 'date'
        
        logger.info(f"    - {code} 成功分析 {len(results)} 行数据切片。")
        return results.sort_index()

    except Exception as e:
        logger.error(f"    - 错误：处理指数 {code} 失败。最终错误: {e}")
        return None

# --- 单个指数处理和保存函数 ---

def process_single_index(code_map):
    """处理单个指数，实现增量下载、计算和覆盖保存"""
    code = code_map['code']
    name = code_map['name']
    
    logger.info(f"-> 正在处理指数: {code} ({name})")
    
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = Path(OUTPUT_DIR) / file_name
    
    start_date_to_request = DEFAULT_START_DATE
    df_old = pd.DataFrame()
    
    # 1. 确定本次下载的起始日期 
    if output_path.exists():
        try:
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest_date_in_repo = df_old.index.max()
                
                start_date_for_calc = latest_date_in_repo - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_request = start_date_for_calc.strftime('%Y-%m-%d')
                
                if start_date_for_calc.strftime('%Y-%m-%d') < DEFAULT_START_DATE:
                    start_date_to_request = DEFAULT_START_DATE
                
                logger.info(f"    - 检测到旧数据，最新日期为 {latest_date_in_repo.strftime('%Y-%m-%d')}。API 请求从 {start_date_to_request} 开始的切片（含重叠）。")
            else:
                logger.warning(f"    - 旧文件 {output_path.name} 为空，将全量下载。")
        except Exception as e:
            logger.error(f"    - 警告：读取旧文件 {output_path.name} 失败 ({e})，将全量下载。")
            
    else:
        logger.info(f"    - 文件不存在，将全量下载。")


    # 2. 获取最新数据和指标 
    df_new_analyzed = get_and_analyze_data_slice(code, start_date_to_request)
    
    if df_new_analyzed is None:
        is_today_updated = False
        if not df_old.empty and pd.api.types.is_datetime64_any_dtype(df_old.index):
             today = datetime.now(shanghai_tz).date()
             is_today_updated = df_old.index.max().date() == today
        
        if is_today_updated:
            logger.info(f"    - {code} 数据已是今天最新，跳过保存。")
        else:
            logger.warning(f"    - {code} 未获取到新数据，保持原文件。")
        return False

    # 3. 整合新旧数据 
    if not df_old.empty:
        df_old.index = df_old.index.date
        old_data_to_keep = df_old[df_old.index < df_new_analyzed.index.min()]
    else:
        old_data_to_keep = pd.DataFrame()
        
    df_new_analyzed.index = pd.to_datetime(df_new_analyzed.index)
    old_data_to_keep.index = pd.to_datetime(old_data_to_keep.index)


    df_combined = pd.concat([old_data_to_keep, df_new_analyzed])
    results_to_save = df_combined[~df_combined.index.duplicated(keep='last')]
    results_to_save = results_to_save.sort_index()

    logger.info(f"    - ✅ {code} 成功更新。总行数: {len(results_to_save)}")
    
    # 4. 保存到 CSV 
    results_to_save.to_csv(output_path, encoding='utf-8')
    return True

# --- 主执行逻辑 ---
def main():
    start_time = time.time()
    output_path = Path(OUTPUT_DIR)
    
    lock_file_path = Path(LOCK_FILE)
    if lock_file_path.exists():
        logger.warning("检测到锁文件，脚本可能正在运行或上次异常退出。终止本次运行。")
        return
    lock_file_path.touch() 
    
    logger.info("—" * 50)
    logger.info("🚀 脚本开始运行 (使用 AkShare V9 - 终极稳定版)")
    
    try:
        output_path.mkdir(exist_ok=True) 
        logger.info(f"结果将保存到专用目录: {output_path.resolve()}")
        logger.info(f"准备串行处理 {len(INDEX_LIST)} 个指数...")

        successful = 0
        failed = 0
        
        jobs = [{'code': code, **data} for code, data in INDEX_LIST.items()]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            
            futures = {
                executor.submit(process_single_index, job): job
                for job in jobs
            }
            
            for future in tqdm(futures, desc="处理指数", unit="个", ncols=100, leave=True):
                job = futures[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"处理 {job['code']} ({job['name']}) 时发生未捕获异常: {e}")
                    failed += 1
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("—" * 50)
        logger.info(f"✅ 所有指数数据处理完成。总耗时: {elapsed_time:.2f} 秒")
        logger.info(f"统计：成功更新 {successful} 个文件，失败/跳过 {failed} 个。")

    finally:
        lock_file_path.unlink(missing_ok=True)
        logger.info("锁文件已清除。")

if __name__ == "__main__":
    main()
