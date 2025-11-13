# stock_analysis_pytdx_production.py - 最终修复版（防炸 + ExHq 自动 + IP 缓存）
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
import logging
from pathlib import Path
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- pytdx 依赖 ---
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from pytdx.errors import TdxConnectionError
from pandas.api.types import is_datetime64_any_dtype

# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
OUTPUT_DIR = Path("index_data")
DEFAULT_START_DATE = '2000-01-01'
INDICATOR_LOOKBACK_DAYS = 30
LOCK_FILE = "stock_analysis.lock"
MAX_WORKERS = 3
MAX_RETRIES = 3
IP_CACHE = Path("best_ip.json")

# pytdx 周期映射
TDX_FREQ_MAP = {'D': 9, 'W': 5, 'M': 6}

# --- 指数列表 ---
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

# 行业指数合并（保持不变）
SW_INDUSTRY_DICT = { ... }  # 保持原样
CS_INDUSTRY_DICT = { ... }
WIND_INDUSTRY_DICT = { ... }

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

# --- 日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("stock_analysis.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- IP 缓存 + 动态服务器 ---
def get_best_servers(num_servers=5):
    primary = ('115.238.56.198', 7709)
    if IP_CACHE.exists():
        try:
            cached = json.load(IP_CACHE.open())
            if isinstance(cached, list) and len(cached) == 2:
                primary = tuple(cached)
                logger.info(f" - 从缓存加载上次成功 IP: {primary}")
        except Exception as e:
            logger.warning(f" - IP 缓存读取失败: {e}")

    backup_servers = [
        ('114.80.149.19', 7709), ('114.80.149.22', 7709), ('114.80.149.84', 7709),
        ('114.80.80.222', 7709), ('119.147.164.60', 7709), ('123.125.108.23', 7709),
        ('180.153.18.17', 7709), ('121.36.81.195', 7709), ('124.71.187.122', 7709),
        ('119.147.212.81', 7721), ('119.147.212.81', 7709),
    ]
    backup_servers = [s for s in backup_servers if s != primary]
    servers = [primary] + backup_servers
    logger.info(f" - 最终服务器列表: {servers[:num_servers]}")
    return servers[:num_servers]

# --- 连接 API（支持 Hq 和 ExHq）---
def connect_tdx_api(code, servers):
    need_exhq = code in ['000688', '000852', '000348', '000928', '399006', '399300']  # 科创/创业板/新指数
    api_class = TdxExHq_API if need_exhq else TdxHq_API
    api = api_class()

    for ip, port in servers:
        try:
            logger.info(f" - [{'ExHq' if need_exhq else 'Hq'}] 尝试连接 {ip}:{port}")
            if api.connect(ip, port, timeout=10):
                # 成功后写缓存
                json.dump([ip, port], IP_CACHE.open('w'))
                logger.info(f" - 连接成功: {ip}:{port}")
                return api
        except Exception as e:
            logger.warning(f" - 连接失败 {ip}:{port}: {e}")
    return None

# --- 数据获取（防炸核心）---
def get_full_history_data(api, market, code, freq):
    all_data = []
    for start in range(0, 50000, 800):
        try:
            data = api.get_security_bars(freq, market, code, start, 800)
            if not data:
                break
            df = api.to_df(data)
            if df.empty:
                break
            all_data.append(df)
            if len(df) < 800:
                break
        except Exception as e:
            logger.error(f" - 分页失败 {code} (start={start}): {e}")
            break

    if not all_data:
        return pd.DataFrame()

    df_combined = pd.concat(all_data, ignore_index=True)
    df_combined.drop_duplicates(subset=['datetime'], keep='first', inplace=True)
    df_combined.sort_values(by='datetime', inplace=True)

    # --- 关键修复：强制格式 + 双重检查 ---
    df_combined['valid_datetime'] = pd.to_datetime(
        df_combined['datetime'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )

    if not is_datetime64_any_dtype(df_combined['valid_datetime']):
        logger.warning(f" - {code} datetime 转换失败，dtype={df_combined['valid_datetime'].dtype}，样本={df_combined['datetime'].head(3).tolist()}")
        return pd.DataFrame()

    df_combined.dropna(subset=['valid_datetime'], inplace=True)
    if df_combined.empty:
        logger.warning(f" - {code} 无效日期全被移除")
        return pd.DataFrame()

    df_combined['date'] = df_combined['valid_datetime'].dt.date
    df_combined.set_index('date', inplace=True)
    return df_combined

# --- 指标计算 ---
def calculate_full_technical_indicators(df):
    if df.empty:
        return df
    df = df.set_index('date') if not df.index.name == 'date' else df
    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True)
    df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D'})
    df.ta.macd(append=True)
    df = df.rename(columns={'MACD_12_26_9': 'MACD', 'MACDh_12_26_9': 'MACDh', 'MACDs_12_26_9': 'MACDs'})
    df.ta.bbands(length=20, std=2, append=True)
    df = df.rename(columns={'BBL_20_2.0': 'BB_lower', 'BBM_20_2.0': 'BB_middle', 'BBU_20_2.0': 'BB_upper'})
    df.ta.atr(length=14, append=True)
    df = df.rename(columns={'ATRr_14': 'ATR14'})
    df.ta.cci(length=20, append=True)
    df = df.rename(columns={'CCI_20_0.015': 'CCI20'})
    df.ta.obv(append=True)
    return df.reset_index()

# --- 聚合 ---
def aggregate_and_analyze(df_raw_slice, freq, prefix):
    if df_raw_slice.empty:
        return pd.DataFrame()
    df_raw_slice.index = pd.to_datetime(df_raw_slice.index, errors='coerce')
    if not is_datetime64_any_dtype(df_raw_slice.index):
        return pd.DataFrame()
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'vol': 'sum'
    }).dropna(subset=['close'])
    if agg_df.empty:
        return pd.DataFrame()
    agg_df = agg_df.reset_index().rename(columns={'index': 'date', 'vol': 'volume'})
    agg_df['date'] = agg_df['date'].dt.date
    agg_df = calculate_full_technical_indicators(agg_df)
    cols = agg_df.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume'])
    agg_df = agg_df.rename(columns={c: f'{c}_{prefix}' for c in cols})
    return agg_df.set_index('date')

# --- 核心处理 ---
def get_and_analyze_data_slice(api, market, code, start_date):
    logger.info(f" - 获取 {code} 全量数据...")
    df_full = get_full_history_data(api, market, code, TDX_FREQ_MAP['D'])
    if df_full.empty:
        return None

    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    df_raw = df_full[df_full.index >= start_dt].copy()
    if df_raw.empty:
        return None

    df_raw.rename(columns={'vol': 'volume'}, inplace=True)
    df_raw_processed = df_raw[['open', 'close', 'high', 'low', 'volume']].copy()
    df_raw_processed = df_raw_processed.reset_index()
    df_daily = calculate_full_technical_indicators(df_raw_processed.copy())

    df_raw.reset_index(inplace=True)
    df_raw.set_index('date', inplace=True)

    daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume'])
    df_daily = df_daily.rename(columns={c: f'{c}_D' for c in daily_cols}).set_index('date')

    df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
    df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
    df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')

    results = df_daily.join(df_weekly, how='left').join(df_monthly, how='left').join(df_yearly, how='left')
    return results.sort_index()

# --- 单指数处理 ---
def process_single_index(code_map):
    code = code_map['code']
    name = code_map['name']
    market = code_map['market']
    logger.info(f"-> 处理: {code} ({name})")

    file_name = f"{code}.csv"
    output_path = OUTPUT_DIR / file_name

    start_date_to_request = DEFAULT_START_DATE
    df_old = pd.DataFrame()

    if output_path.exists():
        try:
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest = df_old.index.max().date()
                start_calc = latest - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_request = max(start_calc, datetime.strptime(DEFAULT_START_DATE, '%Y-%m-%d').date()).strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f" - 读取旧文件失败 {e}")

    # 每个指数独立连接
    servers = get_best_servers(3)
    api = connect_tdx_api(code, servers)
    if not api:
        logger.error(f" - {code} 连接失败")
        return False

    try:
        df_new = get_and_analyze_data_slice(api, market, code, start_date_to_request)
        if df_new is None:
            logger.warning(f" - {code} 无新数据")
            return False

        df_combined = pd.concat([df_old, df_new])
        results = df_combined[~df_combined.index.duplicated(keep='last')].sort_index()
        results.to_csv(output_path, encoding='utf-8')
        logger.info(f" - {code} 更新成功，共 {len(results)} 行")
        return True
    except Exception as e:
        logger.error(f" - {code} 处理失败: {e}")
        return False
    finally:
        try:
            api.close()
        except:
            pass

# --- 主函数 ---
def main():
    start_time = time.time()
    lock_file = Path(LOCK_FILE)
    if lock_file.exists():
        logger.warning("检测到锁文件，终止运行")
        return
    lock_file.touch()

    OUTPUT_DIR.mkdir(exist_ok=True)
    logger.info("—" * 50)
    logger.info("脚本开始运行")
    logger.info(f"处理 {len(INDEX_LIST)} 个指数")

    jobs = [{'code': c, **d} for c, d in INDEX_LIST.items()]
    successful = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_index, job) for job in jobs]
        for future in tqdm(as_completed(futures), total=len(futures), desc="处理指数", unit="个"):
            if future.result():
                successful += 1
            else:
                failed += 1
            time.sleep(0.2)  # 防封

    elapsed = time.time() - start_time
    logger.info("—" * 50)
    logger.info(f"完成！耗时: {elapsed:.2f}s，成功: {successful}，失败: {failed}")
    lock_file.unlink(missing_ok=True)

if __name__ == "__main__":
    main()
