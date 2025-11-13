# stock_analysis_pytdx_production.py - 最终 pytdx 稳定版 (动态 IP & 行业指数)
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
import time
import logging
from pathlib import Path
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- pytdx 依赖 ---
from pytdx.hq import TdxHq_API
from pytdx.util import best_ip
from pytdx.errors import TdxConnectionError
from pytdx.exhq import TdxExHq_API  # 保留完整性

# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
OUTPUT_DIR = "index_data"
DEFAULT_START_DATE = '2000-01-01'
INDICATOR_LOOKBACK_DAYS = 30
LOCK_FILE = "stock_analysis.lock"
MAX_WORKERS = 1
MAX_RETRIES = 3
TDX_FREQ_MAP = {'D': 9, 'W': 5, 'M': 6}

# --- 动态 IP 获取 ---
def get_best_servers(num_servers=5):
    """同步获取最佳服务器列表（兼容 pytdx 1.72+）"""
    fallback_servers = [
        ('114.80.149.19', 7709), ('114.80.149.22', 7709), ('114.80.149.84', 7709),
        ('114.80.80.222', 7709), ('115.238.56.198', 7709), ('119.147.164.60', 7709),
        ('123.125.108.23', 7709), ('180.153.18.17', 7709), ('121.36.81.195', 7709),
        ('124.71.187.122', 7709),
    ]

    try:
        best = best_ip.select_best_ip()  # 同步返回 dict
        if best and best.get('ip'):
            logger.info(f" - 自动挑选最佳 IP: {best['ip']}:{best['port']}")
            servers = [(best['ip'], best['port'])]
            count = 1
            for ip, port in fallback_servers:
                if ip != best['ip'] and count < num_servers:
                    servers.append((ip, port))
                    count += 1
            return servers
        else:
            logger.warning(" - best_ip 返回为空，使用备用列表")
            return fallback_servers[:num_servers]
    except Exception as e:
        logger.error(f" - 获取最佳 IP 失败: {e}，使用内置备用")
        return fallback_servers[:num_servers]

# --- 指数列表 ---
INDEX_LIST = {
    '000001': {'name': '上证指数', 'market': 1}, '399001': {'name': '深证成指', 'market': 0},
    '399006': {'name': '创业板指', 'market': 0}, '000016': {'name': '上证50', 'market': 1},
    '000300': {'name': '沪深300', 'market': 1}, '000905': {'name': '中证500', 'market': 1},
    '000852': {'name': '中证1000', 'market': 1}, '000688': {'name': '科创50', 'market': 1},
    '399300': {'name': '沪深300(深)', 'market': 0}, '000991': {'name': '中证全指', 'market': 1},
    '000906': {'name': '中证800', 'market': 1}, '399005': {'name': '中小板指', 'market': 0},
    '399330': {'name': '深证100', 'market': 0}, '000010': {'name': '上证180', 'market': 1},
    '000015': {'name': '红利指数', 'market': 1}, '000011': {'name': '上证基金指数', 'market': 1},
    '399305': {'name': '深证基金指数', 'market': 0}, '399306': {'name': '深证ETF指数', 'market': 0},
}

# --- 行业指数 ---
SW_INDUSTRY_DICT = {
    '801010': '农林牧渔', '801020': '采掘', '801030': '化工', '801040': '钢铁', '801050': '有色金属',
    '801080': '电子', '801110': '家用电器', '801120': '食品饮料', '801130': '纺织服装', '801140': '轻工制造',
    '801150': '医药生物', '801160': '公用事业', '801170': '交通运输', '801180': '房地产', '801200': '商业贸易',
    '801210': '休闲服务', '801230': '综合', '801710': '建筑材料', '801720': '建筑装饰', '801730': '电气设备',
    '801740': '国防军工', '801750': '计算机', '801760': '传媒', '801770': '通信', '801780': '银行',
    '801790': '非银金融', '801880': '汽车', '801890': '机械设备', '801060': '建筑建材', '801070': '机械设备',
    '801090': '交运设备', '801190': '金融服务', '801100': '信息设备', '801220': '信息服务'
}

CS_INDUSTRY_DICT = {
    'CI005001': '石油石化', 'CI005002': '煤炭', 'CI005003': '有色金属', 'CI005004': '电力及公用事业',
    'CI005005': '钢铁', 'CI005006': '基础化工', 'CI005007': '建筑', 'CI005008': '建材', 'CI005009': '轻工制造',
    'CI005010': '机械', 'CI005011': '电力设备', 'CI005012': '国防军工', 'CI005013': '汽车', 'CI005014': '商贸零售',
    'CI005015': '餐饮旅游', 'CI005016': '家电', 'CI005017': '纺织服装', 'CI005018': '医药', 'CI005019': '食品饮料',
    'CI005020': '农林牧渔', 'CI005021': '银行', 'CI005022': '非银行金融', 'CI005023': '房地产',
    'CI005024': '交通运输', 'CI005025': '电子元器件', 'CI005026': '通信', 'CI005027': '计算机',
    'CI005028': '传媒', 'CI005029': '综合'
}

WIND_INDUSTRY_DICT = {
    '882002': '材料', '882001': '能源', '882003': '工业', '882004': '可选消费', '882005': '日常消费',
    '882006': '医疗保健', '882007': '金融', '882008': '信息技术', '882009': '电信服务',
    '882010': '公用事业', '882011': '房地产'
}

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

INDEX_LIST = merge_industry_indexes(INDEX_LIST, SW_INDUSTRY_DICT, "申万一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, CS_INDUSTRY_DICT, "中信一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, WIND_INDUSTRY_DICT, "万得一级_")

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("stock_analysis.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 连接客户端 ---
def connect_tdx_api(servers):
    api = TdxHq_API()
    for ip, port in servers:
        try:
            logger.info(f" - 尝试连接 pytdx 服务器: {ip}:{port}")
            if api.connect(ip, port):
                logger.info(f" - 连接成功: {ip}:{port}")
                return api
        except TdxConnectionError:
            logger.warning(f" - 连接失败: {ip}:{port}")
    return None

# --- 指标计算 ---
def calculate_full_technical_indicators(df):
    if df.empty:
        return df
    df = df.set_index('date')
    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True)
    df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D', 'STOCHj_9_3_3': 'J'})
    df.ta.macd(append=True)
    df = df.rename(columns={'MACD_12_26_9': 'MACD', 'MACDh_12_26_9': 'MACDh', 'MACDs_12_26_9': 'MACDs'})
    df.ta.bbands(length=20, std=2, append=True)
    df = df.rename(columns={
        'BBL_20_2.0': 'BB_lower', 'BBM_20_2.0': 'BB_middle', 'BBU_20_2.0': 'BB_upper',
        'BBB_20_2.0': 'BB_bandwidth', 'BBP_20_2.0': 'BB_percent'
    })
    df.ta.atr(length=14, append=True)
    df = df.rename(columns={'ATRr_14': 'ATR14'})
    df.ta.cci(length=20, append=True)
    df = df.rename(columns={'CCI_20_0.015': 'CCI20'})
    df.ta.obv(append=True)
    return df.reset_index()

def aggregate_and_analyze(df_raw_slice, freq, prefix):
    if df_raw_slice.empty:
        return pd.DataFrame()
    df_raw_slice['turnover_rate'] = float('nan')
    df_raw_slice.index = pd.to_datetime(df_raw_slice.index)
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'vol': 'sum', 'turnover_rate': 'mean'
    }).dropna(subset=['close'])
    if not agg_df.empty:
        agg_df = agg_df.reset_index().rename(columns={'index': 'date', 'vol': 'volume'})
        agg_df['date'] = agg_df['date'].dt.date
        agg_df = calculate_full_technical_indicators(agg_df)
        cols_to_keep = agg_df.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        agg_df = agg_df[['date'] + cols_to_keep.tolist()]
        agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
        agg_df.set_index('date', inplace=True)
    return agg_df

# --- 数据获取 ---
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
            logger.error(f" - pytdx 分页获取 {code} 失败 (Start={start})。错误: {e}")
            break
    if all_data:
        df_combined = pd.concat(all_data, ignore_index=True)
        df_combined.drop_duplicates(subset=['datetime'], keep='first', inplace=True)
        df_combined.sort_values(by='datetime', inplace=True)
        df_combined['date'] = pd.to_datetime(df_combined['datetime']).dt.date
        df_combined.set_index('date', inplace=True)
        return df_combined
    return pd.DataFrame()

def get_and_analyze_data_slice(api, market, code, start_date):
    logger.info(f" - 正在获取 {code} 全量数据...")
    try:
        df_full = get_full_history_data(api, market, code, TDX_FREQ_MAP['D'])
        if df_full.empty:
            logger.warning(f" - {code} 未获取到数据。")
            return None
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        df_raw = df_full[df_full.index >= start_dt].copy()
        if df_raw.empty:
            logger.warning(f" - {code} 筛选后切片为空。")
            return None
        df_raw.rename(columns={'vol': 'volume'}, inplace=True)
        df_raw_processed = df_raw[['open', 'close', 'high', 'low', 'volume']].copy()
        df_raw_processed = df_raw_processed.reset_index()
        df_daily = calculate_full_technical_indicators(df_raw_processed.copy())
        df_raw.reset_index(inplace=True)
        df_raw['turnover_rate'] = float('nan')
        df_raw.set_index('date', inplace=True)
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        df_daily.set_index('date', inplace=True)
        df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')
        results = df_daily.copy()
        results = results.join(df_weekly, how='left').join(df_monthly, how='left').join(df_yearly, how='left')
        results.index.name = 'date'
        logger.info(f" - {code} 成功分析 {len(results)} 行数据切片。")
        return results.sort_index()
    except Exception as e:
        logger.error(f" - 处理指数 {code} 失败: {e}")
        return None

# --- 单指数处理 ---
def process_single_index(api, code_map):
    code = code_map['code']
    name = code_map['name']
    market = code_map['market']
    logger.info(f"-> 正在处理指数: {code} ({name})")
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = Path(OUTPUT_DIR) / file_name
    start_date_to_request = DEFAULT_START_DATE
    df_old = pd.DataFrame()

    if output_path.exists():
        try:
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest_date_in_repo = df_old.index.max()
                start_date_for_calc = latest_date_in_repo - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_request = max(start_date_for_calc, datetime.strptime(DEFAULT_START_DATE, '%Y-%m-%d')).strftime('%Y-%m-%d')
                logger.info(f" - 旧数据最新: {latest_date_in_repo.strftime('%Y-%m-%d')}，从 {start_date_to_request} 获取重叠切片")
        except Exception as e:
            logger.error(f" - 读取旧文件失败: {e}")

    with api:  # 使用上下文管理器
        df_new_analyzed = get_and_analyze_data_slice(api, market, code, start_date_to_request)

    if df_new_analyzed is None:
        today = datetime.now(shanghai_tz).date()
        if not df_old.empty and df_old.index.max().date() == today:
            logger.info(f" - {code} 数据已是今日最新，跳过。")
        else:
            logger.warning(f" - {code} 未获取新数据，保留原文件。")
        return False

    old_data_to_keep = df_old[df_old.index < df_new_analyzed.index.min()] if not df_old.empty else pd.DataFrame()
    df_combined = pd.concat([old_data_to_keep, df_new_analyzed])
    results_to_save = df_combined[~df_combined.index.duplicated(keep='last')].sort_index()
    results_to_save.to_csv(output_path, encoding='utf-8')
    logger.info(f" - {code} 更新成功，共 {len(results_to_save)} 行")
    return True

# --- 主函数 ---
def main():
    start_time = time.time()
    output_path = Path(OUTPUT_DIR)
    lock_file_path = Path(LOCK_FILE)

    if lock_file_path.exists():
        logger.warning("检测到锁文件，脚本可能正在运行，终止。")
        return
    lock_file_path.touch()

    servers = get_best_servers(5)
    logger.info(f" - 使用服务器: {servers}")

    tdx_api = None
    for attempt in range(MAX_RETRIES):
        tdx_api = connect_tdx_api(servers)
        if tdx_api:
            break
        time.sleep(5)

    if not tdx_api:
        logger.error("无法连接 pytdx 服务器，终止。")
        lock_file_path.unlink(missing_ok=True)
        return

    try:
        output_path.mkdir(exist_ok=True)
        logger.info("—" * 50)
        logger.info("脚本开始运行 (pytdx 稳定版)")
        logger.info(f"共 {len(INDEX_LIST)} 个指数")

        jobs = [{'code': code, **data} for code, data in INDEX_LIST.items()]
        successful = failed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_index, tdx_api, job): job for job in jobs}
            for future in tqdm(futures, desc="处理指数", unit="个", ncols=100):
                job = futures[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"处理 {job['code']} 失败: {e}")
                    failed += 1

        elapsed = time.time() - start_time
        logger.info("—" * 50)
        logger.info(f"完成！耗时: {elapsed:.2f}s，成功: {successful}，失败: {failed}")

    finally:
        if tdx_api:
            try:
                tdx_api.close()
            except:
                pass
        lock_file_path.unlink(missing_ok=True)
        logger.info("连接关闭，锁文件清除。")

if __name__ == "__ Rb":
    main()
