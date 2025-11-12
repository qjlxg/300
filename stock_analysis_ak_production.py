# stock_analysis_pytdx_production.py - 最终 pytdx (通达信) 稳定版

import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
import time 

# --- 新增 pytdx 依赖 ---
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from pytdx.util import best_ip
from pytdx.errors import TdxConnectionError

# --- 顶部新增导入 ---
import logging
from pathlib import Path
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
OUTPUT_DIR = "index_data" 
DEFAULT_START_DATE = '2000-01-01' # pytdx 接口需要 YYYY-MM-DD 格式
INDICATOR_LOOKBACK_DAYS = 30 
LOCK_FILE = "stock_analysis.lock" 

MAX_WORKERS = 1 
MAX_RETRIES = 3 # pytdx 连接重试次数，尝试 3 次

# 通达信服务器 IP 和端口
TDX_SERVERS = [
    ('119.147.212.81', 7709), 
    ('119.147.212.81', 7721)  
]
# pytdx 周期映射: 9:日线, 5:周线, 6:月线, 8:1分钟
TDX_FREQ_MAP = {'D': 9, 'W': 5, 'M': 6}

# 定义所有主要 A 股指数列表 (注意：pytdx 需要 SH/SZ 市场代码)
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

# --- 连接客户端 (连接重试) ---

def connect_tdx_api(servers):
    """尝试连接通达信行情 API"""
    api = TdxHq_API()
    for ip, port in servers:
        try:
            logger.info(f"   - 尝试连接 pytdx 服务器: {ip}:{port}")
            if api.connect(ip, port):
                logger.info(f"   - 连接成功: {ip}:{port}")
                return api
        except TdxConnectionError:
            logger.warning(f"   - 连接失败: {ip}:{port}")
    return None

# --- 指标计算函数 (保持不变) ---

def calculate_full_technical_indicators(df):
    """计算完整的技术指标集：MA, RSI, KDJ, MACD, BBANDS, ATR, CCI, OBV"""
    if df.empty:
        return df
    
    df = df.set_index('date')
    # ... (计算逻辑与之前版本相同)
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
    """按频率聚合数据并计算指标 (pytdx 原始数据没有 turnover_rate)"""
    if df_raw_slice.empty:
        return pd.DataFrame()
        
    # pytdx 原始数据没有 turnover_rate，用 volume/amount 估算，或直接不聚合该列
    # 这里为了兼容性，简单将其设置为 NaN，后续指标计算不会依赖它
    df_raw_slice['turnover_rate'] = float('nan')
    
    # 按照 pytdx 数据的日期字段进行重采样
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'vol': 'sum', 'turnover_rate': 'mean'
    }).dropna(subset=['close'])
    
    if not agg_df.empty:
         agg_df = agg_df.reset_index().rename(columns={'index': 'date', 'vol': 'volume'})
         agg_df = calculate_full_technical_indicators(agg_df)
         
         cols_to_keep = agg_df.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
         agg_df = agg_df[['date'] + cols_to_keep.tolist()]
         agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
         agg_df.set_index('date', inplace=True)
         
    return agg_df

# --- 增量数据获取与分析核心函数 (使用 pytdx 分页) ---

def get_full_history_data(api, market, code, freq):
    """
    使用 pytdx 分页获取完整的历史 K 线数据。
    pytdx 单次最多获取 800 条数据。
    """
    all_data = []
    
    # 从最新的数据开始往前分页获取
    for start in range(0, 50000, 800): # 限制最多获取 50000 条数据 (约 200 年，安全限制)
        try:
            # pytdx.get_security_bars 每次最多获取 800 条数据
            data = api.get_security_bars(freq, market, code, start, 800)
            
            if not data:
                break
            
            df = api.to_df(data)
            
            if df.empty:
                break
            
            # pytdx 返回的数据是倒序的（最新数据在最前面），这里需要倒序排列
            all_data.append(df)
            
            # 如果获取的数据不足 800 条，说明已经获取到最旧的数据，可以停止
            if len(df) < 800:
                break
            
        except Exception as e:
            logger.error(f"   - pytdx 分页获取 {code} 失败 (Start={start})。错误: {e}")
            break # 出现错误直接退出循环
            
    if all_data:
        # 合并所有分页数据，并去除重复
        df_combined = pd.concat(all_data, ignore_index=True)
        # 去除重复行 (偶尔出现)
        df_combined.drop_duplicates(subset=['datetime'], keep='first', inplace=True)
        # 按照日期升序排列 (最旧到最新)
        df_combined.sort_values(by='datetime', inplace=True)
        
        # 格式化日期
        df_combined['date'] = pd.to_datetime(df_combined['datetime']).dt.date
        df_combined.set_index('date', inplace=True)
        
        return df_combined
    return pd.DataFrame()


def get_and_analyze_data_slice(api, market, code, start_date):
    """
    获取数据切片 (因为 pytdx 没有按日期范围查询的功能，只能全量获取后本地筛选)。
    """
    logger.info(f"   - 正在获取 {code} (pytdx 接口) 全量数据...")

    try:
        # 1. 全量获取数据
        df_full = get_full_history_data(api, market, code, TDX_FREQ_MAP['D'])

        if df_full.empty:
            logger.warning(f"   - {code} 未获取到数据。")
            return None
        
        # 2. 本地筛选（获取增量/重叠切片）
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        df_raw = df_full[df_full.index >= start_dt].copy()

        if df_raw.empty:
            logger.warning(f"   - {code} 筛选后切片为空。")
            return None
        
        # 3. pytdx 数据清洗和重命名
        df_raw.rename(columns={'vol': 'volume'}, inplace=True)
        
        # 4. 指标计算
        df_raw_processed = df_raw[['open', 'close', 'high', 'low', 'volume']].copy()
        df_raw_processed = df_raw_processed.reset_index()

        df_daily = calculate_full_technical_indicators(df_raw_processed.copy())
        
        # 5. 周/月/年指标聚合计算（需要原始 close/high/low/vol 数据，且 date 设为 index）
        df_raw.reset_index(inplace=True)
        df_raw['turnover_rate'] = float('nan') # 占位
        df_raw.set_index('date', inplace=True)
        
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        df_daily.set_index('date', inplace=True)
        
        # 聚合和合并
        df_weekly = aggregate_and_analyze(df_raw, 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw, 'M', 'M')
        df_yearly = aggregate_and_analyze(df_raw, 'Y', 'Y')

        results = df_daily.copy()
        results = results.join(df_weekly, how='left').join(df_monthly, how='left').join(df_yearly, how='left')
        results.index.name = 'date'
        
        logger.info(f"   - {code} 成功分析 {len(results)} 行数据切片。")
        return results.sort_index()

    except Exception as e:
        logger.error(f"   - 错误：处理指数 {code} 失败。最终错误: {e}")
        return None

# --- 单个指数处理和保存函数 (适配 pytdx) ---

def process_single_index(api, code_map):
    """处理单个指数，实现增量下载、计算和覆盖保存"""
    code = code_map['code']
    name = code_map['name']
    market = code_map['market']
    
    logger.info(f"-> 正在处理指数: {code} ({name})")
    
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = Path(OUTPUT_DIR) / file_name
    
    # pytdx 需要 YYYY-MM-DD 格式
    start_date_to_request = DEFAULT_START_DATE
    df_old = pd.DataFrame()
    
    # 1. 确定本次下载的起始日期 
    # (pytdx 是全量获取，这里 start_date_to_request 仅用于本地筛选重叠切片)
    if output_path.exists():
        try:
            # ... (读取旧文件逻辑不变)
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest_date_in_repo = df_old.index.max()
                
                # 往前推 INDICATOR_LOOKBACK_DAYS 天
                start_date_for_calc = latest_date_in_repo - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_request = start_date_for_calc.strftime('%Y-%m-%d')
                
                if start_date_for_calc.strftime('%Y-%m-%d') < DEFAULT_START_DATE:
                     start_date_to_request = DEFAULT_START_DATE
                
                logger.info(f"   - 检测到旧数据，最新日期为 {latest_date_in_repo.strftime('%Y-%m-%d')}。本地筛选从 {start_date_to_request} 开始的切片（含重叠）。")
            else:
                logger.warning(f"   - 旧文件 {output_path.name} 为空，将全量下载。")
        except Exception as e:
            logger.error(f"   - 警告：读取旧文件 {output_path.name} 失败 ({e})，将全量下载。")
            
    else:
        logger.info(f"   - 文件不存在，将全量下载。")


    # 2. 获取最新数据和指标 (pytdx 是全量获取后本地筛选)
    df_new_analyzed = get_and_analyze_data_slice(api, market, code, start_date_to_request)
    
    if df_new_analyzed is None:
        if not df_old.empty and df_old.index.max().date() == datetime.now(shanghai_tz).date():
             logger.info(f"   - {code} 数据已是今天最新，跳过保存。")
        else:
             logger.warning(f"   - {code} 未获取到新数据，保持原文件。")
        return False

    # 3. 整合新旧数据 (旧数据只需要筛选出比新切片更早的部分)
    if not df_old.empty:
        old_data_to_keep = df_old[df_old.index < df_new_analyzed.index.min()]
    else:
        old_data_to_keep = pd.DataFrame()


    df_combined = pd.concat([old_data_to_keep, df_new_analyzed])
    # 由于我们手动筛选了，理论上不需要去重，但为安全保留
    results_to_save = df_combined[~df_combined.index.duplicated(keep='last')]
    results_to_save = results_to_save.sort_index()

    logger.info(f"   - ✅ {code} 成功更新。总行数: {len(results_to_save)}")
    
    # 4. 保存到 CSV 
    results_to_save.to_csv(output_path, encoding='utf-8')
    return True

# --- 主执行逻辑 ---
def main():
    start_time = time.time()
    output_path = Path(OUTPUT_DIR)
    
    # 1. 检查运行锁
    lock_file_path = Path(LOCK_FILE)
    if lock_file_path.exists():
        logger.warning("检测到锁文件，脚本可能正在运行或上次异常退出。终止本次运行。")
        return
    lock_file_path.touch() 
    
    # 2. 连接 pytdx API
    tdx_api = None
    for attempt in range(MAX_RETRIES):
        tdx_api = connect_tdx_api(TDX_SERVERS)
        if tdx_api:
            break
        if attempt < MAX_RETRIES - 1:
            time.sleep(5)
    
    if not tdx_api:
        logger.error("❌ 无法连接到任何 pytdx 服务器，脚本终止。")
        lock_file_path.unlink(missing_ok=True)
        return
        
    try:
        # 3. 初始化目录和日志
        output_path.mkdir(exist_ok=True) 
        logger.info("—" * 50)
        logger.info("🚀 脚本开始运行 (使用 pytdx)")
        logger.info(f"结果将保存到专用目录: {output_path.resolve()}")
        logger.info(f"准备串行处理 {len(INDEX_LIST)} 个主要指数...")

        successful = 0
        failed = 0
        
        # 4. 转换 INDEX_LIST 格式以方便处理
        jobs = [{'code': code, **data} for code, data in INDEX_LIST.items()]
        
        # 5. 使用 ThreadPoolExecutor 进行串行处理 (MAX_WORKERS = 1)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任务，将 API 客户端作为参数传入
            futures = {
                executor.submit(process_single_index, tdx_api, job): job
                for job in jobs
            }
            
            # 使用 tqdm 包装 futures 循环
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
        
        # 6. 最终统计和输出
        logger.info("—" * 50)
        logger.info(f"✅ 所有指数数据处理完成。总耗时: {elapsed_time:.2f} 秒")
        logger.info(f"统计：成功更新 {successful} 个文件，失败/跳过 {failed} 个。")

    finally:
        # 7. 移除锁文件并断开连接
        tdx_api.close()
        lock_file_path.unlink(missing_ok=True)
        logger.info("pytdx 连接已关闭，锁文件已清除。")

if __name__ == "__main__":
    main()
