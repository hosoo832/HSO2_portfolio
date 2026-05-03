# performance.py (v125 기반 - 2020년 히스토리 추가 버전)

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import gspread
import warnings
import json
import re

import config
import google_api

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# [내부 헬퍼] - 코드 중복 제거 + 비연속 행 안전 처리
# ---------------------------------------------------------
def _collect_inception_dates(*date_dicts):
    """주어진 inception dict들에서 유효한 날짜를 모두 수집해 set으로 반환.
    GROUP_INCEPTION_DATES 와 ACCOUNT_INCEPTION_DATES 양쪽을 한 번에 모을 때 사용.
    """
    out = set()
    for d in date_dicts:
        for v in d.values():
            if v: out.add(v)
    return out

def _iter_months(start_year, start_month, end_year, end_month):
    """(start_year, start_month) ~ (end_year, end_month) 범위의 (year, month) 튜플 yield. 시간 오름차순."""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            y += 1; m = 1

def _iter_quarters(start_year, start_q, end_year, end_q):
    """(start_year, start_q) ~ (end_year, end_q) 범위의 (year, quarter) 튜플 yield. 시간 오름차순."""
    y, q = start_year, start_q
    while (y, q) <= (end_year, end_q):
        yield y, q
        q += 1
        if q > 4:
            y += 1; q = 1

def _month_bounds(year, month):
    """해당 월의 (start, next_start). end_date 는 다음달 1일을 사용해서 기존 연도 로직(s_y, e_y) 과 일관성 유지."""
    s = pd.Timestamp(year=year, month=month, day=1)
    if month == 12: e = pd.Timestamp(year=year+1, month=1, day=1)
    else: e = pd.Timestamp(year=year, month=month+1, day=1)
    return s, e

def _quarter_bounds(year, q):
    """해당 분기의 (start, next_start)."""
    s_month = (q - 1) * 3 + 1
    s = pd.Timestamp(year=year, month=s_month, day=1)
    if q == 4: e = pd.Timestamp(year=year+1, month=1, day=1)
    else: e = pd.Timestamp(year=year, month=s_month + 3, day=1)
    return s, e

def _month_label(year, month):
    return f"{year:04d}-{month:02d}"

def _quarter_label(year, q):
    return f"{year:04d}-Q{q}"

def _runs_of_consecutive(sorted_ints):
    """정렬된 정수 리스트를 [(start, end), ...] 연속 구간 튜플 리스트로 묶음.
    예: [2, 3, 4, 7, 8] -> [(2, 4), (7, 8)]
    같은 '구분'의 행들이 비연속으로 흩어져도 정확히 range 변환되도록 보장.
    """
    if not sorted_ints: return []
    runs = []
    a = b = sorted_ints[0]
    for r in sorted_ints[1:]:
        if r == b + 1:
            b = r
        else:
            runs.append((a, b))
            a = b = r
    runs.append((a, b))
    return runs

# ---------------------------------------------------------
# [사용자 설정]
# ---------------------------------------------------------
ACCOUNT_GROUPS = {
    '53648897': '멘토 포트폴리오',
    '60271589': '멘토 포트폴리오',
    '53649012': 'HS 포트폴리오',
    '856045053982': 'HS 포트폴리오',
    '220914426167': 'HS 포트폴리오',
    '717190227129': 'HS 포트폴리오',
    '1234': '아들 계좌', 
}

CUSTOM_START_DATE = '2025-05-14'
CUSTOM_END_DATE   = '2026-02-13'

# ---------------------------------------------------------
# [월별/분기별 수익률 시작 시점]
# - 2025-05 부터 월별 컬럼 자동 생성 (운용 본격 시작점)
# - 2025-Q3 부터 분기별 컬럼 자동 생성
# - 미래에 더 거슬러 올라가고 싶으면 여기 숫자만 바꾸면 됨
# ---------------------------------------------------------
MONTHLY_START_YEAR  = 2025
MONTHLY_START_MONTH = 5
QUARTERLY_START_YEAR = 2025
QUARTERLY_START_Q    = 3

# ---------------------------------------------------------
# [그룹별/계좌별 분석 시작일 설정]
# - 누적수익률(TWR/MWR) 계산 시작일을 그룹/계좌별로 다르게 지정
# - None 또는 키 미존재 시: 데이터의 자연 시작일 사용
# - "전체(Total)"는 항상 자연 시작일 사용 (이 설정의 영향 받지 않음)
# - "국가별/테마별"도 자연 시작일 사용
# ---------------------------------------------------------
# 그룹별 시작일 (각 그룹의 의미 있는 운용 시작 시점)
# 멘토: 5/14 = CUSTOM_START_DATE (자금 본격 투입 시작일)
# HS:   7/21 = 53649012 본격 투입일 (분리 운용 시작)
GROUP_INCEPTION_DATES = {
    '멘토 포트폴리오': '2025-05-14',
    'HS 포트폴리오':   '2025-07-21',
    # '아들 계좌': None,  # 미지정 시 자연 시작일 사용
}

# 계좌별 시작일 (특정 계좌만 별도로 자르고 싶을 때)
# 비워두면 → 해당 계좌가 속한 그룹의 GROUP_INCEPTION_DATES 자동 상속
# 그룹에도 없으면 → 자연 시작일 사용
ACCOUNT_INCEPTION_DATES = {
    # 예시: '53649012': '2025-08-04',  # 특정 계좌만 다른 시작일 적용 시
}

# 마일스톤 시작일 — 그룹/계좌와 별개로 "이 날짜부터 누적" 관점 추적
# 예: 멘토 강의 기수 시작일 (1기/2기/3기 ...) 같은 외부 이벤트 기준
# 추가하면 자동으로 '지정(YY-MM-DD~)' 컬럼이 모든 행에 생성됨
MILESTONE_DATES = {
    '3기 시작': '2025-10-29',
    # '4기 시작': '2026-XX-XX',  # 시작일 확정되면 추가
}

MARKET_COL_MAP = {
    'KOSPI_price': 'KOSPI',
    'KOSDAQ_price': 'KOSDAQ',
    'SHANGHAI_price': 'Shanghai',
    'SP500_price': 'S&P 500',
    'NASDAQ_price': 'NASDAQ'
}
MAIN_BM_WEIGHTS = {'KOSPI_price':0.375, 'SP500_price':0.375, 'SHANGHAI_price':0.25}

def clean_account_key(val):
    try:
        s_val = str(val).strip()
        s_val = re.sub(r'[^0-9]', '', s_val)
        return s_val
    except: return str(val).strip()

def safe_float(val):
    try:
        if pd.isna(val) or str(val).strip() == "": return 0.0
        clean_str = re.sub(r'[^\d.-]', '', str(val))
        if not clean_str: return 0.0
        return float(clean_str)
    except: return 0.0

def parse_korean_date(date_str):
    try:
        date_str = str(date_str)
        date_str = re.sub(r'\s*\(.\)', '', date_str) 
        date_str = date_str.replace('.', '-')
        return pd.to_datetime(date_str)
    except: return pd.NaT

def get_last_friday(target_date):
    this_monday = target_date - timedelta(days=target_date.weekday())
    return this_monday - timedelta(days=3)

def get_last_day_of_prev_month(target_date):
    return target_date.replace(day=1) - timedelta(days=1)

def get_prev_bday(target_date):
    if target_date.weekday() == 0: return target_date - timedelta(days=3) 
    return target_date - timedelta(days=1)

def run_performance_analysis():
    print("--- [Performance] v125 뼈대 유지 + 2020년 성적표 추가 ---")

    # 1. 데이터 로딩
    df_log, sheet_instance = google_api.get_all_records_as_text("portfolio_log")
    df_master, _ = google_api.get_all_records_as_text(config.SHEET_MASTER_DATA)
    df_market, _ = google_api.get_all_records_as_text(config.SHEET_MARKET_DATA)
    has_market_data = False
    
    if not df_market.empty:
        if 'date' not in df_market.columns:
            df_market.rename(columns={df_market.columns[0]: 'date'}, inplace=True)
        df_market = df_market[df_market['date'].astype(str).str.match(r'^\d')]
        df_market['date'] = df_market['date'].apply(parse_korean_date)
        df_market = df_market.dropna(subset=['date'])
        
        if not df_market.empty:
            df_market = df_market.sort_values('date').set_index('date')
            # 중복 date 인덱스 제거 (cron 과 backfill 이 같은 날짜에 행 추가하면 dup 발생)
            dup_count = df_market.index.duplicated().sum()
            if dup_count > 0:
                print(f"  [Warn] market_data 에 중복 date {dup_count}건 발견 → 마지막 행만 유지")
                df_market = df_market[~df_market.index.duplicated(keep='last')]
            for col in df_market.columns: df_market[col] = df_market[col].apply(safe_float)
            has_market_data = True

    if df_log.empty: return

    # 2. 전처리
    if not df_master.empty:
        df_master['ticker'] = df_master['ticker'].astype(str).str.strip()
        c_dict = df_master.set_index('ticker')['country'].to_dict()
        t_dict = df_master.set_index('ticker')['theme'].to_dict()
        
        def fill_missing(row, col_name, mapping_dict, default_val):
            val = row.get(col_name, '')
            if pd.isna(val) or str(val).strip() == '':
                t = str(row.get('ticker', '')).strip()
                if t == 'MMF_INT': return '한국' if col_name == 'country' else '안전 자산'
                if t.startswith('CASH'): return '통합' if col_name == 'country' else '안전 자산'
                return mapping_dict.get(t, default_val)
            return val

        df_log['country'] = df_log.apply(lambda x: fill_missing(x, 'country', c_dict, '기타'), axis=1)
        df_log['theme'] = df_log.apply(lambda x: fill_missing(x, 'theme', t_dict, '기타'), axis=1)

    df_log['date'] = pd.to_datetime(df_log['date']).dt.floor('D')
    df_log['clean_account'] = df_log['account'].apply(clean_account_key)
    df_log['group_name'] = df_log['clean_account'].map(ACCOUNT_GROUPS).fillna('기타')
    for col in ['market_value_krw', 'net_invested_capital', 'total_cost_krw', 'unrealized_pl_krw']:
        if col in df_log.columns: df_log[col] = df_log[col].apply(safe_float)

    raw_latest_date = df_log['date'].max()
    if raw_latest_date.weekday() >= 5: 
        ideal_friday = raw_latest_date - timedelta(days=raw_latest_date.weekday() - 4)
        available_dates = df_log[df_log['date'] <= ideal_friday]['date']
        latest_date = available_dates.max() if not available_dates.empty else raw_latest_date
    else:
        latest_date = raw_latest_date

    print(f"  > 분석 기준일: {latest_date.strftime('%Y-%m-%d')}")

    final_results = []

    # [#2 개선] 침묵 클램프 진단 로그
    # calculate_return / calculate_mwr_and_cap 에서 요청된 시작일이
    # 데이터의 첫 유효일보다 이르면 자동으로 잘려서 계산되는데, 이 사건들을
    # 여기에 누적해서 분석 끝에 한 번에 요약 출력.
    #
    # 주의: 여기서 "데이터의 첫 유효일"은 portfolio_log 의 (필터링된 부분집합) ×
    # (market_value_krw > 0) 조건을 만족하는 가장 이른 날짜를 뜻함. 따라서
    # 클램프가 떴다 = 실제 거래가 없었다 가 아니라, portfolio_log 에 그 날짜의
    # 해당 부분집합 엔트리가 없거나 market_value 가 0 이라는 뜻.
    clamp_log = []
    # 현재 처리 중인 (dim, category) 컨텍스트 - 클램프 발생 시 출처 추적용
    _current_ctx = ['']

    # =================================================================
    # [Core] TWR 계산기
    # =================================================================
    def calculate_return(daily_df, start_date, end_date, method='twr'):
        df = daily_df.copy()
        if 'date' in df.columns: df = df.set_index('date')
        df = df.sort_index()
        
        valid_dates = df[df['market_value_krw'] > 0].index
        if valid_dates.empty: return 0.0
        
        s_dt = pd.to_datetime(start_date)
        e_dt = pd.to_datetime(end_date)

        first_valid = valid_dates.min()
        if s_dt < first_valid:
            # [#2 개선] 클램프 사건 기록 + 컨텍스트(어느 dim/category에서 발생했는지)
            clamp_log.append({
                'requested': str(start_date)[:10],
                'actual': first_valid.strftime('%Y-%m-%d'),
                'method': 'TWR',
                'ctx': _current_ctx[0]
            })
            s_dt = first_valid

        idx_s = df.index.asof(s_dt)
        idx_e = df.index.asof(e_dt)

        if pd.isna(idx_s) or pd.isna(idx_e): return 0.0
        if idx_s == idx_e: return 0.0

        if method == 'simple':
            val_s = df.loc[idx_s]['market_value_krw']
            val_e = df.loc[idx_e]['market_value_krw']
            if val_s <= 0: return 0.0
            return (val_e - val_s) / val_s

        if method == 'twr' or method == 'dietz': 
            df['prev_nic'] = df['net_invested_capital'].shift(1)
            df['flow'] = (df['net_invested_capital'] - df['prev_nic']).fillna(0.0)
            df['prev_mv'] = df['market_value_krw'].shift(1)
            
            def get_daily_ret(row):
                denom = row['prev_mv'] + row['flow']
                if pd.isna(denom) or denom <= 0: return 0.0
                return (row['market_value_krw'] - denom) / denom

            df['daily_ret'] = df.apply(get_daily_ret, axis=1).fillna(0.0)
            mask = (df.index > s_dt) & (df.index <= e_dt)
            target_df = df.loc[mask]
            if target_df.empty: return 0.0
            return (1 + target_df['daily_ret']).prod() - 1
            
        return 0.0

    # =================================================================
    # [Core] MWR & 평균투자원금 계산기
    # =================================================================
    def calculate_mwr_and_cap(daily_df, start_date, end_date):
        df = daily_df.copy()
        if 'date' in df.columns: df = df.set_index('date')
        df = df.sort_index()
        
        s_dt = pd.to_datetime(start_date)
        e_dt = pd.to_datetime(end_date)
        
        valid_dates = df[df['market_value_krw'] > 0].index
        if valid_dates.empty: return 0.0, 0.0

        first_valid = valid_dates.min()
        if s_dt < first_valid:
            # [#2 개선] 클램프 사건 기록 + 컨텍스트
            clamp_log.append({
                'requested': str(start_date)[:10],
                'actual': first_valid.strftime('%Y-%m-%d'),
                'method': 'MWR',
                'ctx': _current_ctx[0]
            })
            s_dt = first_valid

        idx_s = df.index.asof(s_dt)
        idx_e = df.index.asof(e_dt)

        if pd.isna(idx_s) or pd.isna(idx_e): return 0.0, 0.0

        V0 = df.loc[idx_s]['market_value_krw']
        V1 = df.loc[idx_e]['market_value_krw']
        
        df['prev_nic'] = df['net_invested_capital'].shift(1)
        df['flow'] = (df['net_invested_capital'] - df['prev_nic']).fillna(0.0)
        
        mask = (df.index > s_dt) & (df.index <= e_dt)
        target_df = df.loc[mask].copy()
        
        total_days = (e_dt - s_dt).days
        if total_days <= 0: return 0.0, V0
        
        F = target_df['flow'].sum()
        target_df['days_remaining'] = (e_dt - target_df.index).days
        target_df['weight'] = target_df['days_remaining'] / total_days
        WCF = (target_df['flow'] * target_df['weight']).sum()
        
        avg_capital = V0 + WCF
        mwr = (V1 - V0 - F) / avg_capital if avg_capital > 0 else 0.0
        return mwr, avg_capital

    def calculate_stats(subset_df, dimension_name, category_name, type_mode='account'):
        # [#2 개선] 컨텍스트 세팅 — 이 calculate_stats 호출 동안 발생하는 모든
        # calculate_return / calculate_mwr_and_cap 의 클램프 로그가 이 출처로 묶임.
        _current_ctx[0] = f"{dimension_name}|{category_name}"
        if subset_df.empty: return None

        if type_mode == 'account':
            daily = subset_df.groupby(['date', 'clean_account']).agg({
                'market_value_krw': 'sum', 'net_invested_capital': 'max'
            }).reset_index().groupby('date').sum()
            daily.rename(columns={'net_invested_capital': 'cost_basis'}, inplace=True)
            daily['net_invested_capital'] = daily['cost_basis'] 
            calc_method = 'twr'
        else:
            daily = subset_df.groupby('date').agg({'market_value_krw': 'sum', 'total_cost_krw': 'sum'})
            daily.rename(columns={'total_cost_krw': 'cost_basis'}, inplace=True)
            daily['net_invested_capital'] = daily['cost_basis']
            calc_method = 'simple'  

        daily['profit'] = daily['market_value_krw'] - daily['cost_basis']
        if latest_date not in daily.index: return None
        today_data = daily.loc[latest_date]
        
        target_start_date = daily.index.min()

        # ---------------------------------------------------------
        # [그룹별 시작일 적용] - 누적수익률(%) 의 시작일을 inception 으로 자름
        # 계좌별/국가별/테마별/전체는 자연 시작일 사용 (진짜 "누적")
        # ---------------------------------------------------------
        inception_str = None
        if dimension_name == '그룹별':
            inception_str = GROUP_INCEPTION_DATES.get(category_name)
            if inception_str:
                target_start_date = max(daily.index.min(), pd.to_datetime(inception_str))

        # 기본 정보
        row = {
            '구분': dimension_name, '상세': category_name,
            '총자산': int(today_data['market_value_krw']),
            '투자원금': int(today_data['cost_basis']),
            '평가손익': int(today_data['profit'])
        }

        # 단기 지표 (TWR)
        prev_bday = get_prev_bday(latest_date)
        last_fri = get_last_friday(latest_date)
        last_m_end = get_last_day_of_prev_month(latest_date)
        m1_start = get_last_day_of_prev_month(last_m_end)
        m2_start = get_last_day_of_prev_month(m1_start)

        row['1일'] = calculate_return(daily, prev_bday, latest_date, calc_method)
        row['WTD(이번주)'] = calculate_return(daily, last_fri, latest_date, calc_method)
        row['W-1(저번주)'] = calculate_return(daily, last_fri - timedelta(days=7), last_fri, calc_method)
        row['W-2(2주전)'] = calculate_return(daily, last_fri - timedelta(days=14), last_fri - timedelta(days=7), calc_method)
        row['W-3(3주전)'] = calculate_return(daily, last_fri - timedelta(days=21), last_fri - timedelta(days=14), calc_method)
        row['MTD(이번달)'] = calculate_return(daily, last_m_end, latest_date, calc_method)
        row['M-1(지난달)'] = calculate_return(daily, m1_start, last_m_end, calc_method)
        row['M-2(2달전)'] = calculate_return(daily, m2_start, m1_start, calc_method)

        # 연도별 지표 (2020년까지 자동 추가)
        curr_year = latest_date.year
        row['YTD'] = calculate_return(daily, f"{curr_year}-01-01", latest_date, calc_method)
        row['MWR_YTD'], _ = calculate_mwr_and_cap(daily, f"{curr_year}-01-01", latest_date)
        
        for year in range(curr_year - 1, 2019, -1):
            s_y = f"{year}-01-01"
            e_y = f"{year+1}-01-01"
            row[f'{year}년'] = calculate_return(daily, s_y, e_y, calc_method)
            row[f'MWR_{year}년'], _ = calculate_mwr_and_cap(daily, s_y, e_y)
        
        row['누적수익률(%)'] = calculate_return(daily, target_start_date, latest_date, calc_method)
        row['MWR_누적'], _ = calculate_mwr_and_cap(daily, target_start_date, latest_date)

        # 지정기간 지표 (TWR, MWR, 손익, 평균투자원금)
        col_name_twr = f"지정({CUSTOM_START_DATE[2:]}~)"
        col_name_mwr = f"MWR_지정({CUSTOM_START_DATE[2:]}~)"
        col_name_pl  = f"지정_손익({CUSTOM_START_DATE[2:]}~)"
        
        row[col_name_twr] = calculate_return(daily, CUSTOM_START_DATE, latest_date, calc_method)
        mwr_custom, avg_cap_custom = calculate_mwr_and_cap(daily, CUSTOM_START_DATE, latest_date)
        row[col_name_mwr] = mwr_custom
        row['지정기간 평균투자원금'] = int(avg_cap_custom)

        try:
            c_s = pd.to_datetime(CUSTOM_START_DATE)
            idx_s = daily.index.asof(c_s)
            p_s = daily.loc[idx_s]['profit'] if pd.notna(idx_s) else 0
            p_e = daily.loc[latest_date]['profit']
            row[col_name_pl] = int(p_e - p_s)
        except: row[col_name_pl] = 0

        # ---------------------------------------------------------
        # [모든 행에 적용] 지정기간 inception 컬럼 계산
        # GROUP_INCEPTION_DATES + ACCOUNT_INCEPTION_DATES 의 모든 inception 날짜
        # 에 대해 컬럼 생성 → 전체/계좌별/국가별/테마별 모두 7/21~ 같은 시점
        # 기준 수익률을 같이 보기 위함
        # [#5 개선] 헬퍼로 단일화 (이전에 4곳에 분산되어 있던 set 수집 로직)
        # ---------------------------------------------------------
        for inc_date in sorted(_collect_inception_dates(GROUP_INCEPTION_DATES, ACCOUNT_INCEPTION_DATES, MILESTONE_DATES)):
            inc_twr_col = f"지정({inc_date[2:]}~)"
            inc_mwr_col = f"MWR_지정({inc_date[2:]}~)"
            inc_pl_col = f"지정_손익({inc_date[2:]}~)"
            inc_avg_cap_col = f"지정기간 평균투자원금({inc_date[2:]}~)"

            row[inc_twr_col] = calculate_return(daily, inc_date, latest_date, calc_method)
            inc_mwr_val, inc_avg_cap_val = calculate_mwr_and_cap(daily, inc_date, latest_date)
            row[inc_mwr_col] = inc_mwr_val
            row[inc_avg_cap_col] = int(inc_avg_cap_val)

            # 손익 계산 (해당 inception 시점부터 현재까지 이익 차이)
            try:
                inc_dt = pd.to_datetime(inc_date)
                idx_s = daily.index.asof(inc_dt)
                p_s = daily.loc[idx_s]['profit'] if pd.notna(idx_s) else 0
                p_e = daily.loc[latest_date]['profit']
                row[inc_pl_col] = int(p_e - p_s)
            except:
                row[inc_pl_col] = 0

        # ---------------------------------------------------------
        # [월별 / 분기별 지표 계산]
        # MONTHLY_START_(YEAR/MONTH), QUARTERLY_START_(YEAR/Q) 로부터 latest_date 까지
        # 각 기간마다 TWR / MWR / 손익(profit_end - profit_start) 3종 컬럼 자동 생성
        # ---------------------------------------------------------
        latest_year = latest_date.year
        latest_month = latest_date.month
        latest_q = (latest_month - 1) // 3 + 1

        # --- 월별 ---
        for y, m in _iter_months(MONTHLY_START_YEAR, MONTHLY_START_MONTH, latest_year, latest_month):
            label = _month_label(y, m)
            s_dt, e_dt = _month_bounds(y, m)

            row[label] = calculate_return(daily, s_dt, e_dt, calc_method)
            row[f"MWR_{label}"], _ = calculate_mwr_and_cap(daily, s_dt, e_dt)

            # 손익(profit_end - profit_start) — 기존 CUSTOM/inception 손익 계산과 동일 패턴
            try:
                idx_s = daily.index.asof(s_dt)
                idx_e = daily.index.asof(e_dt)
                p_s = daily.loc[idx_s]['profit'] if pd.notna(idx_s) else 0
                p_e = daily.loc[idx_e]['profit'] if pd.notna(idx_e) else 0
                row[f"손익_{label}"] = int(p_e - p_s)
            except:
                row[f"손익_{label}"] = 0

        # --- 분기별 ---
        for y, q in _iter_quarters(QUARTERLY_START_YEAR, QUARTERLY_START_Q, latest_year, latest_q):
            label = _quarter_label(y, q)
            s_dt, e_dt = _quarter_bounds(y, q)

            row[label] = calculate_return(daily, s_dt, e_dt, calc_method)
            row[f"MWR_{label}"], _ = calculate_mwr_and_cap(daily, s_dt, e_dt)

            try:
                idx_s = daily.index.asof(s_dt)
                idx_e = daily.index.asof(e_dt)
                p_s = daily.loc[idx_s]['profit'] if pd.notna(idx_s) else 0
                p_e = daily.loc[idx_e]['profit'] if pd.notna(idx_e) else 0
                row[f"손익_{label}"] = int(p_e - p_s)
            except:
                row[f"손익_{label}"] = 0

        # ---------------------------------------------------------
        # [그룹별 전용] inception 이전 연도/월/분기 컬럼 NaN 처리
        # - 그룹별은 inception 부터가 의미 있는 시점이므로 그 이전 기간은 빈칸
        # - 다른 행(전체/계좌별/국가별/테마별)은 그대로 표시
        # - 월/분기는 inception 이 속한 기간 자체는 표시(부분 기간이지만 정보 가치 있음),
        #   inception 보다 더 이른 월/분기만 빈칸
        # ---------------------------------------------------------
        if dimension_name == '그룹별' and inception_str:
            inc_dt = pd.to_datetime(inception_str)
            inc_year = inc_dt.year
            inc_year_month = (inc_dt.year, inc_dt.month)
            inc_year_q = (inc_dt.year, (inc_dt.month - 1) // 3 + 1)

            # 연도: 기존 로직 유지 (inception 연도 포함 빈칸 — 부분 연도는 노이즈로 간주)
            for year in range(2020, curr_year + 1):
                if year <= inc_year:
                    if f'{year}년' in row: row[f'{year}년'] = np.nan
                    if f'MWR_{year}년' in row: row[f'MWR_{year}년'] = np.nan

            # 월: inception 월보다 더 이른 월만 빈칸 (inception 월 자체는 부분 데이터로 표시)
            for y, m in _iter_months(MONTHLY_START_YEAR, MONTHLY_START_MONTH, latest_year, latest_month):
                if (y, m) < inc_year_month:
                    label = _month_label(y, m)
                    if label in row: row[label] = np.nan
                    if f"MWR_{label}" in row: row[f"MWR_{label}"] = np.nan
                    if f"손익_{label}" in row: row[f"손익_{label}"] = np.nan

            # 분기: 위와 동일 정책
            for y, q in _iter_quarters(QUARTERLY_START_YEAR, QUARTERLY_START_Q, latest_year, latest_q):
                if (y, q) < inc_year_q:
                    label = _quarter_label(y, q)
                    if label in row: row[label] = np.nan
                    if f"MWR_{label}" in row: row[f"MWR_{label}"] = np.nan
                    if f"손익_{label}" in row: row[f"손익_{label}"] = np.nan

        return row

    def calculate_bm_stats():
        if not has_market_data: return []
        bm_rows = []
        def get_price(ticker, date_obj):
            if ticker not in df_market.columns: return None
            idx = df_market.index.asof(date_obj)
            if pd.isna(idx): return None
            return df_market.loc[idx][ticker]

        def get_bm_ret(ticker, start_date, end_date):
            p_s = get_price(ticker, pd.to_datetime(start_date))
            p_e = get_price(ticker, pd.to_datetime(end_date))
            if p_s and p_e and p_s != 0: return (p_e - p_s) / p_s
            return 0.0

        periods_map = {
            '1일': (get_prev_bday(latest_date), latest_date),
            'WTD(이번주)': (get_last_friday(latest_date), latest_date),
            'W-1(저번주)': (get_last_friday(latest_date) - timedelta(days=7), get_last_friday(latest_date)),
            'W-2(2주전)': (get_last_friday(latest_date) - timedelta(days=14), get_last_friday(latest_date) - timedelta(days=7)),
            'W-3(3주전)': (get_last_friday(latest_date) - timedelta(days=21), get_last_friday(latest_date) - timedelta(days=14)),
            'MTD(이번달)': (get_last_day_of_prev_month(latest_date), latest_date),
            'M-1(지난달)': (get_last_day_of_prev_month(get_last_day_of_prev_month(latest_date)), get_last_day_of_prev_month(latest_date)),
            'M-2(2달전)': (get_last_day_of_prev_month(get_last_day_of_prev_month(get_last_day_of_prev_month(latest_date))), get_last_day_of_prev_month(get_last_day_of_prev_month(latest_date))),
            'YTD': (datetime(latest_date.year, 1, 1), latest_date),
            '누적수익률(%)': ('2020-01-01', latest_date),
            f"지정({CUSTOM_START_DATE[2:]}~)": (CUSTOM_START_DATE, latest_date)
        }

        # 벤치마크에도 inception 지정기간 추가 (전체/계좌/국가/테마와 비교 가능하도록)
        # [#5 개선] 헬퍼로 단일화
        for inc_date in sorted(_collect_inception_dates(GROUP_INCEPTION_DATES, ACCOUNT_INCEPTION_DATES, MILESTONE_DATES)):
            periods_map[f"지정({inc_date[2:]}~)"] = (inc_date, latest_date)
        
        # 벤치마크에도 2020년까지 연도별 기간 추가
        for year in range(latest_date.year - 1, 2019, -1):
            periods_map[f'{year}년'] = (f'{year}-01-01', f'{year+1}-01-01')

        # 벤치마크에도 월별/분기별 기간 추가 (포트폴리오 행과 비교용)
        latest_year = latest_date.year
        latest_month = latest_date.month
        latest_q = (latest_month - 1) // 3 + 1
        for y, m in _iter_months(MONTHLY_START_YEAR, MONTHLY_START_MONTH, latest_year, latest_month):
            s_dt, e_dt = _month_bounds(y, m)
            periods_map[_month_label(y, m)] = (s_dt, e_dt)
        for y, q in _iter_quarters(QUARTERLY_START_YEAR, QUARTERLY_START_Q, latest_year, latest_q):
            s_dt, e_dt = _quarter_bounds(y, q)
            periods_map[_quarter_label(y, q)] = (s_dt, e_dt)

        targets = [('벤치마크', '2026 MAIN BM', MAIN_BM_WEIGHTS)]
        for t, n in MARKET_COL_MAP.items(): targets.append(('벤치마크', n, {t: 1.0}))
        
        for g_name, s_name, weights in targets:
            row = {'구분': g_name, '상세': s_name}
            for col_name, (s_dt, e_dt) in periods_map.items():
                c_ret, v_w = 0.0, 0.0
                for ticker, w in weights.items():
                    if ticker in df_market.columns:
                        ret = get_bm_ret(ticker, s_dt, e_dt)
                        c_ret += ret * w; v_w += w
                row[col_name] = (c_ret/v_w) if v_w>0 else 0.0
            bm_rows.append(row)
        return bm_rows

    print(" [1] 포트폴리오 분석 진행 중...")
    if res := calculate_stats(df_log, "전체", "Total Portfolio"): final_results.append(res)
    for g in df_log['group_name'].unique():
        if g != '기타' and (res := calculate_stats(df_log[df_log['group_name'] == g], "그룹별", g)): final_results.append(res)
    for acc in df_log['clean_account'].unique():
        if res := calculate_stats(df_log[df_log['clean_account'] == acc], "계좌별", str(acc)): final_results.append(res)
    for c in [x for x in df_log['country'].unique() if x and x != '기타']:
        if res := calculate_stats(df_log[df_log['country'] == c], "국가별", c, 'asset'): final_results.append(res)
    for t in [x for x in df_log['theme'].unique() if x and x != '기타']:
        if res := calculate_stats(df_log[df_log['theme'] == t], "테마별", t, 'asset'): final_results.append(res)

    print(" [2] 벤치마크 분석 진행 중...")
    if bm_results := calculate_bm_stats(): final_results.extend(bm_results)

    # [#2 개선] 클램프 진단 요약 출력 — 컨텍스트(어느 dim/category) 포함
    # 동일한 (ctx, 요청일, 실제일) 조합은 중복 제거 (TWR/MWR 같이 묶임)
    #
    # 이 메시지가 의미하는 것 (정확한 표현):
    #   "이 차원·카테고리로 필터링한 부분집합" 의 daily 집계에서
    #   market_value_krw > 0 인 가장 이른 날짜가, 요청한 시작일보다 늦다.
    #
    # 클램프가 발생하는 두 가지 시나리오:
    #   A) portfolio_log 자체에 그 날짜 엔트리가 없음 → backfill 필요할 수 있음
    #   B) portfolio_log 엔 있는데, 그 차원/카테고리 필터링 결과엔 해당 날짜의
    #      보유가 없음 (예: 국가별|미국 카테고리는 미국 주식 첫 매수일까지
    #      market_value 가 0 이므로 이 카테고리의 first_valid 는 그 날 이후가 됨)
    #      → 이건 정상 동작이고 무시해도 됨.
    if clamp_log:
        seen = set()
        unique_clamps = []
        for c in clamp_log:
            key = (c['ctx'], c['requested'], c['actual'])
            if key not in seen:
                seen.add(key)
                unique_clamps.append(key)
        # ctx 가나다순 → 요청일 순으로 정렬
        unique_clamps.sort()

        print(f"\n  [Clamp Notice] 일부 부분집합의 첫 유효일(market_value>0)이")
        print(f"  요청 시작일보다 늦어 자동 조정된 케이스 {len(unique_clamps)}건:")
        # 출처 표시 폭 자동 조정
        max_ctx_len = max((len(c[0]) for c in unique_clamps), default=20)
        for ctx, req, act in unique_clamps:
            print(f"    - {ctx:<{max_ctx_len}} | 요청: {req} → 실제: {act}")
        print(f"  ※ 해석 가이드:")
        print(f"     · 전체/그룹별/계좌별 에서 발생 → portfolio_log 의 해당 출처가")
        print(f"       그 날짜에 데이터가 없는 것. 의도와 다르면 backfill 점검 필요.")
        print(f"     · 국가별/테마별 에서 발생 → 그 카테고리의 보유가 그 날짜에 0 이라")
        print(f"       자연스럽게 첫 매수일부터 계산됨. 정상 동작이며 무시 가능.")

    if final_results:
        df_res = pd.DataFrame(final_results)
        curr_year = latest_date.year
        
        # 지정기간 열 이름 변수화
        c_roi = f"지정({CUSTOM_START_DATE[2:]}~)"
        c_mwr = f"MWR_지정({CUSTOM_START_DATE[2:]}~)"
        c_pl  = f"지정_손익({CUSTOM_START_DATE[2:]}~)"

        # [그룹별/계좌별 전용] inception 기준 지정기간 컬럼명들 수집
        # [#5 개선] 헬퍼로 단일화. 날짜 오름차순 정렬 (오래된 → 최근)
        inception_dates_sorted = sorted(_collect_inception_dates(GROUP_INCEPTION_DATES, ACCOUNT_INCEPTION_DATES, MILESTONE_DATES))
        inc_twr_cols = [f"지정({d[2:]}~)" for d in inception_dates_sorted]
        inc_mwr_cols = [f"MWR_지정({d[2:]}~)" for d in inception_dates_sorted]
        # [신규] inception별 손익 / 평균투자원금 컬럼들
        inc_pl_cols = [f"지정_손익({d[2:]}~)" for d in inception_dates_sorted]
        inc_avg_cap_cols = [f"지정기간 평균투자원금({d[2:]}~)" for d in inception_dates_sorted]

        # [호섭님 요청] 완벽한 열 순서 재배치 (TWR 묶음 먼저, 그다음 MWR 묶음)
        # [개선] '1주' 제거 (WTD 와 거의 동일해서 잉여)
        base_cols = [
            '구분', '상세', '1일', 'WTD(이번주)',
            'W-1(저번주)', 'W-2(2주전)', 'W-3(3주전)',
            'MTD(이번달)', 'M-1(지난달)', 'M-2(2달전)'
        ]

        twr_cols = ['YTD']
        mwr_cols = ['MWR_YTD']

        # 연도별 열을 각각의 리스트에 모으기 (최신 → 오래된 순)
        for year in range(curr_year - 1, 2019, -1):
            twr_cols.append(f'{year}년')
            mwr_cols.append(f'MWR_{year}년')

        # 누적 → CUSTOM 지정기간 → inception 지정기간 순으로 배치
        twr_cols.extend(['누적수익률(%)', c_roi] + inc_twr_cols)
        mwr_cols.extend(['MWR_누적', c_mwr] + inc_mwr_cols)

        # ---------------------------------------------------------
        # [신규] 월별 / 분기별 컬럼 — 최신 → 오래된 순(연도와 동일 정책)
        # TWR / MWR / 손익 3종 세트로 자동 생성
        # ---------------------------------------------------------
        latest_year = latest_date.year
        latest_month = latest_date.month
        latest_q = (latest_month - 1) // 3 + 1

        # 월: 시간 오름차순으로 모은 뒤 reversed 로 최신 → 오래된 순으로 정렬
        monthly_pairs = list(_iter_months(MONTHLY_START_YEAR, MONTHLY_START_MONTH, latest_year, latest_month))
        monthly_pairs.reverse()
        monthly_twr_cols = [_month_label(y, m) for y, m in monthly_pairs]
        monthly_mwr_cols = [f"MWR_{_month_label(y, m)}" for y, m in monthly_pairs]
        monthly_pl_cols  = [f"손익_{_month_label(y, m)}" for y, m in monthly_pairs]

        # 분기: 동일
        quarterly_pairs = list(_iter_quarters(QUARTERLY_START_YEAR, QUARTERLY_START_Q, latest_year, latest_q))
        quarterly_pairs.reverse()
        quarterly_twr_cols = [_quarter_label(y, q) for y, q in quarterly_pairs]
        quarterly_mwr_cols = [f"MWR_{_quarter_label(y, q)}" for y, q in quarterly_pairs]
        quarterly_pl_cols  = [f"손익_{_quarter_label(y, q)}" for y, q in quarterly_pairs]

        # TWR / MWR 묶음에 월별 → 분기별 순서로 추가
        twr_cols.extend(monthly_twr_cols + quarterly_twr_cols)
        mwr_cols.extend(monthly_mwr_cols + quarterly_mwr_cols)

        # 손익 묶음(CUSTOM + inception들 + 월별 + 분기별) → 자산/원금 → 평균원금
        end_cols = ([c_pl] + inc_pl_cols + monthly_pl_cols + quarterly_pl_cols
                    + ['평가손익', '총자산', '투자원금', '지정기간 평균투자원금']
                    + inc_avg_cap_cols)
        
        # 베이스 -> TWR 쫙 -> MWR 쫙 -> 나머지 정보 순서로 합치기
        final_cols = base_cols + twr_cols + mwr_cols + end_cols
        
        # 중복 제거 (CUSTOM_START_DATE 와 GROUP_INCEPTION_DATES 의 한 값이 같을 때 발생)
        # dict.fromkeys 는 순서 유지하면서 중복 제거 (Python 3.7+)
        existing_cols = list(dict.fromkeys([c for c in final_cols if c in df_res.columns]))
        df_res = df_res[existing_cols]

        # ---------------------------------------------------------
        # [NaN 처리]
        # - 연도별/월별/분기별 컬럼: 그룹별 행에만 NaN(빈칸) 있음 → 보존
        # - 그 외 컬럼: 0 채움 (inception 지정 컬럼은 모든 행에 값 있어서 보호 불필요)
        # [개선] 명시적 set 으로 변경 — 패턴 매칭보다 안전, 미래 컬럼 추가에도 견고
        # ---------------------------------------------------------
        preserve_nan_cols = set()
        for year in range(2020, curr_year + 1):
            preserve_nan_cols.add(f'{year}년')
            preserve_nan_cols.add(f'MWR_{year}년')
        for _label in monthly_twr_cols:
            preserve_nan_cols.update([_label, f'MWR_{_label}', f'손익_{_label}'])
        for _label in quarterly_twr_cols:
            preserve_nan_cols.update([_label, f'MWR_{_label}', f'손익_{_label}'])

        for col in df_res.columns:
            if col not in preserve_nan_cols:
                df_res[col] = df_res[col].fillna(0)

        # 남은 NaN(=그룹별 행의 연도 컬럼)을 빈 문자열로 변환 → 시트에 빈칸 출력
        df_res = df_res.where(pd.notna(df_res), "")

        # ---------------------------------------------------------
        # [계좌별 장기 지표 정화]
        # 계좌별 행은 "운용 단위"가 아니라 "행정 단위"라서, 계좌 간 자금 이동이
        # 운용 결정과 무관하게 장기 수익률을 왜곡시킴.
        # 예: 60271589 의 5/14~ MWR 77% vs TWR 2% 는 12월의 ₩86M 그룹 내부 이체
        #     때문이지 실제 운용 성과가 아님.
        # → 계좌별 행에서만 장기 지표(YTD/연도별/누적/지정 TWR+MWR+손익+평균원금)를
        #   빈칸 처리. 단기/월별/분기별/스냅샷은 진단 목적으로 유지.
        # 운용 성과의 진실은 그룹별 행에서 보면 됨.
        # ---------------------------------------------------------
        account_blank_cols = []
        # YTD/MWR_YTD 는 살림 — 올해(2026~)부터는 계좌 간 큰 자금 이동이 없어서
        # YTD 가 계좌별로도 의미 있는 진단 지표가 됨.
        # 연도별 (2020 ~ 작년)
        for year in range(2020, curr_year + 1):
            account_blank_cols.extend([f'{year}년', f'MWR_{year}년'])
        # 누적
        account_blank_cols.extend(['누적수익률(%)', 'MWR_누적'])
        # CUSTOM 지정 (TWR/MWR/손익/평균원금)
        account_blank_cols.extend([c_roi, c_mwr, c_pl, '지정기간 평균투자원금'])
        # inception 지정 (TWR/MWR/손익/평균원금)
        account_blank_cols.extend(inc_twr_cols)
        account_blank_cols.extend(inc_mwr_cols)
        account_blank_cols.extend(inc_pl_cols)
        account_blank_cols.extend(inc_avg_cap_cols)

        account_mask = df_res['구분'] == '계좌별'
        if account_mask.any():
            for col in account_blank_cols:
                if col in df_res.columns:
                    df_res.loc[account_mask, col] = ""

        # [복구 완료!] 날아갔던 NIC 검증표 재생성
        audit_df = df_log[df_log['date'] == latest_date].groupby('clean_account').agg({
             'group_name': 'first', 'net_invested_capital': 'max', 'market_value_krw': 'sum'
        }).reset_index()
        audit_df['PL'] = audit_df['market_value_krw'] - audit_df['net_invested_capital']

        try:
            doc = sheet_instance.spreadsheet
            try: ps = doc.worksheet("performance_summary")
            except: ps = doc.add_worksheet("performance_summary", 100, 30)
            ps.clear()
            ps.update(range_name='A1', values=[df_res.columns.tolist()] + df_res.values.tolist())
            
            # 메인 리포트 5칸 아래에 NIC 검증표 예쁘게 붙여넣기
            start_row = len(df_res) + 5
            ps.update(range_name=f'A{start_row}', values=[["■ 계좌별 원금(NIC) 검증표"]])
            audit_vals = [audit_df.columns.tolist()] + audit_df.values.tolist()
            ps.update(range_name=f'A{start_row+1}', values=json.loads(json.dumps(audit_vals, default=str)))

            # ---------------------------------------------------------
            # [의사결정 트리] NIC 검증표 아래에 자동으로 박아두기
            # 매번 ps.clear() 되어도 코드가 자동 재생성하므로 안 사라짐
            # ---------------------------------------------------------
            tree_start_row = start_row + len(audit_df) + 4  # NIC표 + 4칸 띄움
            decision_tree = [
                ["■ TWR vs MWR 의사결정 트리"],
                ["  계좌/카테고리 보면서:"],
                ["  ├── TWR ≈ MWR (차이 5%p 이내)"],
                ["  │   → MWR만 보면 됨. 단순."],
                ["  │"],
                ["  ├── TWR ≫ MWR (TWR이 훨씬 좋음)"],
                ["  │   → 종목은 잘 골랐는데 타이밍에서 까먹음"],
                ["  │   → 자금 흐름 점검 필요 (와이프 자금 임팩트?)"],
                ["  │"],
                ["  └── MWR ≫ TWR (MWR이 훨씬 좋음)"],
                ["      → 타이밍이 좋았다 (운? 실력?)"],
                ["      → 반복 가능성 점검. 종목 셀렉션은 평이"],
                [""],
                ["■ 음영 정책"],
                ["  - 회색 = 참고용 (메인 지표 아님)"],
                ["  - 굵게 = 메인 지표 (집중해서 봐야 함)"],
                ["  - 계좌별 '지정(7/21~) TWR'은 회색 제외 (와이프 변호용)"],
            ]
            ps.update(range_name=f'A{tree_start_row}', values=decision_tree)

            # ---------------------------------------------------------
            # [음영/강조 처리] 행 종류별로 차등 적용
            # - 계좌별/국가별/테마별: TWR 회색 (단, 계좌별의 지정(7/21~) TWR은 강조)
            # - 전체/그룹별/계좌별/국가별/테마별: MWR 강조 (벤치마크 제외)
            # ---------------------------------------------------------
            all_cols = list(df_res.columns)

            # 컬럼 분류 (인덱스 1-based 시트용)
            def col_letter(col_name):
                if col_name not in all_cols: return None
                return gspread.utils.rowcol_to_a1(1, all_cols.index(col_name) + 1).rstrip('1')

            # inception 지정 TWR/MWR 컬럼명
            # [#5 개선] 헬퍼로 단일화 — 전역 GROUP/ACCOUNT_INCEPTION_DATES 에서 수집
            _inception_sorted = sorted(_collect_inception_dates(GROUP_INCEPTION_DATES, ACCOUNT_INCEPTION_DATES, MILESTONE_DATES))
            inception_twr_col_names = [f"지정({d[2:]}~)" for d in _inception_sorted if f"지정({d[2:]}~)" in all_cols]
            inception_mwr_col_names = [f"MWR_지정({d[2:]}~)" for d in _inception_sorted if f"MWR_지정({d[2:]}~)" in all_cols]

            # 지정_손익(25-05-14~) / 손익_2025-05 / 손익_2025-Q3 등 손익 컬럼들은
            # 수익률(%) 이 아니므로 음영 대상에서 제외
            pl_cols = [c for c in all_cols
                       if c.startswith('지정_손익(') or c.startswith('손익_')]
            # 지정기간 평균투자원금(...) 컬럼들도 수익률 아님 → 음영 대상 제외
            avg_cap_cols = [c for c in all_cols if c.startswith('지정기간 평균투자원금')]

            # CUSTOM_START_DATE 기준 지정 TWR/MWR 컬럼명 (변호용 - 메인 운용 시작일)
            custom_twr_col = f"지정({CUSTOM_START_DATE[2:]}~)"
            custom_mwr_col = f"MWR_지정({CUSTOM_START_DATE[2:]}~)"

            # 회색 보호 대상 = inception 지정 TWR + CUSTOM 지정 TWR (둘 다 변호용)
            twr_protected_cols = set(inception_twr_col_names)
            if custom_twr_col in all_cols:
                twr_protected_cols.add(custom_twr_col)

            # TWR 수익률 컬럼들 (단기 + 연도 + 누적, 손익/평균원금 제외)
            non_return_cols = {'구분', '상세', '평가손익', '총자산', '투자원금', '지정기간 평균투자원금'}
            non_return_cols.update(pl_cols)       # 지정_손익 류 추가 보호
            non_return_cols.update(avg_cap_cols)  # 지정기간 평균투자원금 류 추가 보호

            twr_return_cols_all = [c for c in all_cols
                                   if c not in non_return_cols
                                   and not c.startswith('MWR_')]
            # ---------------------------------------------------------
            # [정책 변경] TWR 회색 대상 = "장기" 만 (연도별 + 누적)
            # 단기/중기 TWR (1일, WTD, W-1~3, MTD, M-1~2, YTD, 월별, 분기별) 은
            # 회색 처리 안 함 — 그 기간엔 자금 흐름 효과 미미해서 TWR≈MWR 이고
            # 표준 단기 성과 지표로 그냥 보여줘도 무방.
            # ---------------------------------------------------------
            _year_pattern = re.compile(r'^\d{4}년$')
            twr_gray_cols = [c for c in twr_return_cols_all
                             if (_year_pattern.match(c) or c == '누적수익률(%)')
                             and c not in twr_protected_cols]
            # 계좌별 강조 대상 = inception 지정 TWR (변호용 강조)
            twr_account_bold_cols = inception_twr_col_names

            # MWR 수익률 컬럼들 (강조 대상)
            mwr_bold_cols = [c for c in all_cols 
                             if c not in non_return_cols 
                             and c.startswith('MWR_')]

            # [#3 개선] 행을 dim별로 그룹화 — 행 인덱스 리스트로 보관
            # 이전 버전은 [first, last] 만 보관해서 같은 dim의 행이 비연속으로
            # 흩어지면 중간 행까지 음영이 번지는 버그 가능성 있었음.
            # 이제는 모든 행 인덱스를 보관하고, _runs_of_consecutive 로
            # 연속 구간만 묶어 range 처리 → 비연속에도 정확함.
            row_groups = {}
            for idx, row in df_res.iterrows():
                dim = row['구분']
                sheet_row = idx + 2  # 헤더 1행 + 0-indexed 보정
                row_groups.setdefault(dim, []).append(sheet_row)

            # 포맷 정의
            GRAY_FMT = {
                "backgroundColor": {"red": 0.93, "green": 0.93, "blue": 0.93},
                "textFormat": {
                    "bold": False,  # 회색일 땐 굵게 명시적으로 해제 (의미 없음 표시)
                    "foregroundColor": {"red": 0.55, "green": 0.55, "blue": 0.55}
                }
            }
            BOLD_FMT = {"textFormat": {"bold": True}}

            def cols_to_ranges(col_names, row_start, row_end):
                """컬럼 리스트를 인접한 그룹으로 묶어 range 문자열 리스트로 변환 (API 호출 최소화)"""
                if not col_names: return []
                indices = sorted([all_cols.index(c) + 1 for c in col_names])
                ranges = []
                run_start = indices[0]
                run_end = indices[0]
                for i in indices[1:]:
                    if i == run_end + 1:
                        run_end = i
                    else:
                        s_a1 = gspread.utils.rowcol_to_a1(row_start, run_start)
                        e_a1 = gspread.utils.rowcol_to_a1(row_end, run_end)
                        ranges.append(f"{s_a1}:{e_a1}")
                        run_start = i; run_end = i
                s_a1 = gspread.utils.rowcol_to_a1(row_start, run_start)
                e_a1 = gspread.utils.rowcol_to_a1(row_end, run_end)
                ranges.append(f"{s_a1}:{e_a1}")
                return ranges

            # =====================================================================
            # [#4 개선] ⚠️ 포맷 적용 순서 — 변경 금지!
            # ---------------------------------------------------------------------
            # format_requests 는 추가된 순서대로 적용되며, 같은 셀에 여러 규칙이
            # 겹치면 "마지막 규칙이 이긴다"(last-wins) 정책.
            #
            # 현재 의도된 순서:
            #   0. RESET   — 전체 영역 흰 배경/검정 글자/일반 굵기로 초기화
            #   1. GRAY    — "장기" TWR (연도별 + 누적) 회색 (계좌별/국가별/테마별)
            #                 단기/중기/월별/분기별 TWR 은 회색 안 함 (그대로 표시)
            #   2. BOLD    — 지정 TWR 보호용 굵게 (1번을 덮어씀, 변호용)
            #   3. BOLD    — MWR 묶음 굵게 (전체/그룹별/계좌별/국가별/테마별)
            #   4. GRAY    — 그룹별 행의 "다른 inception" 컬럼 회색 (3번을 덮어씀)
            #
            # 핵심: 4번이 3번보다 뒤에 있어야 그룹별 행에서 자기 inception MWR만
            # 굵게 남고 다른 inception MWR 은 회색이 된다. 이 순서를 바꾸면
            # 음영이 뒤집히므로 절대 변경 금지.
            # =====================================================================
            format_requests = []

            # ---------------------------------------------------------
            # [⭐ 핵심] 0. 데이터 영역 포맷 리셋 (이전 실행의 잔재 음영 제거)
            # ps.clear() 는 데이터만 지우고 포맷은 그대로 유지함.
            # 따라서 새 음영을 적용하기 전에 전 영역을 흰 배경/검은 글자/일반 굵기로 초기화.
            # ---------------------------------------------------------
            total_rows = len(df_res) + 1  # 헤더 1 + 데이터
            total_cols = len(all_cols)
            reset_range = f"A1:{gspread.utils.rowcol_to_a1(total_rows, total_cols)}"
            RESET_FMT = {
                "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                "textFormat": {
                    "bold": False,
                    "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}
                }
            }
            format_requests.append({"range": reset_range, "format": RESET_FMT})

            # 1. "장기" TWR 회색 적용 (연도별 + 누적만, 단기/중기는 그대로 표시)
            # 정책: 자금 흐름 효과가 누적되는 장기 지표만 "MWR 보세요" 라는 의미로 회색.
            #       단기(1일/주차/월차) 및 월별/분기별은 표준 단기 성과 지표로 그냥 노출.
            # 보호: 지정 TWR (5/14~, 7/21~) 은 변호용으로 모든 행에서 굵게 (단계 2에서 처리)
            # [#3 개선] _runs_of_consecutive 로 비연속 행도 안전하게 묶어 처리
            for dim in ['계좌별', '국가별', '테마별']:
                rows_in_dim = sorted(row_groups.get(dim, []))
                for r_start, r_end in _runs_of_consecutive(rows_in_dim):
                    for rng in cols_to_ranges(twr_gray_cols, r_start, r_end):
                        format_requests.append({"range": rng, "format": GRAY_FMT})

            # 2. 지정 TWR 컬럼 강조 (변호용) - 계좌별/국가별/테마별 모두 동일
            # twr_protected_cols = 지정(5/14~) + 지정(7/21~) - 둘 다 변호용 강조 대상
            twr_protected_list = sorted(twr_protected_cols)
            for dim in ['계좌별', '국가별', '테마별']:
                rows_in_dim = sorted(row_groups.get(dim, []))
                for r_start, r_end in _runs_of_consecutive(rows_in_dim):
                    for rng in cols_to_ranges(twr_protected_list, r_start, r_end):
                        format_requests.append({"range": rng, "format": BOLD_FMT})

            # 3. MWR 강조 적용 (벤치마크 제외)
            for dim in ['전체', '그룹별', '계좌별', '국가별', '테마별']:
                rows_in_dim = sorted(row_groups.get(dim, []))
                for r_start, r_end in _runs_of_consecutive(rows_in_dim):
                    for rng in cols_to_ranges(mwr_bold_cols, r_start, r_end):
                        format_requests.append({"range": rng, "format": BOLD_FMT})

            # ---------------------------------------------------------
            # 4. [그룹별 전용] 자기 inception 이 아닌 지정 컬럼 회색
            # - 멘토 그룹 행: 지정(25-07-21~) TWR/MWR 회색 (의미 없음)
            # - HS 그룹 행:   지정(25-05-14~) TWR/MWR 회색 (의미 없음)
            # - MWR 강조(3번)는 이미 적용됐지만, 회색이 마지막에 덮어씀 → 의도대로 회색 우선
            # ---------------------------------------------------------
            # 모든 의미 있는 inception 날짜 (CUSTOM + 그룹별 inception 통합)
            all_designated_dates = set(GROUP_INCEPTION_DATES.values()) | {CUSTOM_START_DATE}
            all_designated_dates = {d for d in all_designated_dates if d}

            # 그룹별 행 단위로 순회 (각 그룹마다 inception 다름)
            for idx, row in df_res.iterrows():
                if row['구분'] != '그룹별': continue
                sheet_row = idx + 2
                group_name = row['상세']
                # 자기 그룹의 inception (없으면 CUSTOM_START_DATE 로 fallback)
                my_inception = GROUP_INCEPTION_DATES.get(group_name) or CUSTOM_START_DATE
                # 자기 것 외의 지정 날짜들의 TWR/MWR 컬럼을 회색 처리
                other_dates = all_designated_dates - {my_inception}
                other_cols = []
                for d in other_dates:
                    twr_c = f"지정({d[2:]}~)"
                    mwr_c = f"MWR_지정({d[2:]}~)"
                    if twr_c in all_cols: other_cols.append(twr_c)
                    if mwr_c in all_cols: other_cols.append(mwr_c)
                if other_cols:
                    for rng in cols_to_ranges(other_cols, sheet_row, sheet_row):
                        format_requests.append({"range": rng, "format": GRAY_FMT})

            if format_requests:
                ps.batch_format(format_requests)
                print(f"  [Format] 음영/강조 {len(format_requests)}개 규칙 적용 완료.")
            
            print("--- [성공] 리포트 데이터 + NIC 검증표 + 의사결정 트리 + 음영 처리 완료 ---")
        except Exception as e:
            print(f"[!] 업로드 실패: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    run_performance_analysis()

    