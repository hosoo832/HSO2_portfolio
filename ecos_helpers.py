"""
ecos_helpers.py — ECOS (한국은행 경제통계시스템) API 연동 모듈

기능:
  - KR 10Y 국고채 금리 (일별)
  - KOSPI / KOSDAQ 거래대금 (일별, 단위 억원)

환경변수:
  ECOS_API_KEY — ECOS Open API 인증키 (https://ecos.bok.or.kr 에서 발급)
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

ECOS_BASE = "https://ecos.bok.or.kr/api"

# 사용할 시리즈 정의: 컬럼명 -> (stat_code, item_code, unit)
ECOS_DAILY = {
    'KR_10Y_Bond_rate': ('817Y002', '010210000', '연%'),     # 국고채 10년
    'KOSPI_volume':     ('802Y001', '0088000',   '억원'),    # KOSPI 거래대금
    'KOSDAQ_volume':    ('802Y001', '0091000',   '억원'),    # KOSDAQ 거래대금
}


def get_api_key():
    key = os.environ.get('ECOS_API_KEY')
    if not key:
        raise RuntimeError("[ECOS] 환경변수 ECOS_API_KEY 가 비어있음.")
    return key


def fetch_daily_series(stat_code, item_code, start_date, end_date, api_key=None):
    """
    ECOS 일별 데이터를 DataFrame 으로 반환.

    start_date / end_date : 'YYYYMMDD' 또는 'YYYY-MM-DD'
    Returns: DataFrame with DatetimeIndex and 'value' column (float).
    """
    api_key = api_key or get_api_key()
    s = str(start_date).replace("-", "")
    e = str(end_date).replace("-", "")

    url = (f"{ECOS_BASE}/StatisticSearch/{api_key}/json/kr/1/100000/"
           f"{stat_code}/D/{s}/{e}/{item_code}")
    r = requests.get(url, timeout=30)
    j = r.json()

    if "RESULT" in j:
        # ECOS 에러 응답: {"RESULT": {"CODE": "...", "MESSAGE": "..."}}
        raise RuntimeError(f"[ECOS API 에러] {j['RESULT']}")

    rows = j.get("StatisticSearch", {}).get("row", [])
    if not rows:
        return pd.DataFrame(columns=['value'])

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['TIME'], format='%Y%m%d', errors='coerce')
    df['value'] = pd.to_numeric(df['DATA_VALUE'], errors='coerce')
    df = df.dropna(subset=['date'])
    df = df.set_index('date')[['value']]
    return df


def fetch_all_history(start_date, end_date, api_key=None, verbose=True):
    """
    ECOS_DAILY 정의된 모든 시리즈를 한 번에 받아 가로로 합친 DataFrame 반환.

    Returns: DataFrame index=date, columns=ECOS_DAILY 키들.
    """
    api_key = api_key or get_api_key()
    out = pd.DataFrame()
    for label, (stat, item, unit) in ECOS_DAILY.items():
        try:
            df = fetch_daily_series(stat, item, start_date, end_date, api_key=api_key)
            df = df.rename(columns={'value': label})
            if out.empty:
                out = df
            else:
                out = out.join(df, how='outer')
            if verbose:
                print(f"  [ECOS] {label}: {len(df)}일치 받음")
        except Exception as e:
            if verbose:
                print(f"  [ECOS] {label} 실패: {e}")
    return out


def fetch_latest_two(label, lookback_days=14, api_key=None):
    """
    cron 용 — 특정 라벨의 가장 최근 2개 유효값 반환.

    Returns: (latest_value, prev_value) 또는 (None, None)
    """
    if label not in ECOS_DAILY:
        return (None, None)
    stat, item, _ = ECOS_DAILY[label]
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    try:
        df = fetch_daily_series(
            stat, item,
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
            api_key=api_key
        )
        df = df.dropna()
        if len(df) >= 2:
            return (float(df['value'].iloc[-1]), float(df['value'].iloc[-2]))
        elif len(df) == 1:
            return (float(df['value'].iloc[-1]), None)
        else:
            return (None, None)
    except Exception as e:
        print(f"  [ECOS fetch_latest_two] {label} 실패: {e}")
        return (None, None)
