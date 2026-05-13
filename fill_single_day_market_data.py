"""
fill_single_day_market_data.py — market_data 시트의 특정 1일치 행 보충

용도:
  - cron(`daily-market.yml`)이 실패했거나, 사용자가 특정 날짜 행을 지운 뒤
    그 날짜만 다시 채워넣고 싶을 때.
  - 시트 통째로 덮어쓰지 않고 1행만 append (cron 과 동일 동작).

사용법:
  python fill_single_day_market_data.py 2026-05-11   # 명시
  python fill_single_day_market_data.py              # 인자 없으면 어제

설계:
  - backfill_market_data.py 의 컬럼/계산 로직을 그대로 따와서 cron 결과와 형식 일치.
  - 등락률 계산을 위해 target 기준 14일 윈도우를 받은 뒤, target 날짜 행만 추출.
  - 실행 전에 생성된 행 미리보기 + y/N 확인 prompt (안전장치).

주의:
  - 같은 날짜 행이 이미 시트에 있으면 중복 append 됨 → 실행 전 시트에서 제거 권장.
  - target_date 가 한국/미국 동시 휴장일이면 직전 거래일 값으로 ffill 됨.
"""
import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import gspread


SHEET_NAME = "거래내역"
WORKSHEET_NAME = "market_data"

# backfill_market_data.py 의 FINAL_COLUMNS_37 과 동일 (39 컬럼이지만 변수명 유지)
FINAL_COLUMNS = [
    "date", "KOSPI_price", "KOSPI_chg_pct", "KOSPI_volume",
    "KOSDAQ_price", "KOSDAQ_chg_pct", "KOSDAQ_volume",
    "SP500_price", "SP500_chg_pct",
    "NASDAQ_price", "NASDAQ_chg_pct",
    "SHANGHAI_price", "SHANGHAI_chg_pct",
    "NIKKEI_price", "NIKKEI_chg_pct",
    "DAX_price", "DAX_chg_pct",
    "USDKRW_price", "USDKRW_chg_pct",
    "USD_IDX_price", "USD_IDX_chg_pct",
    "US_10Y_Bond_rate", "US_10Y_Bond_chg_bps",
    "US_30Y_Bond_rate", "US_30Y_Bond_chg_bps",
    "WTI_price", "WTI_chg_pct",
    "GOLD_price", "GOLD_chg_pct",
    "BTC_price", "BTC_chg_pct",
    "VIX_price", "VIX_chg_pct",
    "KR_10Y_Bond_rate", "KR_10Y_Bond_chg_bps",
    "Customer_Deposit_value", "Customer_Deposit_chg_pct",
    "Credit_Balance_value", "Credit_Balance_chg_pct",
]

# yfinance 티커 → 컬럼명 매핑
YF_TICKERS = [
    '^KS11', '^KQ11', '^GSPC', '^IXIC', '000001.SS', '^N225', '^GDAXI',
    'USDKRW=X', 'DX-Y.NYB', '^TNX', '^TYX', 'CL=F', 'GC=F', 'BTC-USD', '^VIX',
]
YF_RENAME = {
    '^KS11': 'KOSPI_price', '^KQ11': 'KOSDAQ_price', '^GSPC': 'SP500_price',
    '^IXIC': 'NASDAQ_price', '000001.SS': 'SHANGHAI_price', '^N225': 'NIKKEI_price',
    '^GDAXI': 'DAX_price', 'USDKRW=X': 'USDKRW_price', 'DX-Y.NYB': 'USD_IDX_price',
    '^TNX': 'US_10Y_Bond_rate', '^TYX': 'US_30Y_Bond_rate', 'CL=F': 'WTI_price',
    'GC=F': 'GOLD_price', 'BTC-USD': 'BTC_price', '^VIX': 'VIX_price',
}

PRICE_COLS = [
    'KOSPI_price', 'KOSDAQ_price', 'SP500_price', 'NASDAQ_price',
    'SHANGHAI_price', 'NIKKEI_price', 'DAX_price', 'USDKRW_price', 'USD_IDX_price',
    'US_10Y_Bond_rate', 'US_30Y_Bond_rate', 'WTI_price', 'GOLD_price',
    'BTC_price', 'VIX_price',
]


def _load_dotenv(path='.env'):
    """간단 .env 파서 — backfill_market_data.py 와 동일."""
    if not os.path.exists(path):
        return
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def get_gspread_client():
    """GCP_SA_JSON 환경변수 우선, 없으면 service_account.json 사용."""
    sa_json_env = os.environ.get('GCP_SA_JSON')
    if sa_json_env:
        return gspread.service_account_from_dict(json.loads(sa_json_env))
    if os.path.exists('service_account.json'):
        return gspread.service_account(filename='service_account.json')
    sys.exit("[!] 인증 실패: GCP_SA_JSON 환경변수 또는 service_account.json 파일 필요")


def build_row_for_date(target_date_str):
    """target_date 기준 1행 생성. 반환값: list (FINAL_COLUMNS 순서)."""
    target_date = pd.to_datetime(target_date_str).date()

    # 등락률 계산용 14일 윈도우 (휴장일/주말 고려)
    start_window = (target_date - timedelta(days=14)).strftime('%Y-%m-%d')
    end_window = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')  # end exclusive

    # --- [1] yfinance ---
    print(f"\n[1/4] yfinance 다운로드 ({start_window} ~ {end_window})...")
    data = yf.download(
        tickers=YF_TICKERS, start=start_window, end=end_window,
        progress=False, auto_adjust=False,
    )
    if data.empty:
        sys.exit("[!] yfinance 결과 비어있음")

    df_yf_price = data['Close']
    df_yf = pd.DataFrame(index=df_yf_price.index)
    for tkr, col in YF_RENAME.items():
        if tkr in df_yf_price.columns:
            df_yf[col] = df_yf_price[tkr]
    df_yf.index = pd.to_datetime(df_yf.index).date

    # --- [2] ECOS (KR 10Y + 거래대금) ---
    print("\n[2/4] ECOS API 다운로드...")
    df_ecos = pd.DataFrame()
    try:
        import ecos_helpers
        ecos_key = os.environ.get('ECOS_API_KEY')
        if not ecos_key:
            print("  [!] ECOS_API_KEY 없음 — ECOS 데이터 스킵 (빈칸 유지)")
        else:
            df_ecos = ecos_helpers.fetch_all_history(
                start_window.replace('-', ''),
                end_window.replace('-', ''),
                api_key=ecos_key,
            )
            if not df_ecos.empty:
                df_ecos.index = pd.to_datetime(df_ecos.index).date
                if 'KOSPI_volume' in df_ecos.columns:
                    df_ecos['KOSPI_volume'] = df_ecos['KOSPI_volume'].round(0).astype('Int64')
                if 'KOSDAQ_volume' in df_ecos.columns:
                    df_ecos['KOSDAQ_volume'] = df_ecos['KOSDAQ_volume'].round(0).astype('Int64')
                if 'KR_10Y_Bond_rate' in df_ecos.columns:
                    df_ecos['KR_10Y_Bond_chg_bps'] = df_ecos['KR_10Y_Bond_rate'].diff() * 100
            else:
                print("  [!] ECOS 빈 결과")
    except Exception as e:
        print(f"  [!] ECOS 수집 실패: {e}")

    # --- [3] 병합 + 계산 (backfill 로직 그대로) ---
    print("\n[3/4] 데이터 병합 및 등락률 계산...")
    df_final = pd.concat([df_yf, df_ecos], axis=1)
    df_final = df_final.sort_index()

    # target_date 가 인덱스에 없으면 (한·미 동시 휴장) 강제 추가
    if target_date not in df_final.index:
        print(f"  [정보] {target_date} 가 데이터에 없음 — 직전 거래일 값으로 ffill")
        df_final.loc[target_date] = pd.NA
        df_final = df_final.sort_index()

    # 1) 등락률 (원본 NaN 유지하며 계산)
    for col_base in ['KOSPI', 'KOSDAQ', 'SP500', 'NASDAQ', 'SHANGHAI', 'NIKKEI', 'DAX',
                     'USDKRW', 'USD_IDX', 'WTI', 'GOLD', 'BTC', 'VIX']:
        price_col = f'{col_base}_price'
        if price_col in df_final.columns:
            df_final[f'{col_base}_chg_pct'] = df_final[price_col].pct_change(fill_method=None)
    if 'US_10Y_Bond_rate' in df_final.columns:
        df_final['US_10Y_Bond_chg_bps'] = df_final['US_10Y_Bond_rate'].diff() * 100
    if 'US_30Y_Bond_rate' in df_final.columns:
        df_final['US_30Y_Bond_chg_bps'] = df_final['US_30Y_Bond_rate'].diff() * 100

    # 2) 가격 ffill (휴장일 처리)
    cols_to_ffill = [c for c in PRICE_COLS if c in df_final.columns]
    df_final[cols_to_ffill] = df_final[cols_to_ffill].ffill()

    # 3) 등락률 ffill + 시작 NaN 0
    chg_cols = [c for c in df_final.columns if c.endswith(('_pct', '_bps'))]
    df_final[chg_cols] = df_final[chg_cols].ffill().fillna(0)

    # 4) 거래대금 ffill
    for vcol in ['KOSPI_volume', 'KOSDAQ_volume']:
        if vcol in df_final.columns:
            df_final[vcol] = df_final[vcol].ffill().fillna(0)

    # 5) 고객예탁금/신용잔고 빈칸
    for col in ['Customer_Deposit_value', 'Customer_Deposit_chg_pct',
                'Credit_Balance_value', 'Credit_Balance_chg_pct']:
        df_final[col] = ""

    # 6) date 컬럼 + 순서 정렬
    df_final['date'] = pd.to_datetime(df_final.index).strftime('%Y-%m-%d')
    df_final = df_final.reindex(columns=FINAL_COLUMNS)

    # 7) 잔여 NaN 0
    df_final = df_final.fillna(0)

    # target_date 행 추출
    target_str = target_date.strftime('%Y-%m-%d')
    target_row_df = df_final[df_final['date'] == target_str]
    if target_row_df.empty:
        sys.exit(f"[!] target_date={target_str} 행 생성 실패. 데이터 부족.")

    # numpy/pandas 타입을 Python native 로 변환 (gspread JSON 직렬화 호환)
    raw_row = target_row_df.iloc[0].tolist()
    return [_to_json_safe(v) for v in raw_row]


def _to_json_safe(v):
    """numpy.int64 / pandas.NA / numpy.float64 등을 Python native 타입으로 변환."""
    # pandas NA / numpy NaN 류는 빈 문자열로
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    # numpy scalar (int64, float64 등) → Python int/float
    if hasattr(v, 'item'):
        try:
            return v.item()
        except (ValueError, AttributeError):
            return v
    return v


def main():
    _load_dotenv()

    # 인자 파싱
    if len(sys.argv) >= 2:
        target_date_str = sys.argv[1]
    else:
        target_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"  Single-Day Market Data Fill — target={target_date_str}")
    print("=" * 60)

    # 데이터 생성
    row = build_row_for_date(target_date_str)

    # 미리보기
    print(f"\n생성된 행 ({len(row)} 컬럼) — 주요 값:")
    key_cols = ['date', 'KOSPI_price', 'KOSPI_chg_pct', 'KOSPI_volume',
                'KOSDAQ_price', 'NASDAQ_price', 'SP500_price', 'USDKRW_price',
                'US_10Y_Bond_rate', 'KR_10Y_Bond_rate', 'BTC_price', 'VIX_price']
    for k in key_cols:
        if k in FINAL_COLUMNS:
            i = FINAL_COLUMNS.index(k)
            print(f"  {k:25s} = {row[i]}")
    print("\n전체 행 (raw):")
    print(row)

    # 확인 prompt
    confirm = input("\n이 행을 market_data 시트에 append 할까? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("취소됨. 시트에 아무것도 쓰지 않았어.")
        return

    # 시트 업로드
    print(f"\n[4/4] '{WORKSHEET_NAME}' 시트에 append 중...")
    gc = get_gspread_client()
    sheet = gc.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    sheet.append_row(row, value_input_option='USER_ENTERED')
    print(f"  ✓ 추가 완료: date={row[0]}")
    print("\n" + "=" * 60)
    print("  ✓ 완료. Dashboard 사이드바의 '🔄 데이터 새로고침' 클릭하면 반영됨.")
    print("=" * 60)


if __name__ == "__main__":
    main()
