# --- [일회성 스크립트] market_data 과거 데이터 일괄 등록 ---
#
# (v70.30: 거래대금(억원) 백필 포함, 나머지는 빈칸 처리)
#
# [중요] 이 스크립트는 딱 한 번만 실행하세요.

import pandas as pd
import numpy as np
from pykrx import stock # [v70.30] 거래대금 수집을 위해 부활
import yfinance as yf
from datetime import datetime

# --- 1. 모듈 불러오기 ---
import config
import google_api

print("\n--- [BACKFILL] 과거 데이터 일괄 등록 스크립트 시작 (v70.30 - 거래대금 억원) ---")

# --- 2. 설정값 정의 ---
START_DATE = '2020-01-01'
END_DATE = datetime.now().strftime('%Y-%m-%d') 

# [v70.31] 39개 최종 열 순서 (NIKKEI 추가)
FINAL_COLUMNS_37 = [
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
    "Credit_Balance_value", "Credit_Balance_chg_pct"
]

def run_backfill():
    if not google_api.sheet_file:
        print("[!!!] 'google_api'가 구글 시트 파일 열기에 실패했습니다.")
        print("'service_account.json' 파일과 구글 시트 공유 설정을 확인하세요.")
        return 

    try:
        # --- 3. [yfinance] 글로벌 지표 일괄 다운로드 ---
        print(f"\n[BACKFILL] 1. yfinance 글로벌 지표 다운로드 중 ({START_DATE} ~ {END_DATE})...")
        yf_tickers = [
            '^KS11', '^KQ11', '^GSPC', '^IXIC', '000001.SS', '^N225', '^GDAXI',
            'USDKRW=X', 'DX-Y.NYB', '^TNX', '^TYX', 'CL=F', 'GC=F', 'BTC-USD', '^VIX'
        ]
        
        data_yf_raw = yf.download(tickers=yf_tickers, start=START_DATE, end=END_DATE, progress=False)
        
        if data_yf_raw.empty:
            print("  [!!!] yfinance 데이터 다운로드 실패. 스크립트를 중지합니다.")
            return

        # yfinance 가격 데이터 추출
        df_yf_price = data_yf_raw['Close']
        df_yf = pd.DataFrame(index=df_yf_price.index)

        df_yf['KOSPI_price'] = df_yf_price['^KS11']
        df_yf['KOSDAQ_price'] = df_yf_price['^KQ11']
        df_yf['SP500_price'] = df_yf_price['^GSPC']
        df_yf['NASDAQ_price'] = df_yf_price['^IXIC']
        df_yf['SHANGHAI_price'] = df_yf_price['000001.SS']
        df_yf['NIKKEI_price'] = df_yf_price['^N225']
        df_yf['DAX_price'] = df_yf_price['^GDAXI']
        df_yf['USDKRW_price'] = df_yf_price['USDKRW=X']
        df_yf['USD_IDX_price'] = df_yf_price['DX-Y.NYB']
        df_yf['US_10Y_Bond_rate'] = df_yf_price['^TNX']
        df_yf['US_30Y_Bond_rate'] = df_yf_price['^TYX']
        df_yf['WTI_price'] = df_yf_price['CL=F']
        df_yf['GOLD_price'] = df_yf_price['GC=F']
        df_yf['BTC_price'] = df_yf_price['BTC-USD']
        df_yf['VIX_price'] = df_yf_price['^VIX']
        
        print("     [yfinance] 글로벌 지표 가공 완료.")

        # --- 4. [v70.33] pykrx 비활성, ECOS API 로 대체 ---
        # pykrx 1.0.51 의 깨진 인덱스 OHLCV / 삭제된 deposit API 우회

        str_start = START_DATE.replace("-", "")
        str_end = END_DATE.replace("-", "")

        df_pykrx = pd.DataFrame()  # 비활성 (deprecated)

        # --- ECOS 데이터 수집 (KR 10Y 금리 + KOSPI/KOSDAQ 거래대금) ---
        print(f"\n[BACKFILL] 2-NEW. ECOS API 데이터 수집 중 ({START_DATE} ~ {END_DATE})...")
        df_ecos = pd.DataFrame()
        try:
            import os
            import ecos_helpers
            ecos_key = os.environ.get('ECOS_API_KEY')
            if not ecos_key:
                print("     [!!!] ECOS_API_KEY 환경변수 없음. ECOS 데이터 스킵.")
            else:
                df_ecos = ecos_helpers.fetch_all_history(str_start, str_end, api_key=ecos_key)
                if not df_ecos.empty:
                    print(f"     [ECOS] 통합 결과 shape={df_ecos.shape}")
                    # 정수 변환 — 거래대금은 억원 단위 정수
                    if 'KOSPI_volume' in df_ecos.columns:
                        df_ecos['KOSPI_volume'] = df_ecos['KOSPI_volume'].round(0).astype('Int64')
                    if 'KOSDAQ_volume' in df_ecos.columns:
                        df_ecos['KOSDAQ_volume'] = df_ecos['KOSDAQ_volume'].round(0).astype('Int64')
                    # KR 10Y chg_bps 사전 계산 (yfinance 와 동일한 방식)
                    if 'KR_10Y_Bond_rate' in df_ecos.columns:
                        df_ecos['KR_10Y_Bond_chg_bps'] = df_ecos['KR_10Y_Bond_rate'].diff() * 100
                else:
                    print("     [!] ECOS 빈 결과")
        except Exception as e:
            print(f"     [!!!] ECOS 수집 실패: {e}")
            import traceback; traceback.print_exc()

        # 고객예탁금/신용잔고는 ECOS/BOK 미제공 → 빈칸 유지
        df_deposit = pd.DataFrame()

        # --- 5. 데이터 최종 병합 및 후처리 ---
        print("\n[BACKFILL] 3. 데이터 최종 병합 및 후처리 중...")
        
        # 인덱스(날짜) 통일 (Timezone 제거)
        df_yf.index = pd.to_datetime(df_yf.index).date
        if not df_pykrx.empty:
            df_pykrx.index = pd.to_datetime(df_pykrx.index).date
        if not df_deposit.empty:
            df_deposit.index = pd.to_datetime(df_deposit.index).date
        if not df_ecos.empty:
            df_ecos.index = pd.to_datetime(df_ecos.index).date

        # 병합 (Outer Join으로 날짜 합집합)
        df_final = pd.concat([df_yf, df_pykrx, df_deposit, df_ecos], axis=1)
        
        # 1. 휴장일 처리 (가격): 직전일 값으로 채우기 (ffill)
        # (거래대금인 Volume은 ffill하지 않고 0으로 두는 게 일반적이나, 
        #  여기서는 일단 가격 데이터만 ffill 함)
        value_cols = [
            'KOSPI_price', 'KOSDAQ_price', 'SP500_price', 'NASDAQ_price',
            'SHANGHAI_price', 'NIKKEI_price', 'DAX_price', 'USDKRW_price', 'USD_IDX_price',
            'US_10Y_Bond_rate', 'US_30Y_Bond_rate', 'WTI_price', 'GOLD_price',
            'BTC_price', 'VIX_price',
            'Customer_Deposit_value', 'Credit_Balance_value'  # v70.31: 자금흐름도 ffill
        ]
        cols_to_ffill = [col for col in value_cols if col in df_final.columns]
        df_final[cols_to_ffill] = df_final[cols_to_ffill].ffill()

        # 2. 등락률 계산
        df_final['KOSPI_chg_pct'] = df_final['KOSPI_price'].pct_change()
        df_final['KOSDAQ_chg_pct'] = df_final['KOSDAQ_price'].pct_change()
        df_final['SP500_chg_pct'] = df_final['SP500_price'].pct_change()
        df_final['NASDAQ_chg_pct'] = df_final['NASDAQ_price'].pct_change()
        df_final['SHANGHAI_chg_pct'] = df_final['SHANGHAI_price'].pct_change()
        df_final['NIKKEI_chg_pct'] = df_final['NIKKEI_price'].pct_change()
        df_final['DAX_chg_pct'] = df_final['DAX_price'].pct_change()
        df_final['USDKRW_chg_pct'] = df_final['USDKRW_price'].pct_change()
        df_final['USD_IDX_chg_pct'] = df_final['USD_IDX_price'].pct_change()
        df_final['WTI_chg_pct'] = df_final['WTI_price'].pct_change()
        df_final['GOLD_chg_pct'] = df_final['GOLD_price'].pct_change()
        df_final['BTC_chg_pct'] = df_final['BTC_price'].pct_change()
        df_final['VIX_chg_pct'] = df_final['VIX_price'].pct_change()

        df_final['US_10Y_Bond_chg_bps'] = df_final['US_10Y_Bond_rate'].diff() * 100
        df_final['US_30Y_Bond_chg_bps'] = df_final['US_30Y_Bond_rate'].diff() * 100

        # v70.31: 자금흐름 등락률 계산
        if 'Customer_Deposit_value' in df_final.columns:
            df_final['Customer_Deposit_chg_pct'] = df_final['Customer_Deposit_value'].pct_change()
        if 'Credit_Balance_value' in df_final.columns:
            df_final['Credit_Balance_chg_pct'] = df_final['Credit_Balance_value'].pct_change()

        # 3. 등락률 NaN -> 0
        chg_cols = [col for col in df_final.columns if col.endswith(('_pct', '_bps'))]
        df_final[chg_cols] = df_final[chg_cols].fillna(0)

        # 4. date 컬럼 생성
        df_final['date'] = pd.to_datetime(df_final.index).strftime('%Y-%m-%d')

        # 5. [v70.33] ECOS/BOK 미제공 — 고객예탁금/신용잔고만 빈칸
        cols_to_empty = [
            "Customer_Deposit_value", "Customer_Deposit_chg_pct",
            "Credit_Balance_value", "Credit_Balance_chg_pct",
        ]
        for col in cols_to_empty:
            df_final[col] = ""

        # 6. 최종 열 순서 맞추기 & 빈 컬럼(Volume 등) 0 또는 "" 채우기
        df_final = df_final.reindex(columns=FINAL_COLUMNS_37)
        
        # Volume이 NaN인 경우(휴장일 등) 0으로 채움
        df_final['KOSPI_volume'] = df_final['KOSPI_volume'].fillna(0)
        df_final['KOSDAQ_volume'] = df_final['KOSDAQ_volume'].fillna(0)
        
        # 7. 정렬
        df_final = df_final.sort_values(by='date')
        
        # 8. 최종 NaN 0 처리 (앞부분 데이터 등)
        # 단, 위에서 빈칸("")으로 설정한 컬럼은 건드리지 않기 위해 numeric_only 안 씀
        # 이미 필요한 건 다 채웠으므로, 혹시 남은 NaN만 0으로
        # (빈 문자열이 있는 컬럼은 fillna(0) 영향 안 받음)
        df_final = df_final.fillna(0)

        print("     [BACKFILL] 모든 과거 데이터 가공 완료.")

        # --- 6. 구글 시트 업로드 ---
        print("\n[BACKFILL] 4. 'market_data' 시트에 일괄 업로드(덮어쓰기) 시작...")
        
        success = google_api.upload_to_google_sheet(
            df_final, 
            config.SHEET_MARKET_DATA
        )
        
        if success:
            print("\n--- [BACKFILL] 모든 과거 데이터가 'market_data'에 성공적으로 업로드되었습니다! ---")
        else:
            print("\n[!!!] 'market_data' 업로드 중 오류가 발생했습니다.")

    except Exception as e:
        print(f"\n[!!!] 백필 스크립트 실행 중 심각한 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_backfill()