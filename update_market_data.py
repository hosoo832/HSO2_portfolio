"""
update_market_data.py — GitHub Actions cron 호출용 (KST 매일 07:00)

기능:
  1. finance_core.fetch_daily_market_data() 로 시장 지표 1행 생성
     (yfinance + pykrx — KOSPI/NASDAQ/USDKRW/금리/원자재/BTC/VIX/예탁금/신용잔고)
  2. 그 행을 구글시트 'market_data' 탭 끝에 append

환경변수 (GitHub Actions Secret 으로 주입):
  GCP_SA_JSON : service_account.json 파일 내용 전체 (JSON 문자열 그대로)

수동 실행:
  GitHub repo → Actions 탭 → "Daily Market Data Update" → "Run workflow"
"""
import json
import os
import sys
from datetime import datetime

import gspread

import finance_core


SHEET_NAME = "거래내역"            # config.GOOGLE_SHEET_NAME 과 동일
WORKSHEET_NAME = "market_data"     # config.SHEET_MARKET_DATA 와 동일


def get_gspread_client():
    """GCP_SA_JSON 환경변수에서 credentials 로드 후 gspread 클라이언트 생성."""
    sa_json = os.environ.get('GCP_SA_JSON')
    if not sa_json:
        sys.exit("[!] 환경변수 GCP_SA_JSON 이 비어있습니다. GitHub Secret 등록 필요.")

    try:
        creds_dict = json.loads(sa_json)
    except json.JSONDecodeError as e:
        sys.exit(f"[!] GCP_SA_JSON 파싱 실패 (JSON 형식 깨짐): {e}")

    return gspread.service_account_from_dict(creds_dict)


def main():
    print("=" * 60)
    print(f"  Market Data Update — {datetime.now().isoformat()}")
    print("=" * 60)

    # 1) 시장 데이터 수집
    print("\n[1/3] yfinance + pykrx 시장 데이터 수집 중... (~30초)")
    market_row = finance_core.fetch_daily_market_data()
    if not market_row:
        sys.exit("[!] 빈 데이터 반환됨 — market_data 갱신 스킵")
    print(f"     → {len(market_row)} 개 컬럼 (date={market_row[0]})")

    # 2) Google Sheets 인증
    print("\n[2/3] Google Sheets 인증...")
    gc = get_gspread_client()
    print("     → 인증 성공")

    # 3) 시트에 행 추가
    print(f"\n[3/3] '{WORKSHEET_NAME}' 시트에 행 추가 중...")
    try:
        sheet = gc.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        sheet.append_row(market_row, value_input_option='USER_ENTERED')
        print(f"     → 추가 완료: {market_row[0]}")
    except Exception as e:
        sys.exit(f"[!] 시트 쓰기 실패: {e}")

    print("\n" + "=" * 60)
    print("  ✓ Market Data Update 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
