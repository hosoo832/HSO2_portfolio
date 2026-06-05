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
from datetime import datetime, timedelta, timezone

import gspread

import finance_core


SHEET_NAME = "거래내역"            # config.GOOGLE_SHEET_NAME 과 동일
WORKSHEET_NAME = "market_data"     # config.SHEET_MARKET_DATA 와 동일
ALERTS_SHEET = "ma_alerts"         # 별도 파일 (Cowork Drive 커넥터가 읽는 파일)
MARKET_TAB = "market"              # ma_alerts 안의 시장지표 미러 탭

# GitHub Actions runners 은 UTC 라서, 로그에 KST 시각 명시
KST = timezone(timedelta(hours=9))


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


def mirror_to_alerts(gc, header, market_row):
    """market_data 최신 행을 ma_alerts 파일 'market' 탭에 세로 형식으로 미러.

    06:30 장전 브리핑(Cowork)이 Drive 커넥터로 ma_alerts 를 읽어 A섹션을 작성.
    '거래내역' 파일은 너무 커서 커넥터로 통째 읽기 불가 → 별도 파일에 미러.
    세로 형식 ([지표, 값] 행 나열) 인 이유: 텍스트 export 시 39컬럼 가로 표보다 안정적.
    실패해도 cron 전체는 계속 진행 (브리핑 A섹션만 영향)."""
    try:
        sh = gc.open(ALERTS_SHEET)
        try:
            ws = sh.worksheet(MARKET_TAB)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=MARKET_TAB, rows=60, cols=3)
        values = [["지표", "값"]] + [
            [str(h), v] for h, v in zip(header, market_row)
        ]
        ws.clear()
        ws.update(values=values, range_name="A1", value_input_option="RAW")
        print(f"     → '{ALERTS_SHEET}/{MARKET_TAB}' 탭 미러 완료 ({len(values)-1}개 지표)")
    except Exception as e:
        print(f"     [!] ma_alerts 미러 실패 (cron 은 계속 진행): {e}")


def main():
    print("=" * 60)
    print(f"  Market Data Update — KST {datetime.now(KST).isoformat()}")
    print("=" * 60)

    # 1) 시장 데이터 수집
    print("\n[1/4] yfinance + pykrx 시장 데이터 수집 중... (~30초)")
    market_row = finance_core.fetch_daily_market_data()
    if not market_row:
        sys.exit("[!] 빈 데이터 반환됨 — market_data 갱신 스킵")
    print(f"     → {len(market_row)} 개 컬럼 (date={market_row[0]})")

    # 2) Google Sheets 인증
    print("\n[2/4] Google Sheets 인증...")
    gc = get_gspread_client()
    print("     → 인증 성공")

    # 3) 시트에 행 추가
    print(f"\n[3/4] '{WORKSHEET_NAME}' 시트에 행 추가 중...")
    try:
        sheet = gc.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        sheet.append_row(market_row, value_input_option='USER_ENTERED')
        print(f"     → 추가 완료: {market_row[0]}")
    except Exception as e:
        sys.exit(f"[!] 시트 쓰기 실패: {e}")

    # 4) ma_alerts 파일 'market' 탭에 미러 (06:30 브리핑 A섹션용)
    print(f"\n[4/4] '{ALERTS_SHEET}' 파일에 시장지표 미러 중...")
    try:
        header = sheet.row_values(1)
    except Exception as e:
        header = []
        print(f"     [!] market_data 헤더 읽기 실패: {e}")
    if header:
        mirror_to_alerts(gc, header, market_row)

    print("\n" + "=" * 60)
    print("  ✓ Market Data Update 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
