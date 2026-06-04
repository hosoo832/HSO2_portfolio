"""
ma_touch.py — 보유 종목 이동평균선 터치 감지 (GitHub Actions daily cron 용)

기능:
  1. '거래내역' 스프레드시트의 dashboard_data 에서 보유 종목(quantity>0) 추출
     (yf_ticker 컬럼 재활용 — main.py 가 이미 심볼 매핑 완료)
  2. yfinance 일봉 1년치 → 이평선 5종 계산:
       일5 / 일10  : 일봉 종가 rolling 평균
       주5 / 주10  : 주봉(W-FRI) 종가 평균 (진행 중인 주 포함)
       월5         : 월봉(ME) 종가 평균 (진행 중인 달 포함)
  3. 터치 판정: 직전 거래일의 [저가 ~ 고가] 범위에 이평선 값 포함 여부
  4. 결과를 별도 스프레드시트 'ma_alerts' 1번 탭에 통째로 덮어씀
     (터치 종목이 위로 오게 정렬. 06:30 브리핑이 이 시트를 읽음)

한국 종목 보완: yfinance 데이터 부실 시 Naver fchart API 폴백 (일봉 OHLC).

환경변수 (GitHub Actions Secret):
  GCP_SA_JSON : service_account.json 내용 전체

사전 준비 (1회):
  - 구글시트 'ma_alerts' 생성 후 서비스계정 이메일에 편집자 공유
"""
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import gspread
import pandas as pd
import requests
import yfinance as yf

SOURCE_SHEET = "거래내역"
SOURCE_TAB = "dashboard_data"
ALERTS_SHEET = "ma_alerts"

KST = timezone(timedelta(hours=9))

# (이평선 라벨, 종류, 기간) — 종류: D=일봉, W=주봉, M=월봉
MA_SPECS = [
    ("일5", "D", 5),
    ("일10", "D", 10),
    ("주5", "W", 5),
    ("주10", "W", 10),
    ("월5", "M", 5),
]


# ----------------------------------------------------------------------
# 데이터 수집
# ----------------------------------------------------------------------

def get_gspread_client():
    sa_json = os.environ.get("GCP_SA_JSON")
    if not sa_json:
        sys.exit("[!] 환경변수 GCP_SA_JSON 이 비어있습니다.")
    try:
        creds = json.loads(sa_json)
    except json.JSONDecodeError as e:
        sys.exit(f"[!] GCP_SA_JSON 파싱 실패: {e}")
    return gspread.service_account_from_dict(creds)


def load_holdings(gc):
    """dashboard_data → 보유 종목 (ticker 단위 dedup). [(ticker, name, yf_symbol), ...]"""
    ws = gc.open(SOURCE_SHEET).worksheet(SOURCE_TAB)
    values = ws.get_all_values()
    if not values:
        sys.exit("[!] dashboard_data 가 비어있습니다.")
    df = pd.DataFrame(values[1:], columns=values[0], dtype=str)

    if "quantity" not in df.columns:
        sys.exit("[!] dashboard_data 에 quantity 컬럼이 없습니다.")
    df["quantity"] = pd.to_numeric(
        df["quantity"].str.replace(",", ""), errors="coerce"
    ).fillna(0)
    df = df[df["quantity"] > 0]

    seen, holdings = set(), []
    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).strip()
        yf_sym = str(row.get("yf_ticker", "")).strip()
        name = str(row.get("name", "")).strip() or ticker
        if not ticker or ticker in seen:
            continue
        if not yf_sym or yf_sym.startswith(("SKIP_", "MANUAL_")):
            continue  # 비상장/수동가격 종목은 이평선 계산 불가
        seen.add(ticker)
        holdings.append((ticker, name, yf_sym))
    return holdings


def fetch_naver_daily(code, count=300):
    """Naver fchart API 일봉 OHLC. 한국 종목 폴백용 (ETN 등 yfinance 미지원 포함)."""
    url = (
        f"https://fchart.stock.naver.com/sise.nhn?"
        f"symbol={code}&timeframe=day&count={count}&requestType=0"
    )
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        items = re.findall(r'<item data="([^"]+)"', r.text)
        rows = []
        for it in items:
            p = it.split("|")
            if len(p) < 5:
                continue
            rows.append(
                (pd.Timestamp(p[0]), float(p[1]), float(p[2]), float(p[3]), float(p[4]))
            )
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close"])
        return df.set_index("Date").sort_index()
    except Exception as e:
        print(f"     [Naver] {code} 폴백 실패: {e}")
        return None


def fetch_all_history(holdings):
    """yfinance 일괄 다운로드 + 한국 종목 Naver 폴백. {ticker: OHLC DataFrame}"""
    symbols = sorted({sym for _, _, sym in holdings if not sym.startswith("NAVER_")})
    yf_data = pd.DataFrame()
    if symbols:
        print(f"  [MA] yfinance 일봉 다운로드 ({len(symbols)}개, 1y)...")
        try:
            yf_data = yf.download(
                tickers=symbols, period="1y", interval="1d",
                group_by="ticker", progress=False, auto_adjust=False,
            )
        except Exception as e:
            print(f"  [!] yfinance 일괄 다운로드 실패: {e}")

    out = {}
    today = pd.Timestamp(datetime.now(KST).date())
    for ticker, name, sym in holdings:
        df = None
        if not sym.startswith("NAVER_") and not yf_data.empty:
            try:
                if len(symbols) == 1:
                    sub = yf_data
                else:
                    sub = yf_data[sym]
                sub = sub[["Open", "High", "Low", "Close"]].dropna(how="all").dropna(subset=["Close"])
                if len(sub) >= 10:
                    df = sub
            except Exception:
                df = None

        # 한국 종목인데 yfinance 부실 → Naver 폴백 (NAVER_ 종목은 바로 Naver)
        is_kr = sym.startswith("NAVER_") or sym.endswith((".KS", ".KQ"))
        stale = True
        if df is not None and len(df) >= 30:
            last_dt = df.index[-1]
            if last_dt.tz is not None:
                last_dt = last_dt.tz_localize(None)
            stale = (today - last_dt).days > 7
        if is_kr and (df is None or stale):
            code = sym.replace("NAVER_", "").replace(".KS", "").replace(".KQ", "")
            naver_df = fetch_naver_daily(code)
            if naver_df is not None and len(naver_df) >= 10:
                df = naver_df
                print(f"     [Naver] {name}({code}) 일봉 {len(df)}행 사용")

        if df is None or df.empty:
            print(f"     [!] {name}({sym}) 일봉 데이터 없음 — 스킵")
            continue
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        out[ticker] = df.sort_index()
    return out


# ----------------------------------------------------------------------
# 이평선 계산 + 터치 판정
# ----------------------------------------------------------------------

def compute_mas(df):
    """{라벨: MA값 or None}. 주/월봉은 진행 중인 캔들 포함 (차트 표시와 동일)."""
    close = df["Close"]
    weekly = close.resample("W-FRI").last().dropna()
    monthly = close.resample("ME").last().dropna()
    series_map = {"D": close, "W": weekly, "M": monthly}

    mas = {}
    for label, kind, n in MA_SPECS:
        s = series_map[kind]
        mas[label] = round(float(s.tail(n).mean()), 4) if len(s) >= n else None
    return mas


def judge_touches(df, mas):
    """직전 거래일 [저가, 고가] 에 이평선 포함 여부. (터치 목록, 거리% dict)"""
    last = df.iloc[-1]
    low, high, close = float(last["Low"]), float(last["High"]), float(last["Close"])
    touched, dist = [], {}
    for label, _, _ in MA_SPECS:
        ma = mas.get(label)
        if ma is None or ma <= 0:
            dist[label] = ""
            continue
        dist[label] = round((close - ma) / ma * 100, 2)  # 종가 기준 이격 %
        if low <= ma <= high:
            touched.append(label)
    return touched, dist, low, high, close


# ----------------------------------------------------------------------
# 시트 기록
# ----------------------------------------------------------------------

HEADER = [
    "기준일", "ticker", "종목명", "심볼", "캔들일자", "종가", "고가", "저가",
    "터치", "일5", "일5이격%", "일10", "일10이격%",
    "주5", "주5이격%", "주10", "주10이격%", "월5", "월5이격%",
]


def write_alerts(gc, rows):
    try:
        sh = gc.open(ALERTS_SHEET)
    except gspread.exceptions.SpreadsheetNotFound:
        sys.exit(
            f"[!] '{ALERTS_SHEET}' 스프레드시트를 찾을 수 없습니다. "
            "생성 후 서비스계정 이메일에 편집자 공유 필요."
        )
    ws = sh.sheet1
    ws.clear()
    ws.update(values=[HEADER] + rows, range_name="A1", value_input_option="RAW")
    print(f"  [MA] '{ALERTS_SHEET}' 기록 완료 ({len(rows)}행)")


# ----------------------------------------------------------------------

def main():
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    print("=" * 60)
    print(f"  MA Touch Alert — KST {datetime.now(KST).isoformat()}")
    print("=" * 60)

    gc = get_gspread_client()
    holdings = load_holdings(gc)
    print(f"  [MA] 보유 종목 {len(holdings)}개")

    history = fetch_all_history(holdings)

    rows = []
    for ticker, name, sym in holdings:
        df = history.get(ticker)
        if df is None:
            continue
        mas = compute_mas(df)
        touched, dist, low, high, close = judge_touches(df, mas)
        candle_date = df.index[-1].strftime("%Y-%m-%d")

        def _ma(label):
            v = mas.get(label)
            return "" if v is None else v

        rows.append([
            today_str, ticker, name, sym, candle_date, close, high, low,
            ",".join(touched),
            _ma("일5"), dist["일5"], _ma("일10"), dist["일10"],
            _ma("주5"), dist["주5"], _ma("주10"), dist["주10"],
            _ma("월5"), dist["월5"],
        ])

    # 터치 종목 먼저, 그 다음 이름순
    rows.sort(key=lambda r: (r[8] == "", r[2]))

    n_touch = sum(1 for r in rows if r[8])
    print(f"  [MA] 터치 {n_touch}건 / 전체 {len(rows)}종목")
    for r in rows:
        if r[8]:
            print(f"     · {r[2]} ({r[1]}): {r[8]}")

    write_alerts(gc, rows)
    print("\n  ✓ MA Touch Alert 완료")


if __name__ == "__main__":
    main()
