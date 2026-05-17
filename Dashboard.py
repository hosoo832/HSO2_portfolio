# dashboard.py — 호섭님 포트폴리오 모바일 대시보드 (Phase 1)
#
# 실행:
#   1) pip install streamlit plotly gspread google-auth  (한 번만)
#   2) streamlit run dashboard.py
#
# 같은 폴더에 service_account.json 이 있어야 함 (이미 있으심)
# config.py 의 GOOGLE_SHEET_NAME ("거래내역") 을 자동으로 사용
#
# Phase 1 범위:
#   - 사이드바: 전체 / 멘토 포폴 / HS 포폴 뷰 전환
#   - 블록 1: Hero (총자산, 평가손익, 누적수익률, 5/14~ MWR)
#   - 블록 2: 비중 도넛 3개 (국가별 / 테마별 / 그룹별 또는 군종별)
#   - 블록 3: 단기 수익률 표 (1일 ~ YTD)
#
# 다음 Phase:
#   - 블록 4: BM 비교
#   - 블록 5: Gross / Long / Net
#   - 블록 6: 종목별 리밸런싱 표

import re
import time
from datetime import datetime, timezone, timedelta

# KST (UTC+9) — Streamlit Cloud 서버는 UTC 라 명시적 변환 필요
KST = timezone(timedelta(hours=9))

def now_kst():
    """현재 한국 시각."""
    return datetime.now(KST)

import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf

# ---------------------------------------------------------
# 설정
# ---------------------------------------------------------
SHEET_NAME = "거래내역"  # config.py 의 GOOGLE_SHEET_NAME 과 동일
CREDS_FILE = "service_account.json"

ACCOUNT_GROUPS = {
    '53648897': '멘토 포트폴리오',
    '60271589': '멘토 포트폴리오',
    '53649012': 'HS 포트폴리오',
    '856045053982': 'HS 포트폴리오',
    '220914426167': 'HS 포트폴리오',
    '717190227129': 'HS 포트폴리오',
}

# ACCOUNT_GROUPS 에서 자동 파생 (단일 진실의 원천 유지)
MENTOR_ACCS = [k for k, v in ACCOUNT_GROUPS.items() if v == '멘토 포트폴리오']
HS_ACCS = [k for k, v in ACCOUNT_GROUPS.items() if v == 'HS 포트폴리오']

# ---------------------------------------------------------
# 페이지 설정 (모바일 친화 layout)
# ---------------------------------------------------------
st.set_page_config(
    page_title="호섭 포트폴리오",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto",
)

# ---------------------------------------------------------
# 유틸
# ---------------------------------------------------------
def clean_account(val):
    return re.sub(r'[^0-9]', '', str(val))

def to_num(series):
    """₩, 쉼표, %, 공백 섞여있어도 숫자로 변환"""
    return pd.to_numeric(
        series.astype(str).str.replace(r'[^\d.-]', '', regex=True).replace('', '0'),
        errors='coerce'
    ).fillna(0)

def fmt_pct(v):
    """percent 값(예: 2.34)을 '+2.34%' 로 포맷"""
    if v is None:
        return "-"
    try:
        return f"{float(v):+.2f}%"
    except:
        return "-"

# ---------------------------------------------------------
# Google Sheets 연결 (캐시) — 로컬 + Streamlit Cloud 양쪽 지원
# ---------------------------------------------------------
@st.cache_resource
def get_gspread_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    # 1) Streamlit Cloud 환경 — Secrets 에 [gcp_service_account] 섹션 등록된 경우
    #    배포 후 Settings > Secrets 에 service_account.json 내용을 TOML 로 넣으면 작동
    try:
        if 'gcp_service_account' in st.secrets:
            return gspread.service_account_from_dict(
                dict(st.secrets['gcp_service_account']),
                scopes=scopes,
            )
    except Exception:
        pass  # st.secrets 미존재 (로컬 환경) → 다음으로

    # 2) 로컬 환경 — 같은 폴더의 service_account.json 사용
    return gspread.service_account(filename=CREDS_FILE, scopes=scopes)

@st.cache_data(ttl=600, show_spinner="구글 시트에서 데이터 로딩 중...")
def load_sheet(sheet_name, max_retries=3):
    """일시적 네트워크 끊김(ConnectionReset 등) 자동 재시도.
    헤더에 빈 문자열/중복이 있어도 안전하게 unique placeholder 로 변환.
    캐시 TTL = 10분 (Google Sheets API 호출 횟수 절감)."""
    gc = get_gspread_client()
    last_err = None
    for attempt in range(max_retries):
        try:
            ws = gc.open(SHEET_NAME).worksheet(sheet_name)
            all_values = ws.get_all_values()
            if not all_values:
                return pd.DataFrame()

            # 첫 행을 헤더로 사용. 빈 문자열 / 중복 헤더는 unique placeholder 로 변환
            raw_header = all_values[0]
            seen_count = {}
            clean_header = []
            for i, h in enumerate(raw_header):
                h = str(h).strip()
                if not h:
                    h = f"_empty_{i}"  # 빈 헤더 → 위치 기반 placeholder
                if h in seen_count:
                    seen_count[h] += 1
                    h = f"{h}_{seen_count[h]}"  # 중복 헤더 → 뒤에 번호 부여
                else:
                    seen_count[h] = 1
                clean_header.append(h)

            data = all_values[1:]
            # 모든 셀이 string 으로 들어옴 (이후 to_num/parse_pct_value 가 변환)
            return pd.DataFrame(data, columns=clean_header)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
    raise last_err

# ---------------------------------------------------------
# 실시간 가격 헬퍼 (Step C 장중 실시간 뷰용)
# - 한국 주식: 네이버 모바일 API (sub-second, 정확)
# - 해외 주식: yfinance batch (배치 호출, 15분 지연)
# - 캐시 2분 (API rate limit 보호)
# ---------------------------------------------------------
EXCHANGE_TO_CURRENCY = {
    'NASDAQ': 'USD', 'NYSE': 'USD', 'AMEX': 'USD',
    'HKG': 'HKD',
    'SSE': 'CNY', 'SZSE': 'CNY',
    'TSE': 'JPY',
}

@st.cache_data(ttl=60, show_spinner=False)
def get_naver_intraday(ticker):
    """한국 주식 장중 데이터 — 네이버 모바일 API.
    Returns dict {current, prev_close, currency='KRW'} or None."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code != 200:
            return None
        data = res.json()
        current = float(str(data.get('closePrice', '0')).replace(',', ''))
        change = float(str(data.get('compareToPreviousClosePrice', '0')).replace(',', ''))
        if current <= 0:
            return None
        prev = current - change
        return {'current': current, 'prev_close': prev, 'currency': 'KRW'}
    except Exception:
        return None

@st.cache_data(ttl=60, show_spinner=False)
def get_yf_batch(yf_tickers_tuple):
    """yfinance 배치 호출 (외국 주식). Returns dict {yf_ticker: {current, prev_close}}"""
    yf_tickers = list(yf_tickers_tuple)
    if not yf_tickers:
        return {}
    results = {}
    try:
        data = yf.download(yf_tickers, period='5d', interval='1d',
                           progress=False, auto_adjust=False)
        if data.empty:
            return {}

        # 평탄화: single ticker 도 최신 yfinance 는 MultiIndex 컬럼 반환
        is_multi = isinstance(data.columns, pd.MultiIndex)

        for t in yf_tickers:
            try:
                if is_multi and len(yf_tickers) > 1:
                    closes = data[('Close', t)].dropna()
                elif is_multi:
                    # Single ticker 인데 MultiIndex 인 경우 — Close 컬럼 추출
                    closes = data['Close'].iloc[:, 0].dropna()
                else:
                    closes = data['Close'].dropna()

                if len(closes) >= 2:
                    results[t] = {
                        'current': float(closes.iloc[-1]),
                        'prev_close': float(closes.iloc[-2]),
                    }
                elif len(closes) == 1:
                    results[t] = {
                        'current': float(closes.iloc[-1]),
                        'prev_close': float(closes.iloc[-1]),
                    }
            except Exception:
                continue
    except Exception:
        pass
    return results

@st.cache_data(ttl=120, show_spinner=False)
def get_fx_to_krw(currency_code):
    """1 단위 외화 → KRW 환율 (현재)."""
    if currency_code == 'KRW':
        return 1.0
    try:
        rate_data = yf.download(f"{currency_code}KRW=X", period='5d',
                                interval='1d', progress=False, auto_adjust=False)
        if rate_data.empty:
            return None
        # 최신 yfinance 는 single ticker 도 MultiIndex 로 반환 → 평탄화
        if isinstance(rate_data.columns, pd.MultiIndex):
            rate_data.columns = [c[0] for c in rate_data.columns]
        if 'Close' not in rate_data.columns:
            return None
        closes = rate_data['Close'].dropna()
        if len(closes) > 0:
            return float(closes.iloc[-1])
    except Exception:
        pass
    return None

def _build_yf_ticker(ticker, exchange):
    """ticker + exchange → yfinance ticker 변환."""
    ex = str(exchange).upper().strip()
    if ex in ('KOSPI', 'ETF', 'ETN'):
        return f"{ticker}.KS"
    elif ex == 'KOSDAQ':
        return f"{ticker}.KQ"
    elif ex == 'HKG':
        return f"{str(ticker).lstrip('0')}.HK"
    elif ex == 'SSE':
        return f"{ticker}.SS"
    elif ex == 'SZSE':
        return f"{ticker}.SZ"
    elif ex == 'TSE':
        return f"{ticker}.T"
    elif ex in ('NASDAQ', 'NYSE', 'AMEX'):
        return ticker
    # fallback: 6자리 숫자면 KS, 아니면 그대로
    if str(ticker).isdigit() and len(str(ticker)) == 6:
        return f"{ticker}.KS"
    return ticker

# ---------------------------------------------------------
# 사이드바
# ---------------------------------------------------------
with st.sidebar:
    st.title("📊 포트폴리오")

    view = st.radio(
        "뷰 선택",
        ["전체", "멘토 포폴", "HS 포폴", "💼 장중 실시간", "📓 작전 일지"],
        index=0,
    )

    st.markdown("---")

    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # ---- main.py 실행 버튼 (GitHub Actions 트리거) ----
    def _trigger_main_workflow():
        try:
            token = st.secrets.get("GITHUB_PAT")
        except Exception:
            token = None
        if not token:
            return False, "GITHUB_PAT 미등록 (Streamlit Cloud → App settings → Secrets 에 추가)"

        url = "https://api.github.com/repos/hosoo832/HSO2_portfolio/actions/workflows/run-main.yml/dispatches"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            r = requests.post(url, headers=headers, json={"ref": "main"}, timeout=10)
            if r.status_code == 204:
                return True, "실행 시작됨 — 1~2분 후 위 새로고침 클릭"
            return False, f"GitHub API 에러 {r.status_code}: {r.text[:150]}"
        except Exception as e:
            return False, f"네트워크 에러: {e}"

    if st.button("▶️ main.py 실행", use_container_width=True,
                 help="GitHub Actions 트리거. 1~2분 후 데이터 새로고침 누르세요."):
        with st.spinner("GitHub Actions 트리거 중..."):
            ok, msg = _trigger_main_workflow()
        if ok:
            st.success(f"🚀 {msg}")
            st.session_state['_main_triggered_at'] = now_kst()
        else:
            st.error(f"❌ {msg}")

    # 최근 트리거 시각 표시 (3분 안에)
    if '_main_triggered_at' in st.session_state:
        elapsed = (now_kst() - st.session_state['_main_triggered_at']).total_seconds()
        if elapsed < 180:
            st.caption(f"🚀 main.py 실행 중 (트리거 후 {int(elapsed)}초)")
        elif elapsed < 600:
            st.caption("✅ main.py 완료 예상 — 새로고침 권장")

    st.caption(f"마지막 로드: {now_kst().strftime('%H:%M:%S KST')}")
    st.caption("Phase 1 (Hero + 비중 + 단기)")

# ---------------------------------------------------------
# 데이터 로드
# ---------------------------------------------------------
try:
    df_dashboard = load_sheet("dashboard_data")
    df_perf = load_sheet("performance_summary")
    df_master = load_sheet("master_data")
    df_rebal = load_sheet("rebalancing_data")
    df_rebal_master = load_sheet("rebalancing_master")
    # 헤더의 줄바꿈/연속공백을 일반 공백으로 정규화 ("현금\n(목표)" → "현금 (목표)")
    if not df_rebal_master.empty:
        df_rebal_master.columns = [
            re.sub(r'\s+', ' ', str(c)).strip()
            for c in df_rebal_master.columns
        ]
    # master_data 의 'military' 컬럼을 dashboard_data 에 자동 merge
    # → 호섭님이 master_data 만 업데이트하면 main.py 안 건드려도 즉시 반영
    if (not df_master.empty and not df_dashboard.empty
            and 'military' in df_master.columns
            and 'ticker' in df_master.columns
            and 'ticker' in df_dashboard.columns):
        master_mil = (
            df_master[['ticker', 'military']]
            .drop_duplicates(subset=['ticker'])
        )
        if 'military' in df_dashboard.columns:
            df_dashboard = df_dashboard.drop(columns=['military'])
        df_dashboard = df_dashboard.merge(master_mil, on='ticker', how='left')
except Exception as e:
    st.error(f"⚠️ 시트 로딩 실패")
    st.code(str(e), language=None)
    st.info(
        "💡 일시적인 네트워크 / Google API 끊김일 가능성이 가장 높습니다.\n\n"
        "아래 버튼을 한 번 눌러보시거나, 브라우저 새로고침(F5)을 시도해주세요."
    )
    if st.button("🔄 다시 시도", type="primary"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

# 숫자 컬럼 정규화
for col in ['market_value_krw', 'unrealized_pl_krw', 'realized_pl_krw',
            'cumulative_pl_krw', 'total_cost_krw', 'net_invested_capital',
            'quantity', 'current_price_krw', 'avg_cost_krw']:
    if col in df_dashboard.columns:
        df_dashboard[col] = to_num(df_dashboard[col])

# 그룹 매핑
df_dashboard['account_clean'] = df_dashboard['account'].apply(clean_account)
df_dashboard['group_name'] = df_dashboard['account_clean'].map(ACCOUNT_GROUPS).fillna('기타')

# =========================================================
# [작전 일지 뷰] — 매일 시장 진단 + 매매 기록 + 회고
# - 좌측: 시장 주요 지표 (자동), 경제 지표/이슈 (수동)
# - 우측: 매매 내역, 전투 일지, 전투 계획 (수동)
# - 저장: journal_log 시트 (date 키로 upsert)
# =========================================================
if view == "📓 작전 일지":
    from datetime import date as _date

    st.title("📓 작전 일지")
    st.caption("매일 시장 진단 + 매매 기록 + 회고. 같은 날짜로 다시 저장하면 덮어쓰기.")

    # ---- 날짜 선택 ----
    col_d1, col_d2 = st.columns([2, 3])
    with col_d1:
        sel_date = st.date_input("📅 일지 날짜", value=_date.today(), key="journal_date")

    # ---- journal_log 시트 헬퍼 ----
    JOURNAL_HEADERS = ['date', 'updated_at', '시장요약', '경제지표', '시장이슈', '매매내역', '전투일지', '전투계획']

    def _normalize_date_str(s):
        """Sheets locale 변환된 날짜를 YYYY-MM-DD 로 정규화.
        '2026. 5. 8.' / '2026. 5. 8 (수)' / '2026-05-08' 모두 → '2026-05-08'
        """
        s = str(s).strip()
        if not s:
            return s
        s = re.sub(r'\s*\([^)]*\)\s*', '', s)
        s = re.sub(r'[\.\s/]+', '-', s).strip('-')
        try:
            return pd.to_datetime(s, errors='coerce').strftime('%Y-%m-%d')
        except Exception:
            return s

    @st.cache_data(ttl=30, show_spinner=False)
    def _load_journal_raw():
        gc = get_gspread_client()
        try:
            ws = gc.open(SHEET_NAME).worksheet("journal_log")
        except Exception:
            return None
        vals = ws.get_all_values()
        if not vals:
            return None
        df = pd.DataFrame(vals[1:], columns=vals[0])
        # date 컬럼 locale 정규화 (Sheets 가 '2026. 5. 8.' 로 저장한 경우 대응)
        if 'date' in df.columns:
            df['date'] = df['date'].apply(_normalize_date_str)
        return df

    def _ensure_journal_sheet():
        """journal_log 시트 없으면 생성 + 헤더 입력. 있으면 worksheet 반환.
        가독성 위한 포맷팅(wrap text, 컬럼 폭) 자동 적용."""
        gc = get_gspread_client()
        wb = gc.open(SHEET_NAME)
        try:
            ws = wb.worksheet("journal_log")
        except Exception:
            ws = wb.add_worksheet(title="journal_log", rows="500", cols="12")
            ws.append_row(JOURNAL_HEADERS, value_input_option='USER_ENTERED')

        # 헤더 누락된 컬럼 보정 (구버전 → 시장요약 컬럼 신규 추가)
        existing = ws.row_values(1)
        if existing != JOURNAL_HEADERS:
            try:
                ws.update(range_name=f"A1:{chr(ord('A') + len(JOURNAL_HEADERS) - 1)}1",
                          values=[JOURNAL_HEADERS], value_input_option='USER_ENTERED')
            except Exception:
                pass

        # 가독성 포맷팅 (한 번만 적용해도 영구)
        try:
            sid = ws.id
            requests = [
                # 모든 텍스트 컬럼: wrap text + top-align
                {"repeatCell": {
                    "range": {"sheetId": sid, "startRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 8},
                    "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)"
                }},
                # 헤더 행: bold + 배경
                {"repeatCell": {
                    "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.93, "green": 0.93, "blue": 0.95},
                        "horizontalAlignment": "CENTER",
                    }},
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }},
                # 컬럼 폭 — A:date 90, B:updated_at 130, C:시장요약 250, 나머지 텍스트 350
                *[{"updateDimensionProperties": {
                    "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }} for i, w in enumerate([95, 145, 280, 280, 380, 380, 380, 380])],
                # 데이터 행 높이: 자동(=내용에 맞춰 늘어남)
                {"updateDimensionProperties": {
                    "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 1, "endIndex": 500},
                    "properties": {"pixelSize": 80},  # 기본 높이 80px (wrap 시 자동 확장)
                    "fields": "pixelSize"
                }},
                # 헤더 freeze (스크롤해도 헤더 보임)
                {"updateSheetProperties": {
                    "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount"
                }},
            ]
            wb.batch_update({"requests": requests})
        except Exception:
            pass  # 포맷팅 실패해도 저장은 계속

        return ws

    def _save_journal(date_str, fields):
        """날짜 기준으로 upsert. fields = dict (경제지표, 시장이슈, ...)
        - value_input_option='RAW' 사용: Sheets 의 자동 date 파싱 회피 (locale 변환 X)
        - 기존 행 매칭 시 normalize 비교 (옛 데이터 호환)
        """
        ws = _ensure_journal_sheet()
        all_vals = ws.get_all_values()
        if not all_vals:
            ws.append_row(JOURNAL_HEADERS, value_input_option='RAW')
            all_vals = [JOURNAL_HEADERS]

        headers = all_vals[0]
        date_col_idx = headers.index('date') if 'date' in headers else 0

        # 기존 행 찾기 — locale 변환된 날짜도 정규화해서 비교
        # 중복 있으면 가장 최근 (마지막) 행을 update 대상으로
        target_row_idx = None
        for i, row in enumerate(all_vals[1:], start=2):
            if len(row) > date_col_idx:
                if _normalize_date_str(row[date_col_idx]) == date_str:
                    target_row_idx = i  # break 안 함 → 마지막 매치 가져감

        new_row = [
            date_str,
            now_kst().strftime('%Y-%m-%d %H:%M:%S'),
            fields.get('시장요약', ''),
            fields.get('경제지표', ''),
            fields.get('시장이슈', ''),
            fields.get('매매내역', ''),
            fields.get('전투일지', ''),
            fields.get('전투계획', ''),
        ]

        if target_row_idx:
            ws.update(range_name=f"A{target_row_idx}:H{target_row_idx}",
                      values=[new_row], value_input_option='RAW')
            return 'updated'
        else:
            ws.append_row(new_row, value_input_option='RAW')
            return 'created'

    # 기존 일지 로드 (있으면)
    df_journal = _load_journal_raw()
    sel_date_str = sel_date.strftime('%Y-%m-%d')
    existing = {}
    if df_journal is not None and not df_journal.empty:
        match = df_journal[df_journal['date'] == sel_date_str]
        if not match.empty:
            r = match.iloc[-1].to_dict()
            existing = {k: r.get(k, '') for k in JOURNAL_HEADERS}
            st.info(f"📖 {sel_date_str} 일지가 이미 있어요. 마지막 수정: {existing.get('updated_at', '?')}. 수정하고 다시 저장 가능.")

    # ============================================================
    # 섹션 A — 시장 주요 지표 (자동)
    # ============================================================
    st.markdown("### 📊 A. 시장 주요 지표")

    # market_data 시트에서 sel_date 또는 가장 가까운 과거 거래일 데이터 가져옴
    try:
        df_md = load_sheet("market_data")
        if not df_md.empty:
            date_col = df_md.columns[0]
            _date_raw = df_md[date_col].astype(str).str.strip()
            _date_norm = (_date_raw
                          .str.replace(r'\s*\([^)]*\)\s*', '', regex=True)
                          .str.replace(r'[\.\s/]+', '-', regex=True)
                          .str.strip('-'))
            df_md['_date'] = pd.to_datetime(_date_norm, errors='coerce')
            df_md = df_md.dropna(subset=['_date']).sort_values('_date')

            # 숫자 컬럼 변환
            for col in df_md.columns:
                if col not in (date_col, '_date'):
                    df_md[col] = pd.to_numeric(
                        df_md[col].astype(str).str.replace(r'[^\d.\-]', '', regex=True),
                        errors='coerce'
                    )

            sel_ts = pd.Timestamp(sel_date)
            match_md = df_md[df_md['_date'] <= sel_ts]
            if match_md.empty:
                st.warning(f"market_data 에 {sel_date_str} 이전 데이터 없음")
                md_row = None
            else:
                md_row = match_md.iloc[-1]
                actual_date = md_row['_date'].strftime('%Y-%m-%d')
                if actual_date != sel_date_str:
                    st.caption(f"⚠️ {sel_date_str} 시장 데이터 없음 → 직전 거래일 ({actual_date}) 데이터 표시")
        else:
            md_row = None
    except Exception as e:
        st.error(f"market_data 로드 실패: {e}")
        md_row = None

    market_summary_lines = []  # journal_log 의 시장요약 컬럼에 저장

    if md_row is not None:
        def _fmt_v(v):
            if pd.isna(v): return '-'
            if abs(v) >= 10000: return f"{v:,.0f}"
            return f"{v:,.2f}"

        def _fmt_p(v):
            if pd.isna(v): return None
            return f"{v:+.2f}%"

        def _row_idx(col):
            return md_row.get(col) if col in md_row.index else None

        def _big_card(label, value, chg=None, suffix=''):
            """큰 폰트 + 테두리 카드 (라벨/가격/변동률 모두 큼직하게)."""
            if chg is None:
                chg_html = ''
            else:
                color = '#2e7d32' if not chg.startswith('-') else '#c62828'
                arrow = '▲' if not chg.startswith('-') else '▼'
                chg_html = (
                    f"<span style='color:{color}; font-size:1.4rem; font-weight:700; "
                    f"margin-left:14px; vertical-align:middle; white-space:nowrap'>"
                    f"{arrow} {chg.lstrip('+-')}</span>"
                )
            return (
                f"<div style='"
                f"border:1px solid #e0e3e7; border-radius:10px; "
                f"padding:14px 18px; background:#ffffff; "
                f"margin-bottom:12px; box-shadow:0 1px 2px rgba(0,0,0,0.04); "
                f"line-height:1.25;'>"
                f"<div style='font-size:1.5rem; color:#444; font-weight:500; margin-bottom:6px'>{label}</div>"
                f"<div style='font-size:2.2rem; font-weight:700; color:#111'>{value}{suffix}{chg_html}</div>"
                f"</div>"
            )

        def _record_summary(label, value, chg):
            """시장요약 텍스트 누적 (journal_log 저장용)."""
            line = f"{label}: {value}"
            if chg:
                line += f" ({chg})"
            market_summary_lines.append(line)

        def _render(cols, items):
            """items = [(label, price_col, chg_col, suffix?), ...]"""
            for col_st, item in zip(cols, items):
                label = item[0]
                value_raw = _row_idx(item[1]) if item[1] else None
                chg_raw = _row_idx(item[2]) if len(item) > 2 and item[2] else None
                suffix = item[3] if len(item) > 3 else ''
                value_str = _fmt_v(value_raw) if value_raw is not None else '-'
                chg_str = _fmt_p(chg_raw)
                col_st.markdown(_big_card(label, value_str, chg_str, suffix), unsafe_allow_html=True)
                _record_summary(label, value_str + suffix, chg_str)

        def _section_header(text):
            st.markdown(
                f"<div style='margin: 32px 0 16px 0; font-size: 1.75rem; "
                f"font-weight: 700; color: #222; line-height: 1.3;'>{text}</div>",
                unsafe_allow_html=True
            )

        # ─ 국내
        _section_header("🇰🇷 국내")
        _render(st.columns(3), [
            ('KOSPI', 'KOSPI_price', 'KOSPI_chg_pct'),
            ('KOSDAQ', 'KOSDAQ_price', 'KOSDAQ_chg_pct'),
            ('KR 10Y 채권', 'KR_10Y_Bond_rate', None, '%'),
        ])
        _render(st.columns(3), [
            ('KOSPI 거래대금', 'KOSPI_volume', None, ' 억'),
            ('KOSDAQ 거래대금', 'KOSDAQ_volume', None, ' 억'),
            ('USD/KRW', 'USDKRW_price', 'USDKRW_chg_pct'),
        ])

        # ─ 해외
        _section_header("🌐 해외")
        _render(st.columns(3), [
            ('S&P 500', 'SP500_price', 'SP500_chg_pct'),
            ('NASDAQ', 'NASDAQ_price', 'NASDAQ_chg_pct'),
            ('Nikkei', 'NIKKEI_price', 'NIKKEI_chg_pct'),
        ])
        _render(st.columns(3), [
            ('Shanghai', 'SHANGHAI_price', 'SHANGHAI_chg_pct'),
            ('DAX', 'DAX_price', 'DAX_chg_pct'),
            ('USD Index', 'USD_IDX_price', 'USD_IDX_chg_pct'),
        ])

        # ─ 채권/원자재/변동성/크립토
        _section_header("📈 채권 / 원자재 / 변동성 / 크립토")
        _render(st.columns(3), [
            ('US 10Y 채권', 'US_10Y_Bond_rate', None, '%'),
            ('US 30Y 채권', 'US_30Y_Bond_rate', None, '%'),
            ('VIX', 'VIX_price', 'VIX_chg_pct'),
        ])
        _render(st.columns(3), [
            ('WTI', 'WTI_price', 'WTI_chg_pct'),
            ('Gold', 'GOLD_price', 'GOLD_chg_pct'),
            ('BTC', 'BTC_price', 'BTC_chg_pct'),
        ])

        # ─ 추세 비교 차트 (이전 시장 동향 뷰에서 이전) ─
        if not df_md.empty and '_date' in df_md.columns:
            st.divider()
            _section_header("📈 추세 비교")

            # 차트용 라벨 → 컬럼 매핑
            CHART_INDICES = {
                'KOSPI': 'KOSPI_price',
                'KOSDAQ': 'KOSDAQ_price',
                'S&P 500': 'SP500_price',
                'NASDAQ': 'NASDAQ_price',
                'Shanghai': 'SHANGHAI_price',
                'Nikkei': 'NIKKEI_price',
                'DAX': 'DAX_price',
                'USD/KRW': 'USDKRW_price',
                'USD Index': 'USD_IDX_price',
                'VIX': 'VIX_price',
                'US 10Y': 'US_10Y_Bond_rate',
                'US 30Y': 'US_30Y_Bond_rate',
                'KR 10Y': 'KR_10Y_Bond_rate',
                'WTI': 'WTI_price',
                'GOLD': 'GOLD_price',
                'BTC': 'BTC_price',
            }
            latest_chart_date = df_md['_date'].max()

            cc1, cc2, cc3 = st.columns([2, 1, 1])
            with cc1:
                opts = [k for k, v in CHART_INDICES.items() if v in df_md.columns]
                default_picks = [i for i in ['KOSPI', 'NASDAQ', 'USD/KRW'] if i in opts]
                sel_indices = st.multiselect(
                    "지표 선택 (여러 개 비교 가능)",
                    options=opts, default=default_picks,
                    key='journal_chart_indices',
                )
            with cc2:
                t_range = st.selectbox(
                    "기간",
                    ["WTD", "MTD", "QTD", "YTD", "1주", "1달", "3달", "6달", "1년", "전체"],
                    index=6,  # 기본 3달
                    key='journal_chart_range',
                )
            with cc3:
                normalize = st.toggle(
                    "정규화", value=True,
                    help="여러 지표를 같은 스케일로 비교 (시작=100)",
                    key='journal_chart_norm',
                )

            # 시간 범위 적용
            if t_range == "전체":
                df_chart = df_md.copy()
            elif t_range == "WTD":
                cutoff = latest_chart_date - pd.Timedelta(days=latest_chart_date.weekday())
                df_chart = df_md[df_md['_date'] >= cutoff]
            elif t_range == "MTD":
                cutoff = pd.Timestamp(year=latest_chart_date.year,
                                      month=latest_chart_date.month, day=1)
                df_chart = df_md[df_md['_date'] >= cutoff]
            elif t_range == "QTD":
                qm = ((latest_chart_date.month - 1) // 3) * 3 + 1
                cutoff = pd.Timestamp(year=latest_chart_date.year, month=qm, day=1)
                df_chart = df_md[df_md['_date'] >= cutoff]
            elif t_range == "YTD":
                cutoff = pd.Timestamp(year=latest_chart_date.year, month=1, day=1)
                df_chart = df_md[df_md['_date'] >= cutoff]
            else:
                days_map = {"1주": 7, "1달": 30, "3달": 90, "6달": 180, "1년": 365}
                cutoff = latest_chart_date - pd.Timedelta(days=days_map[t_range])
                df_chart = df_md[df_md['_date'] >= cutoff]

            if df_chart.empty or not sel_indices:
                st.info("기간 내 데이터 없음 또는 지표 미선택")
            else:
                if len(df_chart) < 5:
                    st.info(
                        f"📌 '{t_range}' 거래일 데이터 {len(df_chart)}개 — "
                        "월/분기/년 초반엔 흔한 현상. 더 긴 기간 옵션 사용 추천."
                    )
                fig_trend = go.Figure()
                for label in sel_indices:
                    col = CHART_INDICES.get(label)
                    if not col or col not in df_chart.columns:
                        continue
                    series = df_chart[col].copy()
                    if normalize:
                        first_valid = series.dropna()
                        if not first_valid.empty and first_valid.iloc[0] != 0:
                            series = series / first_valid.iloc[0] * 100
                    fig_trend.add_trace(go.Scatter(
                        x=df_chart['_date'], y=series, name=label, mode='lines',
                        line=dict(width=2),
                        hovertemplate=f'<b>{label}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:,.2f}}<extra></extra>',
                    ))
                fig_trend.update_layout(
                    title=dict(
                        text=f"{t_range} 추세" + (" (정규화: 시작=100)" if normalize else " (raw)"),
                        font=dict(size=15),
                    ),
                    height=460, margin=dict(l=40, r=20, t=60, b=40),
                    font=dict(size=13, family='sans-serif'),
                    yaxis=dict(
                        title=dict(text="지수 (시작=100)" if normalize else "값", font=dict(size=13)),
                        tickfont=dict(size=11), gridcolor='#eeeeee',
                    ),
                    xaxis=dict(tickfont=dict(size=11), tickformat='%Y-%m-%d', type='date'),
                    legend=dict(orientation='h', yanchor='top', y=1.10,
                                xanchor='center', x=0.5, font=dict(size=12)),
                    plot_bgcolor='white', hovermode='x unified',
                )
                st.plotly_chart(fig_trend, use_container_width=True)
                if normalize:
                    st.caption("💡 **정규화**: 각 지표의 기간 시작값 = 100 으로 맞춤 (스케일 다른 지표 비교 가능)")
                else:
                    st.caption("💡 **raw**: 실제 값 그대로 — 단위 비슷한 지표끼리 비교에 적합")

    st.divider()

    # ============================================================
    # 폼 바깥의 컨트롤 (즉시 반응) — raw 매매내역 reload 버튼
    # ============================================================
    btn_col, _ = st.columns([1, 3])
    with btn_col:
        if st.button(
            "📥 raw 시트에서 매매 다시 불러오기",
            help="이 날짜의 raw_domestic + raw_international 매매를 가져와 D 섹션에 채움 (현재 입력 덮어씀)",
            use_container_width=True,
        ):
            st.session_state['_journal_reload_raw'] = True
            # data_editor 의 session state 강제 초기화 (옛 입력값이 새 데이터를 덮어쓰는 것 방지)
            _editor_key = f"trades_editor_{sel_date_str}"
            if _editor_key in st.session_state:
                del st.session_state[_editor_key]
            st.rerun()

    st.caption(
        "💡 D 매매내역 표: 셀에 입력 후 **Enter** 또는 **Tab** 으로 commit 한 다음 저장 버튼 클릭."
    )

    # ============================================================
    # 섹션 B-F — 폼 (수동 입력)
    # ============================================================
    # ⚠️ 일반 container 사용 — form 의 widget 입력 batching 문제 회피.
    # 각 widget 의 변경이 즉시 session_state 에 반영됨 → data_editor 입력 손실 없음.
    with st.container():
        st.markdown("### 📝 B. 경제 지표")
        econ = st.text_area(
            "경제 지표 (CPI, 실업수당, 금리 결정 등 — 그날 발표된 것만)",
            value=existing.get('경제지표', ''),
            height=120,
            placeholder="예시:\n미국 - CPI 실적 2.4% / 예상 2.3% / 이전 2.5%\n한국 - 금리 결정 실적 3.25% / 예상 3.25% / 이전 3.5%\n→ 4년만의 인하, 시장 영향 크지 않을듯",
            label_visibility="collapsed",
            key=f'journal_econ_{sel_date_str}',
        )

        st.markdown("### 🌪️ C. 시장 이슈와 해석")
        issue = st.text_area(
            "시장 이슈와 해석",
            value=existing.get('시장이슈', ''),
            height=180,
            placeholder="예시:\n중국 인민은행 경제 부양책 발표\n1. 통화완화 - 기준금리 50bp 인하\n2. 부동산 정책 - 모기지 금리 50bp 인하\n→ 유동성 공급 이후 추가 부양책 고민 필요",
            label_visibility="collapsed",
            key=f'journal_issue_{sel_date_str}',
        )

        st.markdown("### 💸 D. 매매 내역과 이유")

        def _account_to_group(acc):
            acc = clean_account(acc)
            if acc in MENTOR_ACCS: return '멘토'
            if acc in HS_ACCS: return 'HS'
            return ''

        # 그룹 AUM 사전 계산 (정산금액 / 그룹AUM × 100 = 비중%)
        _group_aum = {
            '멘토': float(df_dashboard[df_dashboard['account_clean'].isin(MENTOR_ACCS)]
                          ['market_value_krw'].apply(pd.to_numeric, errors='coerce').sum()),
            'HS':   float(df_dashboard[df_dashboard['account_clean'].isin(HS_ACCS)]
                          ['market_value_krw'].apply(pd.to_numeric, errors='coerce').sum()),
        }

        # raw 시트에서 해당 날짜 매매 자동 import 헬퍼
        def _fetch_trades_for_date(date_str):
            """raw_domestic + raw_international 에서 매매 거래만 추출."""
            rows = []
            # 국내
            try:
                df_dom = load_sheet("raw_domestic")
                if not df_dom.empty and '거래일자' in df_dom.columns:
                    df_dom = df_dom.copy()
                    df_dom['_date_iso'] = pd.to_datetime(
                        df_dom['거래일자'], errors='coerce'
                    ).dt.strftime('%Y-%m-%d')
                    today_dom = df_dom[df_dom['_date_iso'] == date_str]
                    for _, r in today_dom.iterrows():
                        search = str(r.get('거래종류', '')) + ' ' + str(r.get('적요명', ''))
                        if not ('보통매매' in search or '재투자' in search):
                            continue
                        action = '매도' if '매도' in search else '매수'
                        acc = clean_account(r.get('계좌번호', ''))
                        grp = _account_to_group(acc)
                        name = str(r.get('종목명', '')).strip()
                        qty = str(r.get('거래수량', '')).strip().replace(',', '')
                        amt = str(r.get('정산금액', '')).strip().replace(',', '')
                        try:
                            qf, af = float(qty), float(amt)
                            price = af / qf if qf > 0 else 0
                            price_str = f"₩{int(price):,}" if price > 0 else amt
                            settle_str = f"₩{int(af):,}" if af > 0 else amt
                            # 그룹 비중 (KRW 거래 → 그대로 비교)
                            grp_aum = _group_aum.get(grp, 0)
                            ratio_str = f"{(af / grp_aum * 100):.2f}%" if grp_aum > 0 and af > 0 else ''
                        except Exception:
                            price_str = ''; settle_str = ''; ratio_str = ''
                        rows.append({
                            '계좌': acc, '그룹': grp, '매매': action, '종목명': name,
                            '가격': price_str, '정산금액': settle_str,
                            '그룹비중': ratio_str, '이유': '',
                        })
            except Exception:
                pass

            # 해외
            try:
                df_intl = load_sheet("raw_international")
                if not df_intl.empty and '거래일자' in df_intl.columns:
                    df_intl = df_intl.copy()
                    df_intl['_date_iso'] = pd.to_datetime(
                        df_intl['거래일자'], errors='coerce'
                    ).dt.strftime('%Y-%m-%d')
                    today_intl = df_intl[df_intl['_date_iso'] == date_str]
                    for _, r in today_intl.iterrows():
                        memo = str(r.get('적요명', '')).strip()
                        if memo not in ('매수', '매도'):
                            continue
                        action = memo
                        acc = clean_account(r.get('계좌번호', ''))
                        grp = _account_to_group(acc)
                        name = str(r.get('종목명', '')).strip()
                        qty = str(r.get('거래수량', '')).strip().replace(',', '')
                        amt = str(r.get('정산금액(외)', '')).strip().replace(',', '')
                        ccy = str(r.get('통화', '')).strip()
                        try:
                            qf, af = float(qty), float(amt)
                            price = af / qf if qf > 0 else 0
                            price_str = f"{price:.2f} {ccy}" if price > 0 else amt
                            settle_str = f"{af:,.2f} {ccy}" if af > 0 else amt
                        except Exception:
                            price_str = ''; settle_str = ''
                        # 외화 거래는 그룹비중 자동 계산 안 함 (FX 환산 별도 필요) — 빈칸
                        rows.append({
                            '계좌': acc, '그룹': grp, '매매': action, '종목명': name,
                            '가격': price_str, '정산금액': settle_str,
                            '그룹비중': '', '이유': '',
                        })
            except Exception:
                pass
            return rows

        # 기존 매매 내역 파싱
        existing_trades = existing.get('매매내역', '').strip()
        # session_state 의 reload 신호 (form 바깥의 reload 버튼이 설정)
        should_reload_raw = st.session_state.pop('_journal_reload_raw', False)

        empty_default_rows = [
            {'계좌': '', '그룹': '', '매매': '', '종목명': '',
             '가격': '', '정산금액': '', '그룹비중': '', '이유': ''}
        ] * 3

        # ⚡ trades_init 을 session_state 에 캐시 — 매 rerun 마다 새 DataFrame 생성을 막아
        # data_editor 가 user edits 를 reset 하는 문제 해결
        _init_key = f'_trades_init_data_{sel_date_str}'

        if should_reload_raw:
            with st.spinner("raw 시트 로딩 중... (3000+ 행 처리 ~5초)"):
                auto_trades = _fetch_trades_for_date(sel_date_str)
            if auto_trades:
                st.session_state[_init_key] = pd.DataFrame(auto_trades)
                st.success(f"📥 raw 시트에서 {len(auto_trades)}건 매매 불러옴")
            else:
                st.session_state[_init_key] = pd.DataFrame(empty_default_rows)
                st.info(f"raw 시트에 {sel_date_str} 매매 없음")
        elif _init_key not in st.session_state:
            # 처음 — 저장된 내용 파싱 또는 빈 기본값
            if existing_trades:
                rows = []
                for line in existing_trades.split('\n'):
                    parts = line.split('|')
                    if len(parts) >= 8:
                        rows.append({
                            '계좌': parts[0], '그룹': parts[1], '매매': parts[2],
                            '종목명': parts[3], '가격': parts[4],
                            '정산금액': parts[5], '그룹비중': parts[6], '이유': parts[7],
                        })
                    elif len(parts) == 6:
                        rows.append({
                            '계좌': parts[0], '그룹': parts[1], '매매': parts[2],
                            '종목명': parts[3], '가격': parts[4],
                            '정산금액': '', '그룹비중': '', '이유': parts[5],
                        })
                    else:
                        while len(parts) < 5:
                            parts.append('')
                        rows.append({
                            '계좌': '', '그룹': '',
                            '매매': parts[0], '종목명': parts[1], '가격': parts[2],
                            '정산금액': '', '그룹비중': '', '이유': parts[4],
                        })
                st.session_state[_init_key] = pd.DataFrame(rows)
            else:
                st.session_state[_init_key] = pd.DataFrame(empty_default_rows)
                st.caption(
                    "💡 이 날짜의 raw 매매내역 자동 채우려면 위의 "
                    "**📥 raw 시트에서 매매 다시 불러오기** 버튼 클릭."
                )

        trades_init = st.session_state[_init_key]

        trades_edited = st.data_editor(
            trades_init,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                '계좌': st.column_config.TextColumn(width="small"),
                '그룹': st.column_config.SelectboxColumn(
                    options=['', '멘토', 'HS'], width="small"),
                '매매': st.column_config.SelectboxColumn(
                    options=['', '매수', '매도', '관망'], width="small"),
                '종목명': st.column_config.TextColumn(width="medium"),
                '가격': st.column_config.TextColumn(help="단가 (1주당)", width="small"),
                '정산금액': st.column_config.TextColumn(help="총 거래금액", width="small"),
                '그룹비중': st.column_config.TextColumn(
                    help="정산금액 / 그룹 AUM × 100 (국내 자동, 외화 빈칸)", width="small"),
                '이유': st.column_config.TextColumn(width="large"),
            },
            key=f"trades_editor_{sel_date_str}",
        )

        st.markdown("### ⚔️ E. 오늘의 전투 일지")
        log = st.text_area(
            "오늘의 전투 일지",
            value=existing.get('전투일지', ''),
            height=180,
            placeholder="시장 흐름, 내 판단, 잘한 것/실수한 것, 깨달은 것 등 자유 회고",
            label_visibility="collapsed",
            key=f'journal_log_{sel_date_str}',
        )

        st.markdown("### 🎯 F. 내일의 전투 계획")
        plan = st.text_area(
            "내일의 전투 계획",
            value=existing.get('전투계획', ''),
            height=120,
            placeholder="원칙 / 마음가짐 / 내일 체크할 종목·이벤트 등",
            label_visibility="collapsed",
            key=f'journal_plan_{sel_date_str}',
        )

        submitted = st.button(
            "💾 저장",
            type="primary",
            use_container_width=True,
            key=f'journal_save_btn_{sel_date_str}',
        )

        if submitted:
            # 매매 내역 → 8필드 직렬화: 계좌|그룹|매매|종목명|가격|정산금액|그룹비중|이유
            trades_lines = []
            for _, r in trades_edited.iterrows():
                acc = str(r.get('계좌', '')).strip()
                grp = str(r.get('그룹', '')).strip()
                action = str(r.get('매매', '')).strip()
                name = str(r.get('종목명', '')).strip()
                price = str(r.get('가격', '')).strip()
                settle = str(r.get('정산금액', '')).strip()
                ratio = str(r.get('그룹비중', '')).strip()
                reason = str(r.get('이유', '')).strip()
                if not any([acc, grp, action, name, price, settle, ratio, reason]):
                    continue
                trades_lines.append(f"{acc}|{grp}|{action}|{name}|{price}|{settle}|{ratio}|{reason}")
            trades_text = '\n'.join(trades_lines)

            try:
                result = _save_journal(sel_date_str, {
                    '시장요약': '\n'.join(market_summary_lines),
                    '경제지표': econ,
                    '시장이슈': issue,
                    '매매내역': trades_text,
                    '전투일지': log,
                    '전투계획': plan,
                })
                st.cache_data.clear()

                # === 저장 검증: 시트에서 다시 읽어 매매내역 컬럼 실제 반영 확인 ===
                verify_msg = ""
                try:
                    _gc = get_gspread_client()
                    _ws = _gc.open(SHEET_NAME).worksheet("journal_log")
                    _all = _ws.get_all_values()
                    _hdr = _all[0] if _all else []
                    _trade_idx = _hdr.index('매매내역') if '매매내역' in _hdr else -1
                    found = False
                    for _row in _all[1:]:
                        if _row and _normalize_date_str(_row[0]) == sel_date_str:
                            found = True
                            actual = _row[_trade_idx] if _trade_idx >= 0 and len(_row) > _trade_idx else ''
                            if actual.strip() == trades_text.strip():
                                verify_msg = f"검증 ✓ 시트의 매매내역 길이 {len(actual)}자"
                            else:
                                verify_msg = (
                                    f"⚠️ 검증 mismatch — 보낸 길이 {len(trades_text)}자 / "
                                    f"시트 실제 {len(actual)}자"
                                )
                            break
                    if not found:
                        verify_msg = "⚠️ 검증: 저장 후 해당 날짜 행 못찾음"
                except Exception as ve:
                    verify_msg = f"검증 중 오류: {ve}"

                # 저장 성공 → trades_init 캐시 무효화 (다음 렌더에서 시트 새 데이터로 재로드)
                # + data_editor 의 edit 상태 클리어 (이미 시트에 반영됐으니)
                if _init_key in st.session_state:
                    del st.session_state[_init_key]
                _editor_key_clear = f"trades_editor_{sel_date_str}"
                if _editor_key_clear in st.session_state:
                    del st.session_state[_editor_key_clear]

                if result == 'updated':
                    st.success(f"✅ {sel_date_str} 일지 갱신 (매매 {len(trades_lines)}건)")
                else:
                    st.success(f"✅ {sel_date_str} 일지 신규 저장 (매매 {len(trades_lines)}건)")

                with st.expander("🔍 저장 진단 (펼쳐 확인)", expanded=("⚠️" in verify_msg)):
                    st.caption(verify_msg)
                    st.text("저장 직전 trades_text 미리보기:")
                    st.code(trades_text[:500] if trades_text else "(빈 문자열)", language="text")
                    st.text("trades_edited DataFrame:")
                    st.dataframe(trades_edited, hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"❌ 저장 실패: {e}")
                import traceback
                st.code(traceback.format_exc())

    st.stop()  # 작전 일지 뷰는 여기서 종료

# =========================================================
# [장중 실시간 뷰] — 별도 흐름, st.stop() 으로 종료
# 한국 주식: 네이버 모바일 API
# 외국 주식: yfinance batch + 현재 환율 → KRW 환산
# =========================================================
if view == "💼 장중 실시간":
    st.title("💼 장중 실시간 수익률")
    st.caption(
        f"갱신 시각: {now_kst().strftime('%H:%M:%S KST')} · "
        "최신 가격 보려면 페이지 새로고침 또는 사이드바 *🔄 데이터 새로고침* 클릭"
    )

    # 그룹/계좌 선택 — 그룹 우선, 개별 계좌도 가능
    accounts_avail = sorted([a for a in df_dashboard['account_clean'].unique() if a])
    if not accounts_avail:
        st.warning("계좌 데이터가 없습니다.")
        st.stop()

    # 그룹 옵션 (먼저 보여줌) + 개별 계좌 옵션 (구분선)
    GROUP_OPTIONS = {
        "🌐 전체 (멘토 + HS)": [a for a in accounts_avail if a in (MENTOR_ACCS + HS_ACCS)],
        "📈 멘토 포폴 (전체)": [a for a in accounts_avail if a in MENTOR_ACCS],
        "🇰🇷 HS 포폴 (전체)": [a for a in accounts_avail if a in HS_ACCS],
    }
    options = list(GROUP_OPTIONS.keys()) + ["─── 개별 계좌 ───"] + accounts_avail

    default_label = "🇰🇷 HS 포폴 (전체)"
    default_idx = options.index(default_label) if default_label in options else 0

    selected = st.selectbox(
        "그룹 / 계좌 선택",
        options,
        index=default_idx,
        help="그룹을 선택하면 그 그룹의 모든 계좌의 보유 종목을 한번에 표시.",
    )

    # 선택된 항목 → 계좌 리스트
    if selected == "─── 개별 계좌 ───":
        st.info("위에서 개별 계좌를 선택하세요.")
        st.stop()
    elif selected in GROUP_OPTIONS:
        accounts_in_scope = GROUP_OPTIONS[selected]
    else:
        accounts_in_scope = [selected]

    if not accounts_in_scope:
        st.warning(f"'{selected}' 에 해당하는 계좌가 없습니다.")
        st.stop()

    # 해당 계좌들의 보유 종목 (현금 제외, 수량 > 0)
    sub_rt = df_dashboard[df_dashboard['account_clean'].isin(accounts_in_scope)].copy()
    sub_rt = sub_rt[~sub_rt['ticker'].astype(str).str.startswith('CASH')]
    sub_rt = sub_rt[sub_rt['quantity'].abs() > 0]

    if sub_rt.empty:
        st.info("선택한 그룹/계좌에 보유 종목이 없습니다.")
        st.stop()

    # 한국 / 외국 분리
    # 한국 시장 판단: 6자 길이 + 영문/숫자 혼합 가능 + 숫자 1개 이상 + 거래소 한국
    # (예: 091180 = 일반 주식, 0035T0 = 영문 혼합 ETF 모두 한국)
    def _is_kr_ticker(ticker_s, exchange_s):
        s = str(ticker_s).strip()
        if len(s) != 6 or not s.isalnum() or not any(c.isdigit() for c in s):
            return False
        if s.isdigit():
            return True  # 6자리 숫자 → 일반 한국주식
        # 영문 혼합 → 거래소 힌트로 검증
        ex = str(exchange_s).upper().strip()
        return ex in ('KOSPI', 'KOSDAQ', 'ETF', 'ETN', '')

    # ticker → {theme, postion, country} 룩업 (dashboard_data 기반)
    attr_lookup = {}
    for _, r in sub_rt.iterrows():
        tk = str(r['ticker']).strip()
        attr_lookup[tk] = {
            'theme': str(r.get('theme', '') or '').strip(),
            'postion': str(r.get('postion', '') or '').strip(),
            'country': str(r.get('country', '') or '').strip(),
        }

    kr_rows = []      # [(ticker, name, qty, avg_cost, account), ...]
    foreign_rows = [] # [(ticker, name, qty, avg_cost, exchange, yf_ticker, account), ...]
    for _, r in sub_rt.iterrows():
        tk = str(r['ticker']).strip()
        nm = str(r.get('name', tk))
        qty = float(r['quantity'])
        avg_cost = float(r.get('avg_cost_krw', 0) or 0)
        ex = str(r.get('exchange', '')).upper()
        acc = str(r.get('account_clean', '') or r.get('account', '')).strip()

        if _is_kr_ticker(tk, ex):
            kr_rows.append((tk, nm, qty, avg_cost, acc))
        else:
            yf_t = _build_yf_ticker(tk, ex)
            foreign_rows.append((tk, nm, qty, avg_cost, ex, yf_t, acc))

    # 실시간 가격 조회
    with st.spinner(f"실시간 가격 조회 중... (한국 {len(kr_rows)}개 / 외국 {len(foreign_rows)}개)"):
        # 한국: 네이버 (sequential) — kr_rows: (ticker, name, qty, avg_cost, acc)
        kr_prices = {}
        for tk, _, _, _, _ in kr_rows:
            if tk in kr_prices:
                continue  # 같은 ticker 여러 계좌 보유 — 한 번만 fetch
            r = get_naver_intraday(tk)
            if r:
                kr_prices[tk] = r

        # 외국: yfinance batch — foreign_rows: (ticker, name, qty, avg_cost, exchange, yf_ticker, acc)
        foreign_yf_tuple = tuple(sorted(set(
            row[5] for row in foreign_rows  # yf_ticker is index 5
        )))
        foreign_yf_data = get_yf_batch(foreign_yf_tuple) if foreign_yf_tuple else {}

        # 환율 (필요한 통화만)
        currencies = set()
        for row in foreign_rows:
            ex = row[4]  # exchange is index 4
            currencies.add(EXCHANGE_TO_CURRENCY.get(ex, 'USD'))
        fx_rates = {c: (get_fx_to_krw(c) or 1.0) for c in currencies}

    # 결과 행 생성
    result_rows = []

    def _qty_fmt(q):
        return int(q) if q == int(q) else round(q, 2)

    def _build_row(tk, nm, qty, avg_cost, acc, prev_orig, curr_orig, fx):
        prev_krw = prev_orig * fx
        curr_krw = curr_orig * fx
        change_pct = ((curr_orig - prev_orig) / prev_orig * 100) if prev_orig > 0 else 0
        today_pl = (curr_krw - prev_krw) * qty
        cost_total = avg_cost * qty
        current_value = curr_krw * qty
        cumulative_pl = current_value - cost_total
        attrs = attr_lookup.get(tk, {})
        # 컬럼 순서: 계좌 → 종목명/테마/포지션/국가 → 매입/평가/누적 → 오늘 변동
        return {
            '계좌': acc,
            '종목명': nm,
            '테마': attrs.get('theme', ''),
            '포지션': attrs.get('postion', ''),
            '국가': attrs.get('country', ''),
            '수량': _qty_fmt(qty),
            '매입가': avg_cost,
            '매입금액': cost_total,
            '현재 평가액': current_value,
            '누적 손익': cumulative_pl,
            '전일종가': prev_krw,
            '현재가': curr_krw,
            '변동률': change_pct,
            '오늘 손익': today_pl,
            '_ok': True,
            '_ticker': tk,  # 실패 시 reference 용
        }

    def _build_failed_row(tk, nm, qty, avg_cost, acc):
        cost_total = avg_cost * qty
        attrs = attr_lookup.get(tk, {})
        return {
            '계좌': acc,
            '종목명': nm,
            '테마': attrs.get('theme', ''),
            '포지션': attrs.get('postion', ''),
            '국가': attrs.get('country', ''),
            '수량': _qty_fmt(qty),
            '매입가': avg_cost, '매입금액': cost_total,
            '현재 평가액': 0, '누적 손익': 0,
            '전일종가': 0, '현재가': 0,
            '변동률': 0, '오늘 손익': 0,
            '_ok': False,
            '_ticker': tk,
        }

    for tk, nm, qty, avg_cost, acc in kr_rows:
        r = kr_prices.get(tk)
        if r and r.get('prev_close', 0) > 0:
            result_rows.append(_build_row(tk, nm, qty, avg_cost, acc, r['prev_close'], r['current'], 1.0))
        else:
            result_rows.append(_build_failed_row(tk, nm, qty, avg_cost, acc))

    for tk, nm, qty, avg_cost, ex, yf_t, acc in foreign_rows:
        r = foreign_yf_data.get(yf_t)
        if r and r.get('prev_close', 0) > 0:
            ccy = EXCHANGE_TO_CURRENCY.get(ex, 'USD')
            fx = fx_rates.get(ccy, 1.0)
            result_rows.append(_build_row(tk, nm, qty, avg_cost, acc, r['prev_close'], r['current'], fx))
        else:
            result_rows.append(_build_failed_row(tk, nm, qty, avg_cost, acc))

    # 계좌 우선 정렬 (같은 계좌끼리 시각적으로 묶임), 그 안에서 변동률 desc
    df_rt = pd.DataFrame(result_rows).sort_values(
        ['계좌', '변동률'], ascending=[True, False]
    ).reset_index(drop=True)

    # 요약 메트릭 (4개)
    ok_rows = df_rt[df_rt['_ok']]
    total_today_pl = ok_rows['오늘 손익'].sum()
    total_value = ok_rows['현재 평가액'].sum()
    total_cost = ok_rows['매입금액'].sum()
    total_cumulative_pl = ok_rows['누적 손익'].sum()
    total_prev = (ok_rows['전일종가'] * ok_rows['수량']).sum()
    total_today_pct = (total_today_pl / total_prev * 100) if total_prev > 0 else 0
    total_cum_pct = (total_cumulative_pl / total_cost * 100) if total_cost > 0 else 0

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("오늘 손익", f"₩{total_today_pl:+,.0f}",
              delta=f"{total_today_pct:+.2f}%", delta_color="off")
    s2.metric("현재 평가액", f"₩{total_value:,.0f}")
    s3.metric("누적 매입금액", f"₩{total_cost:,.0f}")
    s4.metric("누적 손익", f"₩{total_cumulative_pl:+,.0f}",
              delta=f"{total_cum_pct:+.2f}%", delta_color="off")

    # 색상 코딩
    def _color_change_rt(v):
        try:
            vv = float(v)
            if vv > 0: return 'color: #2e7d32; font-weight: 600'
            if vv < 0: return 'color: #c62828; font-weight: 600'
        except: pass
        return ''

    # 계좌별 행 배경색 (그룹 뷰에서 계좌 시각적 구분)
    ACCOUNT_COLORS = {
        '60271589':     '#fff3e0',  # 멘토 — 옅은 주황
        '53648897':     '#fffde7',  # 멘토 — 옅은 노랑
        '53649012':     '#e3f2fd',  # HS 일반 — 옅은 파랑
        '856045053982': '#e8f5e9',  # HS 일반 — 옅은 녹
        '220914426167': '#f3e5f5',  # HS 퇴직연금 — 옅은 보라
        '717190227129': '#fce4ec',  # HS 퇴직연금 — 옅은 분홍
    }

    def _color_account_row(row):
        acc = str(row.get('계좌', ''))
        bg = ACCOUNT_COLORS.get(acc, '')
        return [f'background-color: {bg}' if bg else ''] * len(row)

    display_rt = df_rt.drop(columns=['_ok', '_ticker'])
    styled_rt = (
        display_rt.style
        .format({
            '매입가': '₩{:,.0f}',
            '매입금액': '₩{:,.0f}',
            '전일종가': '₩{:,.0f}',
            '현재가': '₩{:,.0f}',
            '변동률': '{:+.2f}%',
            '오늘 손익': '₩{:+,.0f}',
            '현재 평가액': '₩{:,.0f}',
            '누적 손익': '₩{:+,.0f}',
        })
        .apply(_color_account_row, axis=1)  # 행 전체 배경 (계좌 색)
        .map(_color_change_rt, subset=['변동률', '오늘 손익', '누적 손익'])
    )

    st.dataframe(styled_rt, use_container_width=True, hide_index=True,
                 height=min(700, 60 + 38 * len(display_rt)))

    # 색상 범례 (그룹/전체 뷰일 때만 보여주면 의미 있음)
    if len(accounts_in_scope) > 1:
        legend_parts = []
        for acc in accounts_in_scope:
            color = ACCOUNT_COLORS.get(acc, '#f5f5f5')
            legend_parts.append(
                f"<span style='background:{color}; padding:2px 8px; border-radius:4px; margin-right:6px'>{acc}</span>"
            )
        st.markdown(
            "**📌 계좌 색상**: " + ' '.join(legend_parts),
            unsafe_allow_html=True,
        )

    failed = df_rt[~df_rt['_ok']]
    if not failed.empty:
        st.warning(
            f"⚠️ 가격 조회 실패 {len(failed)}개: "
            + ', '.join(failed['_ticker'].astype(str).head(10).tolist())
            + ('...' if len(failed) > 10 else '')
        )

    st.caption(
        "💡 **한국 주식**: 네이버 모바일 API (장 시간 중 실시간, 장 마감 후 종가). "
        "**외국 주식**: yfinance (~15분 지연). "
        "외화 → KRW 환율은 현재 환율 기준 단순 환산 (전일 환율 변동 효과 미반영). "
        "캐시 2분 (사이드바 *데이터 새로고침* 또는 페이지 새로고침으로 즉시 갱신)."
    )

    st.stop()  # 실시간 뷰는 여기서 종료, 일반 뷰 렌더링 스킵

# ---------------------------------------------------------
# 뷰별 필터
# ---------------------------------------------------------
def _pick_military_col(df):
    """master_data 의 military 컬럼이 의미 있게 채워져 있으면 그걸,
    아니면 fallback 으로 postion (오타 그대로) 사용."""
    if 'military' in df.columns:
        non_empty = df['military'].astype(str).str.strip()
        if non_empty.replace('', pd.NA).notna().sum() > 0:
            return 'military'
    return 'postion'

if view == "전체":
    df_view = df_dashboard.copy()
    perf_label = "Total Portfolio"
    third_pie_col = 'group_name'
    third_pie_title = '그룹별'
elif view == "멘토 포폴":
    df_view = df_dashboard[df_dashboard['group_name'] == '멘토 포트폴리오'].copy()
    perf_label = "멘토 포트폴리오"
    third_pie_col = _pick_military_col(df_view)
    third_pie_title = '군종별'
else:
    df_view = df_dashboard[df_dashboard['group_name'] == 'HS 포트폴리오'].copy()
    perf_label = "HS 포트폴리오"
    third_pie_col = _pick_military_col(df_view)
    third_pie_title = '군종별'

# performance_summary 에서 해당 뷰 행
perf_row = None
if not df_perf.empty and '상세' in df_perf.columns:
    matched = df_perf[df_perf['상세'].astype(str).str.strip() == perf_label]
    if not matched.empty:
        perf_row = matched.iloc[0]

def get_perf_raw(col_name):
    """performance_summary 셀의 raw 숫자값 (포맷 무시). 통화 컬럼용."""
    if perf_row is None or col_name not in perf_row.index:
        return None
    raw = perf_row[col_name]
    s = str(raw).strip()
    if s == '' or s == '-':
        return None
    cleaned = re.sub(r'[^\d.\-]', '', s)
    if cleaned in ('', '-', '.'):
        return None
    try:
        return float(cleaned)
    except:
        return None

def parse_pct_value(raw):
    """임의의 값을 '퍼센트 단위 숫자' 로 변환.
    원본에 '%' 있으면 이미 퍼센트값으로 인식 (예: '0.7%' → 0.7)
    원본에 '%' 없으면 소수점 형태로 보고 ×100 (예: '0.0234' → 2.34)
    """
    s = str(raw).strip()
    if s == '' or s == '-':
        return None
    has_pct = '%' in s
    cleaned = re.sub(r'[^\d.\-]', '', s)
    if cleaned in ('', '-', '.'):
        return None
    try:
        v = float(cleaned)
        if not has_pct:
            v *= 100  # 소수 형태로 저장된 경우만 ×100
        return v
    except:
        return None

def get_perf_pct(col_name):
    """현재 perf_row(전체/멘토/HS) 의 셀을 퍼센트 단위로 가져옴."""
    if perf_row is None or col_name not in perf_row.index:
        return None
    return parse_pct_value(perf_row[col_name])

def get_perf_raw(col_name):
    """현재 perf_row 에서 raw 숫자 추출 (₩ 통화 컬럼 / 손익 컬럼용)."""
    if perf_row is None or col_name not in perf_row.index:
        return None
    raw = perf_row[col_name]
    s = str(raw).strip()
    if s == '' or s == '-':
        return None
    cleaned = re.sub(r'[^\d.\-]', '', s)
    if cleaned in ('', '-', '.'):
        return None
    try:
        return float(cleaned)
    except:
        return None

def get_bm_pct(bm_label, col_name):
    """벤치마크 행에서 셀을 퍼센트 단위로 가져옴."""
    if df_perf.empty or '구분' not in df_perf.columns:
        return None
    bm_rows = df_perf[
        (df_perf['구분'].astype(str).str.strip() == '벤치마크') &
        (df_perf['상세'].astype(str).str.strip() == bm_label)
    ]
    if bm_rows.empty or col_name not in bm_rows.columns:
        return None
    return parse_pct_value(bm_rows.iloc[0][col_name])

# 호환용 alias (디버그 패널이 쓰는 이름)
def get_perf(col_name):
    return get_perf_pct(col_name)

# ---------------------------------------------------------
# 헤더
# ---------------------------------------------------------
st.title(f"📊 {view}")

# ---------------------------------------------------------
# [블록 1] Hero 영역
# ---------------------------------------------------------
total_assets = df_view['market_value_krw'].sum()

# 평가손익(누적): 비현금 종목들의 unrealized + realized 합
mask_non_cash = df_view.get('asset_class', pd.Series(['']*len(df_view))) != '현금'
total_unrealized = df_view.loc[mask_non_cash, 'unrealized_pl_krw'].sum() if 'unrealized_pl_krw' in df_view.columns else 0
total_realized = df_view.loc[mask_non_cash, 'realized_pl_krw'].sum() if 'realized_pl_krw' in df_view.columns else 0
total_pl = total_unrealized + total_realized

cum_ret_pct = get_perf_pct('누적수익률(%)')
daily_ret_pct = get_perf_pct('1일')

# 뷰별 inception 날짜 (MWR Hero metric 의 기준일)
# - 멘토: 5/14 (멘토 그룹 운용 본격 시작)
# - HS:   7/21 (HS 그룹 분리 운용 시작)
# - 전체: 5/14 (CUSTOM_START_DATE 기본값)
HERO_INCEPTION_BY_VIEW = {
    '전체':       '2025-05-14',
    '멘토 포폴':  '2025-05-14',
    'HS 포폴':    '2025-07-21',
}
inception_date = HERO_INCEPTION_BY_VIEW.get(view, '2025-05-14')
inception_short = inception_date[2:]  # 'YY-MM-DD' (예: '25-07-21')

# performance_summary 컬럼명 매칭 (performance.py 가 만든 형식과 동일)
custom_twr_col = f'지정({inception_short}~)'
custom_mwr_col = f'MWR_지정({inception_short}~)'

custom_twr_pct = get_perf_pct(custom_twr_col)
custom_mwr_pct = get_perf_pct(custom_mwr_col)

# Hero metric 라벨용: '25/7/21~ MWR' 형식 (연도 포함)
_yy = inception_date[2:4]
_mm = str(int(inception_date[5:7]))
_dd = str(int(inception_date[8:10]))
mwr_metric_label = f"{_yy}/{_mm}/{_dd}~ MWR"

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(
        "총자산",
        f"₩{total_assets:,.0f}",
        delta=f"{daily_ret_pct:+.2f}% (1일)" if daily_ret_pct is not None else None,
    )
with c2:
    st.metric("평가손익 (누적)", f"₩{total_pl:,.0f}")
with c3:
    st.metric(
        "누적 수익률 (TWR)",
        fmt_pct(cum_ret_pct),
    )
with c4:
    st.metric(
        mwr_metric_label,
        fmt_pct(custom_mwr_pct),
        delta=f"TWR {fmt_pct(custom_twr_pct)}" if custom_twr_pct is not None else None,
        delta_color="off",
    )

# ---------------------------------------------------------
# [Hero 보조] 계좌별 자산 분포 (전체 / 멘토 / HS 뷰)
# ---------------------------------------------------------
if view in ("전체", "멘토 포폴", "HS 포폴"):
    if view == "전체":
        target_accs_for_breakdown = MENTOR_ACCS + HS_ACCS
    elif view == "멘토 포폴":
        target_accs_for_breakdown = MENTOR_ACCS
    else:
        target_accs_for_breakdown = HS_ACCS

    def _classify_region(row):
        """국내 vs 해외 분류 — '진짜 해외 계좌 잔고' 의 의미.
        - 국내 = KRW 로 보유 (한국 ETF, 한국주식, KRW 예수금 — 한국 ETF 가 미국 추종해도 국내로 분류)
        - 해외 = 외화로 보유 (직접 매수한 외국주, 외화 예수금)
        """
        ticker = str(row.get('ticker', ''))
        if ticker.startswith('CASH_KRW'):
            return '국내'
        if ticker.startswith('CASH_FX'):
            return '해외'
        currency = str(row.get('currency', '')).strip().upper()
        if currency == 'KRW':
            return '국내'
        if currency in ('', 'NAN', 'NONE'):
            return '미분류'
        return '해외'

    def _account_group(acc):
        """계좌 → 그룹명."""
        if acc in MENTOR_ACCS:
            return '멘토'
        if acc in HS_ACCS:
            return 'HS'
        return '기타'

    breakdown_rows = []
    for acc in target_accs_for_breakdown:
        sub = df_view[df_view['account_clean'] == acc].copy()
        if sub.empty:
            continue
        sub['_region'] = sub.apply(_classify_region, axis=1)
        domestic = sub[sub['_region'] == '국내']['market_value_krw'].sum()
        foreign = sub[sub['_region'] == '해외']['market_value_krw'].sum()
        unclassified = sub[sub['_region'] == '미분류']['market_value_krw'].sum()
        total = sub['market_value_krw'].sum()
        breakdown_rows.append({
            '계좌': acc,
            '그룹': _account_group(acc),
            '국내 (KRW)': domestic,
            '해외 (외화→KRW)': foreign,
            '미분류': unclassified,
            '합계': total,
        })

    if breakdown_rows:
        df_breakdown = pd.DataFrame(breakdown_rows)
        # 비중 % 계산 (합계 행 포함 전 — 분모는 모든 계좌 합)
        grand_total = df_breakdown['합계'].sum()
        df_breakdown['비중'] = (
            df_breakdown['합계'] / grand_total * 100 if grand_total > 0 else 0.0
        )

        # 합계 행 추가
        total_row = pd.DataFrame([{
            '계좌': '**합계**',
            '그룹': '',
            '국내 (KRW)': df_breakdown['국내 (KRW)'].sum(),
            '해외 (외화→KRW)': df_breakdown['해외 (외화→KRW)'].sum(),
            '미분류': df_breakdown['미분류'].sum(),
            '합계': df_breakdown['합계'].sum(),
            '비중': 100.0,
        }])
        df_breakdown = pd.concat([df_breakdown, total_row], ignore_index=True)

        # 미분류가 0이면 컬럼 숨김
        base_cols = ['계좌', '그룹', '국내 (KRW)', '해외 (외화→KRW)', '합계', '비중']
        show_cols = base_cols if df_breakdown['미분류'].sum() == 0 else (
            ['계좌', '그룹', '국내 (KRW)', '해외 (외화→KRW)', '미분류', '합계', '비중']
        )

        with st.expander("📊 계좌별 자산 분포", expanded=False):
            st.caption(
                "국내 = KRW 보유분 / 해외 = 외화 보유분, KRW 환산. "
                "한국 계좌에서 산 미국 추종 ETF 는 국내로 잡힘."
            )
            st.dataframe(
                df_breakdown[show_cols].style.format({
                    '국내 (KRW)': '₩{:,.0f}',
                    '해외 (외화→KRW)': '₩{:,.0f}',
                    '미분류': '₩{:,.0f}',
                    '합계': '₩{:,.0f}',
                    '비중': '{:.1f}%',
                }),
                use_container_width=True,
                hide_index=True,
            )

# ---------------------------------------------------------
# [블록 1.7] 연간 KPI 진행
# 호섭님 KPI: 최소 10% / 평균 15% / 최대 20%
# YTD TWR 로 평가 (자금 흐름 효과 자동 제거)
# ---------------------------------------------------------
st.divider()

# 현재 연도 자동 추출
month_cols_kpi = sorted([c for c in df_perf.columns if re.match(r'^\d{4}-\d{2}$', str(c))])
if month_cols_kpi:
    _kpi_year = month_cols_kpi[-1].split('-')[0]
else:
    _kpi_year = str(now_kst().year)

st.subheader(f"🎯 {_kpi_year} 연간 KPI 진행")

KPI_MIN_PCT = 10.0
KPI_AVG_PCT = 15.0
KPI_MAX_PCT = 20.0

ytd_twr = get_perf_pct('YTD')

# YTD 손익 (KRW) — 월별 손익 합산
ytd_pl_kpi = sum(
    (get_perf_raw(f'손익_{m}') or 0)
    for m in month_cols_kpi if m.startswith(_kpi_year)
)

if ytd_twr is None:
    st.info("YTD 수익률 데이터 없음")
else:
    # 5 메트릭 카드 (YTD 손익 카드 추가)
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("현재 YTD (TWR)", f"{ytd_twr:+.2f}%")
    with k2:
        # YTD 손익 (KRW) — 자금흐름 차이 있어서 caveat 는 expander 에서
        st.metric("YTD 손익", f"₩{ytd_pl_kpi:+,.0f}")
    with k3:
        diff = ytd_twr - KPI_MIN_PCT
        st.metric("최소 목표 10%", f"{diff:+.2f}p",
                  delta="달성 ✓" if diff >= 0 else "미달성",
                  delta_color="off")
    with k4:
        diff = ytd_twr - KPI_AVG_PCT
        st.metric("평균 목표 15%", f"{diff:+.2f}p",
                  delta="달성 ✓" if diff >= 0 else "미달성",
                  delta_color="off")
    with k5:
        diff = ytd_twr - KPI_MAX_PCT
        st.metric("최대 목표 20%", f"{diff:+.2f}p",
                  delta="달성 ✓" if diff >= 0 else "미달성",
                  delta_color="off")

    # 진행 상태 메시지
    if ytd_twr >= KPI_MAX_PCT:
        status_msg = f"🏆 **최대 목표 ({KPI_MAX_PCT:.0f}%) 초과 달성!** +{ytd_twr-KPI_MAX_PCT:.2f}%p 더 위"
    elif ytd_twr >= KPI_AVG_PCT:
        status_msg = f"✅ **평균 목표 ({KPI_AVG_PCT:.0f}%) 달성!** 최대까지 {KPI_MAX_PCT-ytd_twr:.2f}%p 남음"
    elif ytd_twr >= KPI_MIN_PCT:
        status_msg = f"✅ **최소 목표 ({KPI_MIN_PCT:.0f}%) 달성!** 평균까지 {KPI_AVG_PCT-ytd_twr:.2f}%p 남음"
    elif ytd_twr >= 0:
        status_msg = f"⏳ **진행 중** — 최소 ({KPI_MIN_PCT:.0f}%)까지 {KPI_MIN_PCT-ytd_twr:.2f}%p 더 필요"
    else:
        status_msg = f"⚠️ **마이너스 영역** — 회복 후 최소까지 {KPI_MIN_PCT-ytd_twr:.2f}%p 필요"
    st.markdown(status_msg)

    # 가로 막대 차트 — 현재 YTD + 목표 라인 3 개
    x_max = max(KPI_MAX_PCT + 5, ytd_twr + 3, 25)
    x_min = min(0, ytd_twr - 3)

    fig_kpi = go.Figure()
    bar_color = '#2e7d32' if ytd_twr >= 0 else '#c62828'
    fig_kpi.add_trace(go.Bar(
        y=['YTD'],
        x=[ytd_twr],
        orientation='h',
        marker_color=bar_color,
        text=[f'{ytd_twr:+.2f}%'],
        textposition='inside',
        textfont=dict(size=15, color='white'),
        showlegend=False,
        hovertemplate=f'현재 YTD: {ytd_twr:+.2f}%<extra></extra>',
    ))
    for tgt, name, color in [
        (KPI_MIN_PCT, '최소', '#ff9800'),
        (KPI_AVG_PCT, '평균', '#2196f3'),
        (KPI_MAX_PCT, '최대', '#9c27b0'),
    ]:
        fig_kpi.add_vline(
            x=tgt,
            line=dict(color=color, width=2, dash='dash'),
            annotation_text=f'{name} {tgt:.0f}%',
            annotation_position='top',
            annotation_font=dict(size=12, color=color),
        )
    fig_kpi.add_vline(x=0, line=dict(color='gray', width=1))
    fig_kpi.update_layout(
        height=180,
        margin=dict(l=40, r=20, t=40, b=40),
        font=dict(size=14, family='sans-serif'),
        xaxis=dict(
            title=dict(text='수익률 (%)', font=dict(size=13)),
            range=[x_min, x_max],
            tickfont=dict(size=12),
            gridcolor='#eeeeee',
            zeroline=False,
        ),
        yaxis=dict(showticklabels=False),
        plot_bgcolor='white',
    )
    st.plotly_chart(fig_kpi, use_container_width=True)

    # ₩ 환산 expander (자금 흐름 caveat 포함)
    with st.expander("📊 ₩ 환산 (대략적 참고)"):
        # ytd_pl_kpi 는 위에서 이미 계산됨 (카드 5 에서 사용)
        # 연초 자산 추정 ≈ 현재 자산 - 올해 손익
        estimated_yr_start = total_assets - ytd_pl_kpi

        st.markdown(f"""
**연초 자산 추정**: ₩{estimated_yr_start:,.0f}  *(현재 총자산 ₩{total_assets:,.0f} − 올해 손익 ₩{ytd_pl_kpi:+,.0f})*

| 목표 | 수익률 | 예상 ₩ 수익 (연초자산 × 목표%) |
|------|--------|------------------------------|
| 최소 | {KPI_MIN_PCT:.0f}% | ₩{estimated_yr_start * KPI_MIN_PCT / 100:,.0f} |
| 평균 | {KPI_AVG_PCT:.0f}% | ₩{estimated_yr_start * KPI_AVG_PCT / 100:,.0f} |
| 최대 | {KPI_MAX_PCT:.0f}% | ₩{estimated_yr_start * KPI_MAX_PCT / 100:,.0f} |

**현재 올해 실제 손익**: ₩{ytd_pl_kpi:+,.0f}

⚠️ **주의 — 호섭님이 짚으신 그 한계**:
- 위 ₩ 수익 = *"연초 자산만큼만 운용했다면"* 의 추정치
- 올해 자금 유입 (입금) / 유출 (출금) 있으면 실제 ₩ 손익은 위 추정과 다름
- **정확한 KPI 추적은 위 막대 차트 (TWR %)** 로. 이게 자금 흐름 자동 제거된 *"순수 운용 실력"*
- ₩ 수치는 *"감 잡기"* 용 참고치
""")

st.divider()

# ---------------------------------------------------------
# [블록 4] 벤치마크 비교 (v75.x reorder: KPI 진행 아래로 이동)
# ---------------------------------------------------------
st.subheader("🆚 벤치마크 비교")

BM_LABELS = ['2026 MAIN BM', 'KOSPI', 'KOSDAQ', 'Shanghai', 'S&P 500', 'NASDAQ']

# 비교 가능한 기간 후보 (df_perf 컬럼에 있는 것만 노출)
period_candidates = [
    '1일', 'WTD(이번주)', 'W-1(저번주)', 'MTD(이번달)', 'M-1(지난달)', 'M-2(2달전)',
    'YTD', '누적수익률(%)',
    '지정(25-05-14~)',  # 멘토 2기 운용 시작
    '지정(25-07-21~)',  # HS 포트폴리오 분리 시작
    '지정(25-10-29~)',  # 멘토 3기 시작
]
period_options = [p for p in period_candidates if not df_perf.empty and p in df_perf.columns]

if not period_options:
    st.info("비교 가능한 기간 컬럼이 없습니다")
else:
    # 기본값: MTD 가 있으면 그걸로
    default_idx = period_options.index('MTD(이번달)') if 'MTD(이번달)' in period_options else 0
    bm_period = st.selectbox(
        "📅 비교 기간 선택",
        period_options,
        index=default_idx,
        help=(
            "벤치마크 구성:\n"
            "• 2026 MAIN BM = KOSPI 37.5% + S&P 500 37.5% + Shanghai 25% (호섭님 자산배분 기준 가중 글로벌 BM)\n"
            "• KOSPI / KOSDAQ / Shanghai / S&P 500 / NASDAQ = 각 지수 단독\n\n"
            "수익률은 모두 같은 기간의 단순 가격 변화율"
        ),
    )

    # 데이터 빌드: 내 포폴 + 6개 벤치마크
    bm_data = []
    me_val = get_perf_pct(bm_period)
    bm_data.append({
        '대상': f'내 포폴 ({view})',
        '수익률': me_val if me_val is not None else 0,
        '있음': me_val is not None,
        'is_me': True,
    })
    for bm in BM_LABELS:
        v = get_bm_pct(bm, bm_period)
        bm_data.append({
            '대상': bm,
            '수익률': v if v is not None else 0,
            '있음': v is not None,
            'is_me': False,
        })

    # 색상: 내 포폴=파랑(강조), BM 양수=초록, BM 음수=빨강, 데이터없음=회색
    bm_colors = []
    for d in bm_data:
        if not d['있음']:
            bm_colors.append('#bdbdbd')
        elif d['is_me']:
            bm_colors.append('#1976d2')  # 강조
        elif d['수익률'] >= 0:
            bm_colors.append('#2e7d32')
        else:
            bm_colors.append('#c62828')

    text_labels_bm = [
        f"{d['수익률']:+.2f}%" if d['있음'] else '-'
        for d in bm_data
    ]

    fig_bm = go.Figure(go.Bar(
        x=[d['대상'] for d in bm_data],
        y=[d['수익률'] for d in bm_data],
        marker_color=bm_colors,
        text=text_labels_bm,
        textposition='outside',
        textfont=dict(size=14, color='#222'),
        cliponaxis=False,
        hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
    ))
    fig_bm.add_hline(y=0, line_color='gray', line_width=1)
    fig_bm.update_layout(
        height=400,
        margin=dict(l=40, r=20, t=20, b=80),
        showlegend=False,
        font=dict(size=14, family='sans-serif'),
        yaxis=dict(
            title=dict(text="수익률 (%)", font=dict(size=15)),
            tickfont=dict(size=13),
            gridcolor='#eeeeee',
        ),
        xaxis=dict(
            title="",
            tickfont=dict(size=14),
            tickangle=-15,
        ),
        bargap=0.3,
    )
    st.plotly_chart(fig_bm, use_container_width=True)
    st.caption(
        "💡 **2026 MAIN BM** = KOSPI 37.5% + S&P 500 37.5% + Shanghai 25% "
        "(2026년 자산배분 가중치 기준). 자산배분이 바뀌면 새 연도 BM 추가 가능."
    )

st.divider()

# ---------------------------------------------------------
# [블록 3] 단기 수익률 + 손익 표
# ---------------------------------------------------------
st.subheader("📈 단기 수익률")

if perf_row is None:
    st.warning(f"performance_summary 시트에서 '{perf_label}' 행을 찾을 수 없습니다")
else:
    short_cols = [
        '1일', 'WTD(이번주)', 'W-1(저번주)', 'W-2(2주전)', 'W-3(3주전)',
        'MTD(이번달)', 'M-1(지난달)', 'M-2(2달전)', 'YTD',
    ]

    short_data = []
    for c in short_cols:
        v = get_perf_pct(c)
        short_data.append({
            '기간': c,
            '수익률': v if v is not None else 0,
            '있음': v is not None,
        })

    df_short = pd.DataFrame(short_data)

    # 막대그래프 — 양수=초록, 음수=빨강, 데이터 없음=회색
    import plotly.graph_objects as go

    colors = []
    for _, row in df_short.iterrows():
        if not row['있음']:
            colors.append('#bdbdbd')  # 회색
        elif row['수익률'] >= 0:
            colors.append('#2e7d32')  # 초록
        else:
            colors.append('#c62828')  # 빨강

    text_labels = [
        f"{r['수익률']:+.2f}%" if r['있음'] else '-'
        for _, r in df_short.iterrows()
    ]

    fig_short = go.Figure(go.Bar(
        x=df_short['기간'],
        y=df_short['수익률'],
        marker_color=colors,
        text=text_labels,
        textposition='outside',
        textfont=dict(size=15, color='#222', family='sans-serif'),  # 막대 위 % 라벨
        cliponaxis=False,
        hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
    ))
    fig_short.add_hline(y=0, line_color='gray', line_width=1)
    fig_short.update_layout(
        height=420,
        margin=dict(l=40, r=20, t=30, b=60),
        showlegend=False,
        font=dict(size=14, family='sans-serif'),  # 전체 기본 폰트
        yaxis=dict(
            title=dict(text="수익률 (%)", font=dict(size=15)),
            tickfont=dict(size=13),
            gridcolor='#eeeeee',
            zeroline=False,
        ),
        xaxis=dict(
            title="",
            tickfont=dict(size=15),  # 1일/WTD 등 기간 라벨
        ),
        bargap=0.3,
    )
    st.plotly_chart(fig_short, use_container_width=True)

# ---------------------------------------------------------
# [블록 3.5] 단기 손익 (KRW) — 수익률 차트의 자매
# 이번달/지난달/2달전 + YTD + 5/14~ + 7/21~ + 누적
# ---------------------------------------------------------
st.subheader("💰 단기 손익")

if perf_row is None:
    st.info("손익 데이터 없음 (perf_row 못 찾음)")
else:
    # 사용 가능한 월 컬럼 검색
    month_cols_pl = sorted([c for c in df_perf.columns if re.match(r'^\d{4}-\d{2}$', str(c))])

    pl_labels = []
    pl_values = []

    # 이번달 / 지난달 / 2달전
    if month_cols_pl:
        for offset, label_prefix in [(-1, '이번달'), (-2, '지난달'), (-3, '2달전')]:
            if abs(offset) <= len(month_cols_pl):
                mcol = month_cols_pl[offset]
                v = get_perf_raw(f'손익_{mcol}')
                yymm = mcol[2:].replace('-', '/')  # '26/04'
                pl_labels.append(f'{label_prefix}<br>({yymm})')
                pl_values.append(v if v is not None else 0)

    # YTD (올해 월별 합)
    if month_cols_pl:
        current_month = month_cols_pl[-1]
        current_year = current_month.split('-')[0]
        ytd_months = [c for c in month_cols_pl if c.startswith(current_year)]
        ytd_total = sum((get_perf_raw(f'손익_{m}') or 0) for m in ytd_months)
        pl_labels.append(f'YTD<br>({current_year})')
        pl_values.append(ytd_total)

    # 5/14~ (멘토 2기 inception)
    pl_5_14 = get_perf_raw('지정_손익(25-05-14~)')
    if pl_5_14 is not None:
        pl_labels.append('5/14~')
        pl_values.append(pl_5_14)

    # 7/21~ (HS inception)
    pl_7_21 = get_perf_raw('지정_손익(25-07-21~)')
    if pl_7_21 is not None:
        pl_labels.append('7/21~')
        pl_values.append(pl_7_21)

    # 10/29~ (멘토 3기 inception)
    pl_10_29 = get_perf_raw('지정_손익(25-10-29~)')
    if pl_10_29 is not None:
        pl_labels.append('10/29~')
        pl_values.append(pl_10_29)

    # 누적 (전체기간)
    cumulative_pl = get_perf_raw('평가손익')
    if cumulative_pl is None:
        cumulative_pl = total_pl  # fallback: dashboard 계산값
    pl_labels.append('누적')
    pl_values.append(cumulative_pl)

    if not pl_labels:
        st.info("표시할 손익 데이터 없음")
    else:
        # 색상: 양수=초록, 음수=빨강
        pl_colors = ['#2e7d32' if v >= 0 else '#c62828' for v in pl_values]

        fig_pl_chart = go.Figure(go.Bar(
            x=pl_labels,
            y=pl_values,
            marker_color=pl_colors,
            text=[f'₩{v:+,.0f}' for v in pl_values],
            textposition='outside',
            textfont=dict(size=12, color='#222', family='sans-serif'),
            cliponaxis=False,
            hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
        ))
        fig_pl_chart.add_hline(y=0, line_color='gray', line_width=1)
        fig_pl_chart.update_layout(
            height=420,
            margin=dict(l=40, r=20, t=20, b=70),
            showlegend=False,
            font=dict(size=14, family='sans-serif'),
            yaxis=dict(
                title=dict(text="손익 (₩)", font=dict(size=14)),
                tickfont=dict(size=11),
                gridcolor='#eeeeee',
            ),
            xaxis=dict(tickfont=dict(size=12)),
            bargap=0.3,
            plot_bgcolor='white',
        )
        st.plotly_chart(fig_pl_chart, use_container_width=True)

st.divider()

# ---------------------------------------------------------
# [블록 7] 월별 / 분기별 수익률 (v75.x reorder: 단기 손익 아래로 이동)
# performance_summary 시트에 자동 생성된 월/분기 컬럼 surface
# ---------------------------------------------------------
st.subheader("📅 월별 / 분기별 수익률")

if perf_row is None:
    st.info(f"performance_summary 시트에서 '{perf_label}' 행을 못 찾아 표시 불가")
else:
    # 사용 가능한 월/분기 컬럼 자동 검색 (패턴 매칭)
    month_cols = sorted([c for c in df_perf.columns if re.match(r'^\d{4}-\d{2}$', str(c))])
    quarter_cols = sorted([c for c in df_perf.columns if re.match(r'^\d{4}-Q\d$', str(c))])

    if not month_cols and not quarter_cols:
        st.info("월별/분기별 컬럼이 performance_summary 에 없음 (performance.py 최신 버전 실행 필요)")
    else:
        def render_period_chart(period_cols, label):
            if not period_cols:
                st.info(f"{label} 데이터 없음")
                return

            twr_vals = [get_perf_pct(c) if get_perf_pct(c) is not None else 0 for c in period_cols]
            mwr_vals = [get_perf_pct(f'MWR_{c}') if get_perf_pct(f'MWR_{c}') is not None else 0 for c in period_cols]
            pl_vals = [get_perf_raw(f'손익_{c}') if get_perf_raw(f'손익_{c}') is not None else 0 for c in period_cols]

            # 차트 1: TWR + MWR 그룹 막대
            fig_ret = go.Figure()
            fig_ret.add_trace(go.Bar(
                name='TWR',
                x=period_cols,
                y=twr_vals,
                marker_color='#1976d2',
                text=[f'{v:+.1f}%' for v in twr_vals],
                textposition='outside',
                textfont=dict(size=11),
            ))
            fig_ret.add_trace(go.Bar(
                name='MWR',
                x=period_cols,
                y=mwr_vals,
                marker_color='#fb8c00',
                text=[f'{v:+.1f}%' for v in mwr_vals],
                textposition='outside',
                textfont=dict(size=11),
            ))
            fig_ret.add_hline(y=0, line_color='gray', line_width=1)
            fig_ret.update_layout(
                barmode='group',
                title=dict(text=f'{label} 수익률 — TWR / MWR (%)', font=dict(size=15)),
                height=380,
                margin=dict(l=20, r=20, t=60, b=50),
                legend=dict(orientation='h', yanchor='top', y=1.08, xanchor='center', x=0.5,
                            font=dict(size=13)),
                font=dict(size=13, family='sans-serif'),
                yaxis=dict(title='수익률 (%)', tickfont=dict(size=11), gridcolor='#eeeeee'),
                xaxis=dict(tickfont=dict(size=12), tickangle=-30),
                bargap=0.2,
                bargroupgap=0.08,
                plot_bgcolor='white',
            )
            st.plotly_chart(fig_ret, use_container_width=True)

            # 차트 2: 손익 (KRW) — 양수=초록, 음수=빨강
            colors = ['#2e7d32' if v >= 0 else '#c62828' for v in pl_vals]
            fig_pl = go.Figure(go.Bar(
                x=period_cols,
                y=pl_vals,
                marker_color=colors,
                text=[f'₩{v:+,.0f}' for v in pl_vals],
                textposition='outside',
                textfont=dict(size=10),
                cliponaxis=False,
            ))
            fig_pl.add_hline(y=0, line_color='gray', line_width=1)
            fig_pl.update_layout(
                title=dict(text=f'{label} 손익 (KRW)', font=dict(size=15)),
                height=320,
                margin=dict(l=20, r=20, t=50, b=50),
                showlegend=False,
                font=dict(size=13, family='sans-serif'),
                yaxis=dict(title='손익 (₩)', tickfont=dict(size=11), gridcolor='#eeeeee'),
                xaxis=dict(tickfont=dict(size=12), tickangle=-30),
                bargap=0.3,
                plot_bgcolor='white',
            )
            st.plotly_chart(fig_pl, use_container_width=True)

        tab_m, tab_q = st.tabs([
            f"📊 월별 ({len(month_cols)}개월)",
            f"📊 분기별 ({len(quarter_cols)}개 분기)",
        ])
        with tab_m:
            render_period_chart(month_cols, '월별')
        with tab_q:
            render_period_chart(quarter_cols, '분기별')

st.divider()

# ---------------------------------------------------------
# [블록 2] 비중 도넛 3개 (성과 섹션 아래로 v75.x reorder)
# ---------------------------------------------------------
st.subheader("🥧 비중")

def _prep_for_pie(df, group_col, cash_label='현금', hedge_label='헷지', detect_hedge=False):
    """cash row 와 (옵션) hedge row 의 group_col 값을 명확한 라벨로 변경.
    - cash 는 ticker.startswith('CASH') 로 감지 → '현금' 라벨
    - hedge 는 **방위군** (postion/position='방위군' OR military='방위군') 만 감지 (detect_hedge=True 시)
      → '헷지' 라벨.
      ⚠️ theme='헷지' 는 매칭 안 함 — 채권혼합 ETF 가 theme='헷지' 라서 같이 잡히면
         rebalancing_master 의 country (G열) 기반 분류와 의미가 어긋남.
         rebalancing_master 와 동일 의미: 방위군 (인버스/VIX) 만 헷지, 채권혼합은 자기 국가로.
    """
    if group_col not in df.columns or df.empty:
        return df
    df = df.copy()
    # 1) Cash 라벨링
    mask_cash = df['ticker'].astype(str).str.startswith('CASH')
    df.loc[mask_cash, group_col] = cash_label
    # 2) Hedge 라벨링 (요청된 경우만) — 방위군만
    if detect_hedge:
        mask_hedge = pd.Series(False, index=df.index)
        # dashboard_data 는 'position' (정상 영문), rebalancing_master 는 'postion' (오타) — 둘 다 매칭
        if 'postion' in df.columns:
            mask_hedge |= df['postion'].astype(str).str.strip() == '방위군'
        if 'position' in df.columns:
            mask_hedge |= df['position'].astype(str).str.strip() == '방위군'
        if 'military' in df.columns:
            mask_hedge |= df['military'].astype(str).str.strip() == '방위군'
        # cash 우선 (cash 면 '현금' 유지, hedge 로 덮어쓰지 않음)
        mask_hedge = mask_hedge & ~mask_cash
        df.loc[mask_hedge, group_col] = hedge_label
    return df

def make_pie(df, group_col, title, value_col='market_value_krw'):
    if group_col not in df.columns or df.empty:
        return None
    grouped = (
        df.groupby(group_col, dropna=False)[value_col]
        .sum()
        .reset_index()
    )
    grouped = grouped[grouped[value_col] > 0]
    grouped[group_col] = grouped[group_col].astype(str).replace('', '미분류')
    if grouped.empty:
        return None
    fig = px.pie(grouped, values=value_col, names=group_col, hole=0.5)
    fig.update_traces(
        textposition='inside',
        textinfo='label+percent',
        textfont=dict(size=14, family='sans-serif'),  # 도넛 안 라벨
        insidetextorientation='radial',
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=18)),  # 차트 제목
        showlegend=False,
        height=340,
        margin=dict(l=10, r=10, t=50, b=10),
        font=dict(size=14, family='sans-serif'),
    )
    return fig

# ---------------------------------------------------------
# Long weight helper — main.py 의 _long_weight_from_pc 와 동일 로직
# 도넛 차트 "Long 자본 분포" 그릴 때 사용
# ---------------------------------------------------------
def _long_weight_for_view(pc, ticker, postion):
    """pension_class + position → Long weight (0.0~1.0)."""
    if (ticker or '').startswith('CASH'):
        return 0.0
    if (postion or '').strip() == '방위군':
        return 0.0
    pc = (pc or '').strip()
    # 순수 숫자
    try:
        v = float(pc)
        if 0 <= v <= 100:
            return v / 100
    except (ValueError, TypeError):
        pass
    # 라벨+숫자 (예: 채권혼합20)
    for prefix in ('채권혼합', '혼합'):
        if pc.startswith(prefix) and len(pc) > len(prefix):
            rest = pc[len(prefix):].strip()
            try:
                v = float(rest)
                if 0 <= v <= 100:
                    return v / 100
            except (ValueError, TypeError):
                pass
    if pc in {'안전', '안전자산', '채권', '국채', 'MMF', '현금'}:
        return 0.0
    if pc in {'채권혼합', '혼합'}:
        return 0.3
    return 1.0  # 위험/빈칸 = 100% Long

# pension_class lookup (master_data 에서)
_pc_lookup_pie = {}
if 'df_master' in globals() and not df_master.empty and 'pension_class' in df_master.columns:
    _pc_lookup_pie = dict(zip(
        df_master['ticker'].astype(str).str.strip(),
        df_master['pension_class'].astype(str).str.strip()
    ))

def _attach_long_mv(df):
    """df 에 effective_mv 컬럼 추가 (market_value × long_weight). 헷지/현금/안전 자동 제외.

    ⚠️ dashboard_data 의 컬럼명은 'position' (정상 영문),
       rebalancing_master 의 컬럼명은 'postion' (오타) — 둘 다 fallback 으로 매칭.
    """
    if df.empty or 'market_value_krw' not in df.columns:
        return df
    df = df.copy()
    def _get_postion(r):
        # 'postion' 우선 (rebalancing_master 호환), 없으면 'position' (dashboard_data)
        return str(r.get('postion', '') or r.get('position', '')).strip()
    df['__long_w'] = df.apply(
        lambda r: _long_weight_for_view(
            _pc_lookup_pie.get(str(r.get('ticker', '')).strip(), ''),
            str(r.get('ticker', '')).strip(),
            _get_postion(r)
        ),
        axis=1
    )
    df['effective_mv'] = df['market_value_krw'] * df['__long_w']
    return df

pc1, pc2, pc3, pc4 = st.columns(4)
with pc1:
    # 국가별 (NAV 기준) — 기존: 헷지/현금 포함
    fig = make_pie(_prep_for_pie(df_view, 'country', detect_hedge=True), 'country', '국가별 (NAV)')
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info("국가별 데이터 없음")

with pc2:
    # 국가별 (Long 기준) — 신규: 헷지/현금/안전 자동 제외 (long_weight=0 이라서)
    # 채권혼합은 30% 만 반영, 순수 베팅 분포 표시
    df_view_lw = _attach_long_mv(df_view)
    fig = make_pie(df_view_lw, 'country', '국가별 (Long)', value_col='effective_mv')
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info("국가별 (Long) 데이터 없음")

with pc3:
    fig = make_pie(df_view, 'theme', '테마별')
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info("테마별 데이터 없음")

with pc4:
    # 군종별 또는 그룹별: cash row → '현금'. 그룹별 (전체뷰) 는 cash 변환 안 함
    if third_pie_col == 'group_name':
        fig = make_pie(df_view, third_pie_col, third_pie_title)
    else:
        fig = make_pie(_prep_for_pie(df_view, third_pie_col), third_pie_col, third_pie_title)
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info(f"{third_pie_title} 데이터 없음")

st.divider()

# ---------------------------------------------------------
# [블록 5] 현금 / Gross / Long / Net 비중
# rebalancing_master 시트의 자동 수식 결과를 직접 사용
# - 멘토/HS 뷰: 해당 그룹 행에서 Q~V 컬럼값 그대로
# - 전체 뷰: 멘토 + HS 의 AUM 가중평균
# ---------------------------------------------------------
st.subheader("⚖️ 현금 / Gross / Long / Net 비중")

with st.expander("ℹ️ 계산 방식 보기"):
    st.markdown(
        """
**값의 출처**: `rebalancing_master` 시트의 자동 수식 결과를 그대로 사용 —
호섭님이 시트에서 수식 바꾸시면 dashboard 도 자동 따라옵니다 (single source of truth).

**개념**:

| 지표 | 의미 |
|------|------|
| **현금** | 투자 안 되고 cash 로 남아있는 비율 |
| **Gross** | 투자된 자산 비중 (현금 제외, 헷지 포함) |
| **Long** | 시장 상승에 베팅한 비중 (헷지 제외, HS는 채권혼합 30%만 인정) |
| **Net** | Long − 헷지의 effective short 노출 (VIX 같은 3x leveraged 는 ×3 차감) |

**전체 뷰** = 멘토 + HS 의 AUM 가중평균.
*"내 전체 자산이 시장에 얼마나 노출되어 있나"* 에 대한 정확한 답.

**멘토 vs HS 차이**: HS는 퇴직연금이 있어서 채권혼합의 30%만 Long에 포함하고, 나머지 70%는 방어 자산으로 빠짐.
        """
    )

def get_group_metric(metric_col, accounts):
    """rebalancing_master 시트에서 해당 계좌들의 metric 값 추출.
    셀 머지 또는 첫 행에만 값이 있을 수 있어, 첫 non-empty 값을 사용."""
    if df_rebal_master.empty or 'account' not in df_rebal_master.columns:
        return None
    if metric_col not in df_rebal_master.columns:
        return None
    sub = df_rebal_master[df_rebal_master['account'].apply(clean_account).isin(accounts)]
    if sub.empty:
        return None
    for v in sub[metric_col]:
        s = str(v).strip()
        if s and s.lower() not in ('-', 'nan', 'none'):
            parsed = parse_pct_value(s)
            if parsed is not None:
                return parsed
    return None

# AUM 계산 (전체 뷰의 가중평균에 사용 + UI 표시용)
mentor_aum = df_dashboard[df_dashboard['account_clean'].isin(MENTOR_ACCS)]['market_value_krw'].sum()
hs_aum = df_dashboard[df_dashboard['account_clean'].isin(HS_ACCS)]['market_value_krw'].sum()

def get_total_weighted(metric_col):
    """전체 뷰: 멘토 + HS 의 AUM 가중평균.
    멘토만 있거나 HS만 있는 metric 도 안전하게 처리."""
    total_aum = mentor_aum + hs_aum
    if total_aum == 0:
        return None
    mv = get_group_metric(metric_col, MENTOR_ACCS)
    hv = get_group_metric(metric_col, HS_ACCS)
    if mv is None and hv is None:
        return None
    if mv is None: mv = 0.0
    if hv is None: hv = 0.0
    return (mentor_aum * mv + hs_aum * hv) / total_aum

# 4 개 metric 의 (목표, 현재) 컬럼 매핑
METRIC_COLUMNS = [
    ('현금', '현금 (목표)', '현금 (현재)'),
    ('Gross', 'Gross (목표)', 'Gross (현재)'),
    ('Long', 'Long (목표)', 'Long (현재)'),
    ('Net', 'Net (목표)', 'Net (현재)'),
]

# 현재 뷰의 metric 값 읽기
if view == "전체":
    metric_values = [
        (label, get_total_weighted(t_col), get_total_weighted(c_col))
        for label, t_col, c_col in METRIC_COLUMNS
    ]
    show_target = False  # 호섭님 spec: 전체는 현재만
elif view == "멘토 포폴":
    metric_values = [
        (label, get_group_metric(t_col, MENTOR_ACCS), get_group_metric(c_col, MENTOR_ACCS))
        for label, t_col, c_col in METRIC_COLUMNS
    ]
    show_target = True
else:  # HS 포폴
    metric_values = [
        (label, get_group_metric(t_col, HS_ACCS), get_group_metric(c_col, HS_ACCS))
        for label, t_col, c_col in METRIC_COLUMNS
    ]
    show_target = True

def fmt_simple_pct(v):
    if v is None: return "-"
    try: return f"{float(v):.1f}%"
    except: return "-"

# 4 컬럼 가로 배치 — 현금 | Gross | Long | Net
cols_m = st.columns(4)
for col_st, (label, target, current) in zip(cols_m, metric_values):
    delta_str = None
    if show_target and target is not None:
        delta_str = f"목표 {fmt_simple_pct(target)}"
    with col_st:
        st.metric(
            label,
            fmt_simple_pct(current),
            delta=delta_str,
            delta_color="off",
        )

# 그룹 막대 차트 — 목표 vs 현재 시각 비교
chart_labels = [m[0] for m in metric_values]
target_vals = [m[1] if m[1] is not None else 0 for m in metric_values]
current_vals = [m[2] if m[2] is not None else 0 for m in metric_values]

fig_gln = go.Figure()
if show_target:
    fig_gln.add_trace(go.Bar(
        name='목표',
        x=chart_labels,
        y=target_vals,
        marker_color='#90caf9',  # 연한 파랑
        text=[f'{v:.1f}%' for v in target_vals],
        textposition='outside',
        textfont=dict(size=13, color='#1565c0'),
    ))
fig_gln.add_trace(go.Bar(
    name='현재' if show_target else '현재 비중',
    x=chart_labels,
    y=current_vals,
    marker_color='#1565c0',  # 진한 파랑
    text=[f'{v:.1f}%' for v in current_vals],
    textposition='outside',
    textfont=dict(size=13, color='#0d47a1'),
))
fig_gln.update_layout(
    barmode='group',
    height=300,
    margin=dict(l=20, r=20, t=30, b=40),
    showlegend=show_target,
    legend=dict(orientation='h', yanchor='top', y=1.12, xanchor='center', x=0.5,
                font=dict(size=13)),
    font=dict(size=14, family='sans-serif'),
    yaxis=dict(
        title=dict(text='비중 (%)', font=dict(size=14)),
        tickfont=dict(size=12),
        gridcolor='#eeeeee',
    ),
    xaxis=dict(tickfont=dict(size=15)),
    bargap=0.25,
    bargroupgap=0.08,
    plot_bgcolor='white',
)
st.plotly_chart(fig_gln, use_container_width=True)

# 출처/가중치 캡션
if view == "전체":
    total_aum = mentor_aum + hs_aum
    if total_aum > 0:
        st.caption(
            f"💡 AUM 가중평균 — 멘토 ₩{mentor_aum:,.0f} ({mentor_aum/total_aum*100:.1f}%) "
            f"+ HS ₩{hs_aum:,.0f} ({hs_aum/total_aum*100:.1f}%) | "
            f"`rebalancing_master` 시트 자동 수식 결과 기반"
        )
    else:
        st.caption("💡 `rebalancing_master` 시트 자동 수식 결과")
else:
    st.caption(
        "💡 위 값은 `rebalancing_master` 시트의 자동 수식 결과 — "
        "시트에서 수식/목표 바꾸시면 dashboard 도 자동 따라옵니다."
    )

# ---------------------------------------------------------
# [블록 5.3] 국가별 Gross 비중
# rebalancing_master 의 K(국가별 Gross 목표) / L(국가별 Gross 현재) 사용
# 머지 셀이라 첫 행에만 값 → df_rebal_master 에서 country 별 첫 non-empty 값 추출
# ---------------------------------------------------------
st.divider()
st.subheader("🌏 국가별 Gross 비중")

with st.expander("ℹ️ 계산 방식 보기"):
    st.markdown(
        """
**값의 출처**: `rebalancing_master` 시트의 `국가별 Gross` 컬럼 (K/L 열).

**개념**: 포트폴리오 내 자산이 **어느 국가 시장**에 얼마나 노출되어 있는지.
- **국내** = 한국 시장 (KOSPI/KOSDAQ 종목)
- **중국** = 중국 본토/홍콩 종목
- **미국** = 미국 시장 종목
- **헷지** = 인버스 ETF / VIX / 채권 등 시장 하락 베팅 자산

**합계 = Gross (현금 제외 투자 자산 합)**. 즉 4개 국가 합 = Gross 비중.
        """
    )

# rebalancing_master 의 country 별 첫 non-empty 값 추출
def get_country_metric(country_label, metric_col, accounts):
    """rebalancing_master 에서 (account, country) 매칭 행의 metric 첫 non-empty 값."""
    if df_rebal_master.empty:
        return None
    if 'account' not in df_rebal_master.columns or 'Country' not in df_rebal_master.columns:
        return None
    if metric_col not in df_rebal_master.columns:
        return None
    sub = df_rebal_master[
        df_rebal_master['account'].apply(clean_account).isin(accounts)
        & (df_rebal_master['Country'].astype(str).str.strip() == country_label)
    ]
    if sub.empty:
        return None
    for v in sub[metric_col]:
        s = str(v).strip()
        if s and s.lower() not in ('-', 'nan', 'none'):
            parsed = parse_pct_value(s)
            if parsed is not None:
                return parsed
    return None

def get_country_total_weighted(country_label, metric_col):
    """전체 뷰: 멘토 + HS 의 AUM 가중평균 (해당 country 행 기준)."""
    total_aum = mentor_aum + hs_aum
    if total_aum == 0:
        return None
    mv = get_country_metric(country_label, metric_col, MENTOR_ACCS)
    hv = get_country_metric(country_label, metric_col, HS_ACCS)
    if mv is None and hv is None:
        return None
    if mv is None: mv = 0.0
    if hv is None: hv = 0.0
    return (mentor_aum * mv + hs_aum * hv) / total_aum

# 4 country 로 표시 — 시트의 Country 라벨과 일치해야 함
COUNTRIES = ['국내', '중국', '미국', '헷지']
TARGET_COL = '국가별 Gross (목표)'
CURRENT_COL = '국가별 Gross (현재)'

if view == "전체":
    country_values = [
        (c, get_country_total_weighted(c, TARGET_COL),
            get_country_total_weighted(c, CURRENT_COL))
        for c in COUNTRIES
    ]
    show_target_country = False
elif view == "멘토 포폴":
    country_values = [
        (c, get_country_metric(c, TARGET_COL, MENTOR_ACCS),
            get_country_metric(c, CURRENT_COL, MENTOR_ACCS))
        for c in COUNTRIES
    ]
    show_target_country = True
else:  # HS 포폴
    country_values = [
        (c, get_country_metric(c, TARGET_COL, HS_ACCS),
            get_country_metric(c, CURRENT_COL, HS_ACCS))
        for c in COUNTRIES
    ]
    show_target_country = True

# 4 카드 가로 배치 (블록 5 와 동일 양식)
cols_country = st.columns(4)
for col_st, (label, target, current) in zip(cols_country, country_values):
    delta_str = None
    if show_target_country and target is not None:
        delta_str = f"목표 {fmt_simple_pct(target)}"
    with col_st:
        st.metric(
            label,
            fmt_simple_pct(current),
            delta=delta_str,
            delta_color="off",
        )

# 그룹 막대 차트 (녹색 톤 — 블록 5 의 파란색과 차별화)
country_labels = [c[0] for c in country_values]
country_target_vals = [c[1] if c[1] is not None else 0 for c in country_values]
country_current_vals = [c[2] if c[2] is not None else 0 for c in country_values]

fig_country = go.Figure()
if show_target_country:
    fig_country.add_trace(go.Bar(
        name='목표',
        x=country_labels,
        y=country_target_vals,
        marker_color='#a5d6a7',  # 연한 녹색 (sage)
        text=[f'{v:.1f}%' for v in country_target_vals],
        textposition='outside',
        textfont=dict(size=13, color='#2e7d32'),
    ))
fig_country.add_trace(go.Bar(
    name='현재' if show_target_country else '현재 비중',
    x=country_labels,
    y=country_current_vals,
    marker_color='#2e7d32',  # 진한 녹색 (forest)
    text=[f'{v:.1f}%' for v in country_current_vals],
    textposition='outside',
    textfont=dict(size=13, color='#1b5e20'),
))
fig_country.update_layout(
    barmode='group',
    height=300,
    margin=dict(l=20, r=20, t=30, b=40),
    showlegend=show_target_country,
    legend=dict(orientation='h', yanchor='top', y=1.12, xanchor='center', x=0.5,
                font=dict(size=13)),
    font=dict(size=14, family='sans-serif'),
    yaxis=dict(
        title=dict(text='비중 (%)', font=dict(size=14)),
        tickfont=dict(size=12),
        gridcolor='#eeeeee',
    ),
    xaxis=dict(tickfont=dict(size=15)),
    bargap=0.25,
    bargroupgap=0.08,
    plot_bgcolor='white',
)
st.plotly_chart(fig_country, use_container_width=True)

st.caption(
    "💡 4개 카드 합 ≈ Gross (현금 제외 투자 비중). "
    "차이가 크면 특정 국가에 과/저 노출된 상태."
)

# ---------------------------------------------------------
# [블록 5.5] 퇴직연금 가드 (HS 뷰 전용)
# 220914426167, 717190227129 의 위험/안전 비중을 실시간 dashboard_data 로 계산.
# 호섭님 시트 가드와 동일 로직, 폰에서도 한눈에 확인 가능.
# ---------------------------------------------------------
PENSION_ACCS = ['220914426167', '717190227129']

if view == "HS 포폴":
    st.divider()
    st.subheader("🛡️ 퇴직연금 가드")
    st.caption(
        "**규제**: 위험자산 ≤ 70% / 안전자산 ≥ 30% (분모 = 계좌 AUM 전체, 현금 포함).  \n"
        "**분류 규칙**: master_data 시트의 `pension_class` 컬럼 명시값 사용. **빈칸 = 위험 (기본값)**.  \n"
        "• `안전`/`채권`/`국채`/`MMF` → 0% 위험  \n"
        "• `채권혼합` → 0% 위험 (한국 규정상 100% 안전, 주식 ≤40%)  \n"
        "• `헷지`/`위험`/빈칸 → 100% 위험  \n"
        "• 숫자 직접 입력 가능 (예: `15` → 15% 위험, `채권혼합50` → 채권혼합 50%)  \n"
        "• 현금 (CASH) 은 시스템상 자동으로 안전"
    )

    # [v75.x] master_data 의 pension_class 컬럼 룩업 (명시적 분류 우선)
    pension_class_lookup = {}
    if not df_master.empty and 'ticker' in df_master.columns and 'pension_class' in df_master.columns:
        pension_class_lookup = dict(zip(
            df_master['ticker'].astype(str).str.strip(),
            df_master['pension_class'].astype(str).str.strip()
        ))

    def _classify_pension(pc, name, theme, position, ticker):
        """반환값: (분류명, 위험비중 0.0~1.0) 튜플.
        우선순위: 현금(시스템) > pc 숫자 > pc 라벨+숫자 > pc 라벨 > 빈칸=위험.
        """
        # 0) CASH 시스템 row 는 항상 안전 (사용자 관리 영역 밖)
        if (ticker or '').startswith('CASH'):
            return ('안전', 0.0)

        pc = (pc or '').strip()

        # 1) 순수 숫자 (예: "15" → 15% 위험)
        try:
            val = float(pc)
            if 0 <= val <= 100:
                return ('사용자지정', val / 100)
        except ValueError:
            pass

        # 2) 라벨+숫자 (예: "채권혼합20" → 채권혼합 20%)
        for prefix in ('채권혼합', '혼합'):
            if pc.startswith(prefix) and len(pc) > len(prefix):
                rest = pc[len(prefix):].strip()
                try:
                    val = float(rest)
                    if 0 <= val <= 100:
                        return ('채권혼합', val / 100)
                except ValueError:
                    pass

        # 3) 라벨 매핑
        SAFE = {'안전', '안전자산', '채권', '국채', 'MMF', '현금'}
        BOND_MIX = {'채권혼합', '혼합'}
        HEDGE = {'헷지', '인버스', 'VIX', '레버리지'}
        RISK = {'위험', '주식', '공격'}
        if pc in SAFE: return ('안전', 0.0)
        # 한국 퇴직연금 규정: 채권혼합 ETF (주식 ≤40%) 는 100% 안전자산
        if pc in BOND_MIX: return ('채권혼합', 0.0)
        if pc in HEDGE: return ('헷지', 1.0)
        if pc in RISK: return ('위험', 1.0)

        # 4) 빈칸 또는 미인식 → 보수적으로 위험으로 간주
        return ('위험', 1.0)

    pension_rows = []
    for acc in PENSION_ACCS:
        sub = df_view[df_view['account_clean'] == acc].copy()
        if sub.empty:
            continue

        themes = sub.get('theme', pd.Series([''] * len(sub))).astype(str).str.strip()
        positions = sub.get('postion', pd.Series([''] * len(sub))).astype(str).str.strip()
        names = sub.get('name', pd.Series([''] * len(sub))).astype(str)
        tickers = sub.get('ticker', pd.Series([''] * len(sub))).astype(str).str.strip()

        # 각 행을 (분류명, 위험비중) 튜플로 분류
        results = [
            _classify_pension(
                pension_class_lookup.get(tickers.iloc[i], ''),
                names.iloc[i], themes.iloc[i], positions.iloc[i], tickers.iloc[i]
            )
            for i in range(len(sub))
        ]
        classifications = pd.Series([r[0] for r in results], index=sub.index)
        risk_ratios = pd.Series([r[1] for r in results], index=sub.index)

        # 위험자산 = 각 종목의 평가액 × 위험비중 합
        # 안전자산 = 각 종목의 평가액 × (1 - 위험비중) 합
        risk_mv = float((sub['market_value_krw'] * risk_ratios).sum())
        safe_mv = float((sub['market_value_krw'] * (1 - risk_ratios)).sum())
        total_mv = sub['market_value_krw'].sum()
        hedge_mv = sub.loc[classifications == '헷지', 'market_value_krw'].sum()

        if total_mv == 0:
            continue

        risk_pct = risk_mv / total_mv * 100
        safe_pct = safe_mv / total_mv * 100

        # 상태 (위험 ≤ 70 AND 안전 ≥ 30 모두 만족해야 ✅)
        if risk_pct > 70 or safe_pct < 30:
            status = "🚨 한도 초과"
        elif risk_pct > 65 or safe_pct < 35:
            status = "⚠️ 임박"
        else:
            status = "✅ 여유"

        pension_rows.append({
            '계좌': acc,
            '계좌 AUM': total_mv,
            '위험자산 (₩)': risk_mv,
            '위험 %': risk_pct,
            '안전자산 (₩)': safe_mv,
            '안전 %': safe_pct,
            '상태': status,
        })

        # === 🐛 디버그: 종목별 분류 + 위험비중 표시 ===
        with st.expander(f"🐛 [{acc}] 종목별 분류 디버그 (펼쳐서 확인)"):
            debug_df = pd.DataFrame({
                'ticker': tickers.values,
                'name': names.values,
                'market_value_krw': sub['market_value_krw'].values,
                'theme': themes.values,
                'pension_class (시트값)': [pension_class_lookup.get(t, '') for t in tickers.values],
                '최종 분류': classifications.values,
                '위험비중': risk_ratios.values,
                '위험기여 (₩)': (sub['market_value_krw'] * risk_ratios).values,
            })
            debug_df = debug_df.sort_values('market_value_krw', ascending=False).reset_index(drop=True)

            def _row_color(row):
                cls = row['최종 분류']
                colors = {
                    '안전': 'background-color: #e8f5e9',
                    '채권혼합': 'background-color: #fff8e1',
                    '사용자지정': 'background-color: #e3f2fd',
                    '헷지': 'background-color: #ffebee',
                    '위험': 'background-color: #f5f5f5',
                }
                return [colors.get(cls, '')] * len(row)

            styled_debug = (
                debug_df.style
                .format({
                    'market_value_krw': '₩{:,.0f}',
                    '위험비중': '{:.0%}',
                    '위험기여 (₩)': '₩{:,.0f}',
                })
                .apply(_row_color, axis=1)
            )
            st.dataframe(styled_debug, use_container_width=True, hide_index=True)
            st.caption(
                "**분류**: 🟢 안전 / 🟡 채권혼합 / 🔵 사용자지정 / 🔴 헷지 / ⚪ 위험.  \n"
                "**위험비중 조정**: master_data 의 `pension_class` 컬럼에  \n"
                "• `안전` → 0% / `채권혼합` → 30% (기본) / `위험` 또는 `헷지` → 100%  \n"
                "• 또는 **숫자 직접 입력** (예: `15` → 15%, `채권혼합20` → 채권혼합 20%)"
            )

    if pension_rows:
        df_pension = pd.DataFrame(pension_rows)

        # 위험/안전 셀에 색상 코딩
        def _color_risk(v):
            try:
                vv = float(v)
                if vv > 70:
                    return 'background-color: #ffebee; color: #b71c1c; font-weight: 600'
                if vv > 65:
                    return 'background-color: #fff8e1; color: #f57c00; font-weight: 600'
                return 'color: #1b5e20; font-weight: 600'
            except: return ''

        def _color_safe(v):
            try:
                vv = float(v)
                if vv < 30:
                    return 'background-color: #ffebee; color: #b71c1c; font-weight: 600'
                if vv < 35:
                    return 'background-color: #fff8e1; color: #f57c00; font-weight: 600'
                return 'color: #1b5e20; font-weight: 600'
            except: return ''

        styled_pension = (
            df_pension.style
            .format({
                '계좌 AUM': '₩{:,.0f}',
                '위험자산 (₩)': '₩{:,.0f}',
                '위험 %': '{:.1f}%',
                '안전자산 (₩)': '₩{:,.0f}',
                '안전 %': '{:.1f}%',
            })
            .map(_color_risk, subset=['위험 %'])
            .map(_color_safe, subset=['안전 %'])
        )

        st.dataframe(styled_pension, use_container_width=True, hide_index=True)
    else:
        st.info("퇴직연금 계좌 데이터 없음 (220914426167, 717190227129)")

# ---------------------------------------------------------
# [블록 6] 종목별 리밸런싱 표 (멘토 / HS 뷰에서만)
# ---------------------------------------------------------
if view in ("멘토 포폴", "HS 포폴"):
    st.divider()
    st.subheader("📋 종목별 리밸런싱 표")

    target_accounts = MENTOR_ACCS if view == "멘토 포폴" else HS_ACCS
    df_rb = df_rebal.copy()

    if df_rb.empty:
        st.warning(f"`rebalancing_data` 시트가 비어있습니다.")
    elif 'account' not in df_rb.columns:
        st.warning(f"`rebalancing_data` 시트에 'account' 컬럼이 없습니다.")
    else:
        # 현재 뷰의 계좌만 필터
        df_rb['_acc_clean'] = df_rb['account'].apply(clean_account)
        df_rb = df_rb[df_rb['_acc_clean'].isin(target_accounts)]

        # 현금 행 제외 (rebalancing 액션 없음)
        df_rb = df_rb[~df_rb['ticker'].astype(str).str.startswith('CASH')]

        # 숫자 컬럼 변환
        for col in ['market_value_krw', 'target_value_krw', 'rebalancing_value_krw',
                    'rebalancing_quantity', 'current_price_krw', 'quantity']:
            if col in df_rb.columns:
                df_rb[col] = to_num(df_rb[col])

        # 비중 컬럼은 percent 단위 (4.5 = 4.5%) 로 정규화
        for col in ['current_ratio', 'target_ratio']:
            if col in df_rb.columns:
                df_rb[col] = df_rb[col].apply(lambda v: parse_pct_value(v) or 0)

        if df_rb.empty:
            st.info("현재 뷰에 매매 필요한 종목이 없습니다.")
        else:
            # ---- 요약 메트릭 (테이블 위) ----
            buy_mask = df_rb['rebalancing_value_krw'] > 100   # 100원 이상이면 매수
            sell_mask = df_rb['rebalancing_value_krw'] < -100  # -100원 이하면 매도
            buy_total = df_rb.loc[buy_mask, 'rebalancing_value_krw'].sum()
            sell_total = abs(df_rb.loc[sell_mask, 'rebalancing_value_krw'].sum())

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("총 매수 금액", f"₩{buy_total:,.0f}", delta=f"{buy_mask.sum()}개 종목", delta_color="off")
            s2.metric("총 매도 금액", f"₩{sell_total:,.0f}", delta=f"{sell_mask.sum()}개 종목", delta_color="off")
            s3.metric("순 매매 금액", f"₩{(buy_total - sell_total):+,.0f}",
                      delta="매수가 매도보다 큼" if buy_total > sell_total else "매도가 매수보다 큼",
                      delta_color="off")
            s4.metric("총 종목 수", f"{len(df_rb)}개")

            # ---- [v75.x] 계좌별 Capacity 요약 — target 비중 균형 관점 ----
            # 계좌마다 target_ratio 합이 그 계좌의 AUM 비중 (W) 와 얼마나 차이나는지가 핵심.
            # 현금 추가 가정 안 함 (퇴직연금 등 입금 불가 계좌 포함).
            try:
                df_dv = df_dashboard[df_dashboard['account_clean'].isin(target_accounts)].copy()
                if not df_dv.empty and 'market_value_krw' in df_dv.columns:
                    df_dv['market_value_krw'] = to_num(df_dv['market_value_krw'])
                    group_aum = df_dv['market_value_krw'].sum()

                    is_cash = df_dv['ticker'].astype(str).str.startswith('CASH')
                    cash_per_acc = df_dv[is_cash].groupby('account_clean')['market_value_krw'].sum()
                    aum_per_acc = df_dv.groupby('account_clean')['market_value_krw'].sum()
                    target_sum_per_acc = df_rb.groupby('_acc_clean')['target_ratio'].sum()

                    cap_rows = []
                    for acc in sorted(target_accounts):
                        a_aum = aum_per_acc.get(acc, 0)
                        a_cash = cash_per_acc.get(acc, 0)
                        w_pct = (a_aum / group_aum * 100) if group_aum > 0 else 0  # 계좌 AUM 비중
                        cash_pct = (a_cash / group_aum * 100) if group_aum > 0 else 0
                        target_sum = float(target_sum_per_acc.get(acc, 0))  # 계좌별 target 합
                        diff = target_sum - w_pct  # +면 초과, -면 여유

                        if diff > 0.5:
                            status = '⚠️ 초과'
                        elif diff < -0.5:
                            status = '🟡 여유'
                        else:
                            status = '✅ 균형'

                        cap_rows.append({
                            '계좌': acc,
                            'AUM': a_aum,
                            '계좌 AUM %': w_pct,
                            '가용현금': a_cash,
                            '가용현금 %': cash_pct,
                            'target 합 %': target_sum,
                            '초과/여유 %p': diff,
                            '상태': status,
                        })
                    cap_df = pd.DataFrame(cap_rows)

                    expand_default = any(r['초과/여유 %p'] > 0.5 for r in cap_rows)
                    with st.expander("💼 계좌별 비중 균형 점검", expanded=expand_default):
                        def _color_status(v):
                            s = str(v)
                            if '초과' in s:
                                return 'background-color: #ffebee; color: #b71c1c; font-weight: 600'
                            if '여유' in s:
                                return 'background-color: #fff8e1; color: #e65100'
                            if '균형' in s:
                                return 'background-color: #e8f5e9; color: #1b5e20'
                            return ''

                        def _color_diff(v):
                            try:
                                vv = float(v)
                                if vv > 0.5:
                                    return 'color: #b71c1c; font-weight: 600'
                                elif vv < -0.5:
                                    return 'color: #e65100; font-weight: 600'
                            except: pass
                            return ''

                        styled_cap = (
                            cap_df.style
                            .format({
                                'AUM': '₩{:,.0f}',
                                '계좌 AUM %': '{:.1f}%',
                                '가용현금': '₩{:,.0f}',
                                '가용현금 %': '{:.1f}%',
                                'target 합 %': '{:.2f}%',
                                '초과/여유 %p': '{:+.2f}%p',
                            })
                            .map(_color_status, subset=['상태'])
                            .map(_color_diff, subset=['초과/여유 %p'])
                        )
                        st.dataframe(styled_cap, use_container_width=True, hide_index=True)
                        st.caption(
                            "💡 **초과/여유** = (그 계좌 종목들의 target_ratio 합) − (계좌 AUM %).  "
                            "**+ 양수 (⚠️ 초과)**: 이 계좌에 너무 많이 줬음 → target 줄이거나 종목을 다른 계좌로 재배정.  "
                            "**− 음수 (🟡 여유)**: 더 채울 수 있음.  "
                            "**0 근처 (✅ 균형)**: 적정 배분."
                        )
            except Exception as _e:
                st.caption(f"⚠️ 계좌별 capacity 계산 실패: {_e}")

            # ---- 필터 컨트롤 ----
            fc1, fc2 = st.columns([1, 2])
            with fc1:
                action_filter = st.radio(
                    "액션",
                    ["전체", "매수만", "매도만", "보유만"],
                    horizontal=True,
                    key=f"action_filter_{view}",
                )
            with fc2:
                unique_accs = sorted(df_rb['_acc_clean'].unique())
                acc_filter = st.multiselect(
                    "계좌 필터 (비우면 전체)",
                    options=unique_accs,
                    default=unique_accs,
                    key=f"acc_filter_{view}",
                )

            # 필터 적용
            df_show = df_rb.copy()
            if acc_filter:
                df_show = df_show[df_show['_acc_clean'].isin(acc_filter)]
            if action_filter == "매수만":
                df_show = df_show[df_show['rebalancing_value_krw'] > 100]
            elif action_filter == "매도만":
                df_show = df_show[df_show['rebalancing_value_krw'] < -100]
            elif action_filter == "보유만":
                df_show = df_show[df_show['rebalancing_value_krw'].abs() <= 100]

            if df_show.empty:
                st.info("필터 조건에 맞는 종목이 없습니다.")
            else:
                # ---- master_data 와 join 하여 theme/postion/country 가져옴 ----
                if not df_master.empty and 'ticker' in df_master.columns:
                    # country 컬럼명 통일 (소문자 우선, 없으면 대문자)
                    country_col = 'country' if 'country' in df_master.columns else (
                        'Country' if 'Country' in df_master.columns else None
                    )
                    extra_cols = [c for c in ['theme', 'postion'] if c in df_master.columns]
                    if country_col:
                        extra_cols.append(country_col)
                    if extra_cols:
                        df_show = df_show.merge(
                            df_master[['ticker'] + extra_cols].drop_duplicates(subset='ticker'),
                            on='ticker',
                            how='left',
                            suffixes=('', '_m'),
                        )
                        # 'Country' → 'country' 통일
                        if country_col == 'Country':
                            df_show['country'] = df_show['Country']

                # 누락 컬럼 안전 채움
                for col in ['theme', 'postion', 'country']:
                    if col not in df_show.columns:
                        df_show[col] = ''

                # 괴리율 (Drift) = 목표비중 - 현재비중 (퍼센트 포인트)
                df_show['drift'] = df_show['target_ratio'].fillna(0) - df_show['current_ratio'].fillna(0)

                # [v75.x] 매수 즉시성 체크: 이 row 의 매수가 그 계좌의 가용현금 안에서 즉시 가능한가?
                # cash_per_acc 는 위 capacity 섹션에서 계산됨 — 안전하게 fallback
                # 메시지 framing: "현금 부족" 이 아니라 "매도 후 가능" — 외부 자금 추가 가정 안 함
                try:
                    _cash_lookup = cash_per_acc.to_dict() if 'cash_per_acc' in dir() else {}
                except Exception:
                    _cash_lookup = {}

                def _capacity_status(r):
                    rb = float(r.get('rebalancing_value_krw') or 0)
                    if rb <= 100:
                        return '➖'  # 매도 또는 보유
                    acc = clean_account(r.get('account', ''))
                    cash = _cash_lookup.get(acc, 0)
                    if rb <= cash:
                        return '✅ 즉시'
                    need = rb - cash
                    return f'⚠️ 매도 후 (₩{need:,.0f})'

                df_show['cash_check'] = df_show.apply(_capacity_status, axis=1)

                # ---- 표시용 DataFrame 빌드 ----
                display = pd.DataFrame({
                    '계좌': df_show['account'],
                    '종목명': df_show['name'],
                    '테마': df_show['theme'].fillna('').astype(str),
                    '포지션': df_show['postion'].fillna('').astype(str),
                    '국가': df_show['country'].fillna('').astype(str),
                    '매매기준(현재가)': df_show['current_price_krw'],
                    '현재수량': df_show['quantity'],
                    '현재 평가액': df_show['market_value_krw'],
                    '현재비중': df_show['current_ratio'],
                    '목표비중': df_show['target_ratio'],
                    '괴리율': df_show['drift'],
                    '매매 필요수량': df_show['rebalancing_quantity'],
                    '리밸런싱 금액': df_show['rebalancing_value_krw'],
                    '목표 평가액': df_show['target_value_krw'],
                    '예수금 체크': df_show['cash_check'],
                })

                # 절대값 큰 순으로 정렬 (큰 매매가 위로)
                display = display.sort_values(
                    '리밸런싱 금액',
                    key=lambda s: s.abs(),
                    ascending=False,
                ).reset_index(drop=True)

                # ---- 색상 처리: 매수=초록, 매도=빨강, 보유=무색 ----
                def color_action(val):
                    try:
                        v = float(val)
                        if v > 100:
                            return 'background-color: #e8f5e9; color: #1b5e20; font-weight: 600'
                        elif v < -100:
                            return 'background-color: #ffebee; color: #b71c1c; font-weight: 600'
                    except:
                        pass
                    return ''

                # 괴리율 색칠: 양수(매수 압력)=연파랑, 음수(매도 압력)=연주황
                def color_drift(val):
                    try:
                        v = float(val)
                        if v > 0.5:
                            return 'background-color: #e3f2fd; color: #0d47a1'
                        elif v < -0.5:
                            return 'background-color: #fff3e0; color: #e65100'
                    except:
                        pass
                    return ''

                # 예수금 체크 컬럼 색칠: ⚠️ 빨강 / ✅ 초록 / ➖ 무색
                def color_cash_check(val):
                    s = str(val)
                    if s.startswith('⚠️'):
                        return 'background-color: #ffebee; color: #b71c1c; font-weight: 600'
                    if s.startswith('✅'):
                        return 'background-color: #e8f5e9; color: #1b5e20'
                    return ''

                styled = (
                    display.style
                    .format({
                        '매매기준(현재가)': '₩{:,.0f}',
                        '현재수량': '{:,.0f}',
                        '현재 평가액': '₩{:,.0f}',
                        '현재비중': '{:.2f}%',
                        '목표비중': '{:.2f}%',
                        '괴리율': '{:+.2f}%p',
                        '매매 필요수량': '{:+,.0f}',
                        '리밸런싱 금액': '₩{:+,.0f}',
                        '목표 평가액': '₩{:,.0f}',
                    })
                    .map(color_action, subset=['리밸런싱 금액', '매매 필요수량'])
                    .map(color_drift, subset=['괴리율'])
                    .map(color_cash_check, subset=['예수금 체크'])
                )

                st.dataframe(
                    styled,
                    use_container_width=True,
                    hide_index=True,
                    height=min(600, 50 + 35 * len(display)),
                )

                st.caption(
                    "💡 **정렬**: 리밸런싱 금액 절대값 큰 순. 헤더 클릭하면 다른 기준으로 정렬 가능.  "
                    "**색상**: 🟢 매수 / 🔴 매도 / ⚪ 보유 (차이 ±₩100 이내).  "
                    "**괴리율** = 목표비중 − 현재비중 (%p). 🔵 +면 매수 압력, 🟠 −면 매도 압력.  "
                    "**리밸런싱 금액** = 목표 평가액 − 현재 평가액 (양수=매수, 음수=매도).  "
                    "**예수금 체크**: ✅ 즉시 매수 가능 / ⚠️ 같은 계좌의 다른 종목 매도 후 가능 (필요 현금 표시) / ➖ 매도·보유."
                )

# ---------------------------------------------------------
st.divider()
st.caption(
    "📱 호섭님 포트폴리오 모바일 대시보드 — 블록 1~7 완성"
)

# ---------------------------------------------------------
# 디버그 패널 (값이 "-" 로 나올 때 펼쳐서 확인)
# ---------------------------------------------------------
with st.expander("🐛 디버그 정보 (값이 비어 있으면 펼쳐보기)"):
    st.markdown("**df_perf 정보**")
    st.write(f"- shape: {df_perf.shape}")
    st.write(f"- 첫 5개 컬럼: {df_perf.columns[:5].tolist() if not df_perf.empty else '(비어있음)'}")
    st.write(f"- 찾는 perf_label: `{perf_label}`")
    if '상세' in df_perf.columns:
        st.write(f"- '상세' 컬럼의 unique 값: {df_perf['상세'].dropna().unique().tolist()[:15]}")

    st.markdown("**perf_row 결과**")
    if perf_row is None:
        st.error("perf_row 못 찾음 → '상세' 컬럼에서 매칭 실패")
    else:
        st.success("perf_row 찾음 ✓")
        # 단기 컬럼 raw 값 확인
        debug_cols = ['1일', 'WTD(이번주)', 'YTD', '누적수익률(%)', 'MWR_지정(25-05-14~)']
        debug_data = []
        for c in debug_cols:
            if c in perf_row.index:
                raw_val = perf_row[c]
                parsed = get_perf(c)
                debug_data.append({
                    '컬럼': c,
                    '원본값': repr(raw_val),
                    '파싱결과(%)': parsed,
                    '표시': fmt_pct(parsed) if parsed is not None else '-'
                })
            else:
                debug_data.append({'컬럼': c, '원본값': '(컬럼없음)', '파싱결과(%)': None, '표시': '-'})
        st.dataframe(pd.DataFrame(debug_data), use_container_width=True)

    st.markdown("**df_dashboard 정보**")
    st.write(f"- shape: {df_dashboard.shape}")
    st.write(f"- df_view shape (현재 뷰 필터링 후): {df_view.shape}")
    st.write(f"- 총자산 합계 (raw): ₩{total_assets:,.0f}")