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
from datetime import datetime

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
        data = yf.download(yf_tickers, period='2d', interval='1d',
                           progress=False, auto_adjust=False)
        for t in yf_tickers:
            try:
                if len(yf_tickers) == 1:
                    closes = data['Close'].dropna()
                else:
                    closes = data[('Close', t)].dropna()
                if len(closes) >= 2:
                    results[t] = {
                        'current': float(closes.iloc[-1]),
                        'prev_close': float(closes.iloc[-2]),
                    }
                elif len(closes) == 1:
                    # 어제 데이터만 있고 오늘 데이터 없음
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
        rate_data = yf.download(f"{currency_code}KRW=X", period='2d',
                                interval='1d', progress=False, auto_adjust=False)
        if not rate_data.empty:
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
        ["전체", "멘토 포폴", "HS 포폴", "💼 장중 실시간", "📰 시장 동향"],
        index=0,
    )

    st.markdown("---")

    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"마지막 로드: {datetime.now().strftime('%H:%M:%S')}")
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
# [시장 동향 뷰] — market_data 시트 기반
# 매일 아침 시장 체크용: 카드 (어제 마감) + 선 차트 (추세)
# =========================================================
if view == "📰 시장 동향":
    st.title("📰 시장 동향")
    st.caption(
        "매일 아침 시장 체크용 — 한국/미국/중국/독일 시장, 환율, 금리, 원자재, 크립토, 변동성, 자금흐름"
    )

    # market_data 시트 로드
    try:
        df_market = load_sheet("market_data")
    except Exception as e:
        st.error(f"market_data 시트 로드 실패: {e}")
        st.stop()

    if df_market.empty:
        st.warning("market_data 시트가 비어있습니다.")
        st.stop()

    # date 컬럼 파싱 — 한국 locale ("2020. 01. 01 (수)") 도 처리
    date_col_name = df_market.columns[0]  # 첫 컬럼이 date
    _date_raw = df_market[date_col_name].astype(str).str.strip()
    # 1) 괄호 포함 요일 제거: "2020. 01. 01 (수)" → "2020. 01. 01"
    # 2) 점/공백/슬래시 → 하이픈
    # 3) 양 끝 하이픈 정리
    _date_norm = (_date_raw
                  .str.replace(r'\s*\([^)]*\)\s*', '', regex=True)
                  .str.replace(r'[\.\s/]+', '-', regex=True)
                  .str.strip('-'))
    df_market['_date'] = pd.to_datetime(_date_norm, errors='coerce')
    df_market = df_market.dropna(subset=['_date']).sort_values('_date').reset_index(drop=True)

    if df_market.empty:
        st.warning("유효한 날짜 데이터가 없습니다.")
        st.stop()

    # 숫자 컬럼 변환
    for col in df_market.columns:
        if col not in (date_col_name, '_date'):
            df_market[col] = pd.to_numeric(
                df_market[col].astype(str).str.replace(r'[^\d.\-]', '', regex=True),
                errors='coerce'
            )

    latest_market = df_market.iloc[-1]
    latest_market_date = latest_market['_date']

    st.caption(f"📅 기준일: **{latest_market_date.strftime('%Y-%m-%d')}** "
               f"(market_data 시트 최신 행)")

    # ── 시장 그룹 매핑 ──
    MARKET_GROUPS = [
        ("🇰🇷 한국", [
            ('KOSPI', 'KOSPI_price', 'KOSPI_chg_pct'),
            ('KOSDAQ', 'KOSDAQ_price', 'KOSDAQ_chg_pct'),
        ]),
        ("🇺🇸 미국", [
            ('S&P 500', 'SP500_price', 'SP500_chg_pct'),
            ('NASDAQ', 'NASDAQ_price', 'NASDAQ_chg_pct'),
        ]),
        ("🌏 중국 / 일본 / 독일", [
            ('Shanghai', 'SHANGHAI_price', 'SHANGHAI_chg_pct'),
            ('Nikkei', 'NIKKEI_price', 'NIKKEI_chg_pct'),
            ('DAX', 'DAX_price', 'DAX_chg_pct'),
        ]),
        ("💱 환율 / 변동성", [
            ('USD/KRW', 'USDKRW_price', 'USDKRW_chg_pct'),
            ('USD Index', 'USD_IDX_price', 'USD_IDX_chg_pct'),
            ('VIX', 'VIX_price', 'VIX_chg_pct'),
        ]),
        ("📈 채권 금리", [
            ('US 10Y', 'US_10Y_Bond_rate', 'US_10Y_Bond_chg_bps'),
            ('US 30Y', 'US_30Y_Bond_rate', 'US_30Y_Bond_chg_bps'),
            ('KR 10Y', 'KR_10Y_Bond_rate', 'KR_10Y_Bond_chg_bps'),
        ]),
        ("🛢️ 원자재 / ₿ 크립토", [
            ('WTI', 'WTI_price', 'WTI_chg_pct'),
            ('GOLD', 'GOLD_price', 'GOLD_chg_pct'),
            ('BTC', 'BTC_price', 'BTC_chg_pct'),
        ]),
        ("💰 한국 자금 흐름", [
            ('고객예탁금 (억원)', 'Customer_Deposit_value', 'Customer_Deposit_chg_pct'),
            ('신용잔고 (억원)', 'Credit_Balance_value', 'Credit_Balance_chg_pct'),
        ]),
    ]

    def _fmt_market_value(val, col_name):
        if val is None or pd.isna(val):
            return "-"
        if 'rate' in col_name:
            return f"{val:.3f}"
        if abs(val) >= 10000:
            return f"{val:,.0f}"
        return f"{val:,.2f}"

    def _fmt_market_change(val, col_name):
        if val is None or pd.isna(val):
            return None
        if 'bps' in col_name:
            return f"{val:+.1f} bps"
        # _chg_pct: 시트 정규화 후 항상 퍼센트 형태 (예: -1.38)
        # 자동감지(<=1.5) 는 -1.38 같은 값을 소수로 오인하는 버그가 있어서 제거.
        return f"{val:+.2f}%"

    # ── 카드 섹션 ──
    st.subheader("📊 오늘 시장 카드")

    for group_name, items in MARKET_GROUPS:
        st.markdown(f"**{group_name}**")
        cols = st.columns(len(items))
        for col_st, (label, val_col, chg_col) in zip(cols, items):
            val = latest_market.get(val_col) if val_col in df_market.columns else None
            chg = latest_market.get(chg_col) if chg_col in df_market.columns else None
            val_str = _fmt_market_value(val, val_col)
            chg_str = _fmt_market_change(chg, chg_col)
            col_st.metric(label, val_str, delta=chg_str)

    # ── 선 차트 섹션 ──
    st.divider()
    st.subheader("📈 추세 비교")

    # 차트 컨트롤
    cc1, cc2, cc3 = st.columns([2, 1, 1])
    all_indices = [item[0] for _, items in MARKET_GROUPS for item in items]
    label_to_col = {item[0]: item[1] for _, items in MARKET_GROUPS for item in items}

    with cc1:
        default_indices = [
            i for i in ['KOSPI', 'NASDAQ', 'USD/KRW']
            if i in all_indices and label_to_col[i] in df_market.columns
        ]
        selected_indices = st.multiselect(
            "지표 선택 (여러 개 비교 가능)",
            options=all_indices,
            default=default_indices,
        )
    with cc2:
        time_range = st.selectbox(
            "기간",
            ["WTD", "MTD", "QTD", "YTD", "1주", "1달", "3달", "6달", "1년", "전체"],
            index=6,  # 기본 '3달'
        )
    with cc3:
        normalize = st.toggle(
            "정규화",
            value=True,
            help="여러 지표를 같은 스케일로 비교 (시작점=100)",
        )

    # 시간 범위 적용
    if time_range == "전체":
        df_chart = df_market.copy()
    elif time_range == "WTD":
        # 이번주 월요일부터 (월=0, 일=6)
        days_since_mon = latest_market_date.weekday()
        cutoff = latest_market_date - pd.Timedelta(days=days_since_mon)
        df_chart = df_market[df_market['_date'] >= cutoff]
    elif time_range == "MTD":
        # 이번 달 1일부터
        cutoff = pd.Timestamp(year=latest_market_date.year,
                              month=latest_market_date.month, day=1)
        df_chart = df_market[df_market['_date'] >= cutoff]
    elif time_range == "QTD":
        # 이번 분기 첫 달 1일부터 (Q1=1월, Q2=4월, Q3=7월, Q4=10월)
        q_start_month = ((latest_market_date.month - 1) // 3) * 3 + 1
        cutoff = pd.Timestamp(year=latest_market_date.year,
                              month=q_start_month, day=1)
        df_chart = df_market[df_market['_date'] >= cutoff]
    elif time_range == "YTD":
        # 올해 1월 1일부터
        cutoff = pd.Timestamp(year=latest_market_date.year, month=1, day=1)
        df_chart = df_market[df_market['_date'] >= cutoff]
    else:
        days_map = {"1주": 7, "1달": 30, "3달": 90, "6달": 180, "1년": 365}
        cutoff = latest_market_date - pd.Timedelta(days=days_map[time_range])
        df_chart = df_market[df_market['_date'] >= cutoff]

    if df_chart.empty or not selected_indices:
        st.info("기간 내 데이터 없음 또는 지표 미선택")
    else:
        # 데이터가 너무 적으면 안내
        if len(df_chart) < 5:
            st.info(
                f"📌 '{time_range}' 기간의 거래일 데이터가 **{len(df_chart)}개** 뿐입니다. "
                f"월/분기/년 초반에 흔한 현상 — 더 긴 추세를 보려면 1달/3달/1년 옵션을 사용해보세요."
            )
        fig_market = go.Figure()
        for idx_label in selected_indices:
            col = label_to_col.get(idx_label)
            if not col or col not in df_chart.columns:
                continue
            series = df_chart[col].copy()
            if normalize:
                first_valid = series.dropna()
                if not first_valid.empty and first_valid.iloc[0] != 0:
                    series = series / first_valid.iloc[0] * 100
            fig_market.add_trace(go.Scatter(
                x=df_chart['_date'],
                y=series,
                name=idx_label,
                mode='lines',
                line=dict(width=2),
                hovertemplate=f'<b>{idx_label}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:,.2f}}<extra></extra>',
            ))

        title_text = (
            f"{time_range} 추세"
            + (" (정규화: 시작=100)" if normalize else " (raw 값)")
        )
        fig_market.update_layout(
            title=dict(text=title_text, font=dict(size=15)),
            height=460,
            margin=dict(l=40, r=20, t=60, b=40),
            font=dict(size=13, family='sans-serif'),
            yaxis=dict(
                title=dict(
                    text="지수 (시작=100)" if normalize else "값",
                    font=dict(size=13),
                ),
                tickfont=dict(size=11),
                gridcolor='#eeeeee',
            ),
            xaxis=dict(
                tickfont=dict(size=11),
                tickformat='%Y-%m-%d',  # 항상 날짜 형식 (시간 단위 자동스케일 방지)
                type='date',
            ),
            legend=dict(
                orientation='h', yanchor='top', y=1.10,
                xanchor='center', x=0.5, font=dict(size=12),
            ),
            plot_bgcolor='white',
            hovermode='x unified',
        )
        st.plotly_chart(fig_market, use_container_width=True)

        # 정규화 설명
        if normalize:
            st.caption(
                "💡 **정규화 모드**: 각 지표의 *기간 시작 시점 값*을 100 으로 맞춰서 표시. "
                "예: KOSPI 가 3000 → 3300 가면 100 → 110 으로 그려짐. "
                "절대값이 다른 지표 (KOSPI 3000 vs USD/KRW 1370) 도 같은 스케일에서 비교 가능."
            )
        else:
            st.caption(
                "💡 **raw 모드**: 각 지표를 실제 값 그대로 그림. "
                "값 단위가 비슷한 지표끼리 비교할 때 유용."
            )

    st.stop()  # 시장 동향 뷰는 여기서 종료

# =========================================================
# [장중 실시간 뷰] — 별도 흐름, st.stop() 으로 종료
# 한국 주식: 네이버 모바일 API
# 외국 주식: yfinance batch + 현재 환율 → KRW 환산
# =========================================================
if view == "💼 장중 실시간":
    st.title("💼 장중 실시간 수익률")
    st.caption(
        f"갱신 시각: {datetime.now().strftime('%H:%M:%S KST')} · "
        "최신 가격 보려면 페이지 새로고침 또는 사이드바 *🔄 데이터 새로고침* 클릭"
    )

    # 계좌 선택
    accounts_avail = sorted([a for a in df_dashboard['account_clean'].unique() if a])
    if not accounts_avail:
        st.warning("계좌 데이터가 없습니다.")
        st.stop()

    default_idx = (accounts_avail.index('220914426167')
                   if '220914426167' in accounts_avail else 0)
    selected_acc = st.selectbox(
        "계좌 선택",
        accounts_avail,
        index=default_idx,
        help="장중 실시간 가격으로 오늘 손익을 계산합니다.",
    )

    # 해당 계좌의 보유 종목 (현금 제외, 수량 > 0)
    sub_rt = df_dashboard[df_dashboard['account_clean'] == selected_acc].copy()
    sub_rt = sub_rt[~sub_rt['ticker'].astype(str).str.startswith('CASH')]
    sub_rt = sub_rt[sub_rt['quantity'].abs() > 0]

    if sub_rt.empty:
        st.info("이 계좌에 보유 종목이 없습니다 (현금만 있거나 데이터 없음).")
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

    kr_rows = []      # [(ticker, name, qty, avg_cost), ...]
    foreign_rows = [] # [(ticker, name, qty, avg_cost, exchange, yf_ticker), ...]
    for _, r in sub_rt.iterrows():
        tk = str(r['ticker']).strip()
        nm = str(r.get('name', tk))
        qty = float(r['quantity'])
        avg_cost = float(r.get('avg_cost_krw', 0) or 0)
        ex = str(r.get('exchange', '')).upper()

        if _is_kr_ticker(tk, ex):
            kr_rows.append((tk, nm, qty, avg_cost))
        else:
            yf_t = _build_yf_ticker(tk, ex)
            foreign_rows.append((tk, nm, qty, avg_cost, ex, yf_t))

    # 실시간 가격 조회
    with st.spinner(f"실시간 가격 조회 중... (한국 {len(kr_rows)}개 / 외국 {len(foreign_rows)}개)"):
        # 한국: 네이버 (sequential) — kr_rows: (ticker, name, qty, avg_cost)
        kr_prices = {}
        for tk, _, _, _ in kr_rows:
            r = get_naver_intraday(tk)
            if r:
                kr_prices[tk] = r

        # 외국: yfinance batch — foreign_rows: (ticker, name, qty, avg_cost, exchange, yf_ticker)
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

    def _build_row(tk, nm, qty, avg_cost, prev_orig, curr_orig, fx):
        prev_krw = prev_orig * fx
        curr_krw = curr_orig * fx
        change_pct = ((curr_orig - prev_orig) / prev_orig * 100) if prev_orig > 0 else 0
        today_pl = (curr_krw - prev_krw) * qty
        cost_total = avg_cost * qty
        current_value = curr_krw * qty
        cumulative_pl = current_value - cost_total
        # 컬럼 순서: 식별 → 매입/평가/누적 (4개 묶음) → 오늘 변동 (4개)
        return {
            'Ticker': tk,
            '종목명': nm,
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
        }

    def _build_failed_row(tk, nm, qty, avg_cost):
        cost_total = avg_cost * qty
        return {
            'Ticker': tk, '종목명': nm, '수량': _qty_fmt(qty),
            '매입가': avg_cost, '매입금액': cost_total,
            '현재 평가액': 0, '누적 손익': 0,
            '전일종가': 0, '현재가': 0,
            '변동률': 0, '오늘 손익': 0,
            '_ok': False,
        }

    for tk, nm, qty, avg_cost in kr_rows:
        r = kr_prices.get(tk)
        if r and r.get('prev_close', 0) > 0:
            result_rows.append(_build_row(tk, nm, qty, avg_cost, r['prev_close'], r['current'], 1.0))
        else:
            result_rows.append(_build_failed_row(tk, nm, qty, avg_cost))

    for tk, nm, qty, avg_cost, ex, yf_t in foreign_rows:
        r = foreign_yf_data.get(yf_t)
        if r and r.get('prev_close', 0) > 0:
            ccy = EXCHANGE_TO_CURRENCY.get(ex, 'USD')
            fx = fx_rates.get(ccy, 1.0)
            result_rows.append(_build_row(tk, nm, qty, avg_cost, r['prev_close'], r['current'], fx))
        else:
            result_rows.append(_build_failed_row(tk, nm, qty, avg_cost))

    df_rt = pd.DataFrame(result_rows).sort_values('변동률', ascending=False)

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

    display_rt = df_rt.drop(columns=['_ok'])
    styled_rt = display_rt.style.format({
        '매입가': '₩{:,.0f}',
        '매입금액': '₩{:,.0f}',
        '전일종가': '₩{:,.0f}',
        '현재가': '₩{:,.0f}',
        '변동률': '{:+.2f}%',
        '오늘 손익': '₩{:+,.0f}',
        '현재 평가액': '₩{:,.0f}',
        '누적 손익': '₩{:+,.0f}',
    }).map(_color_change_rt, subset=['변동률', '오늘 손익', '누적 손익'])

    st.dataframe(styled_rt, use_container_width=True, hide_index=True,
                 height=min(600, 60 + 38 * len(display_rt)))

    failed = df_rt[~df_rt['_ok']]
    if not failed.empty:
        st.warning(
            f"⚠️ 가격 조회 실패 {len(failed)}개: "
            + ', '.join(failed['Ticker'].astype(str).head(10).tolist())
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
# [Hero 보조] 계좌별 자산 분포 (멘토 / HS 뷰만)
# ---------------------------------------------------------
if view in ("멘토 포폴", "HS 포폴"):
    target_accs_for_breakdown = MENTOR_ACCS if view == "멘토 포폴" else HS_ACCS

    def _classify_region(row):
        """국내 vs 해외 분류 — '진짜 해외 계좌 잔고' 의 의미.
        - 국내 = KRW 로 보유 (한국 ETF, 한국주식, KRW 예수금 — 한국 ETF 가 미국 추종해도 국내로 분류)
        - 해외 = 외화로 보유 (직접 매수한 외국주, 외화 예수금)
        """
        ticker = str(row.get('ticker', ''))
        # 현금: ticker 접두사로 판단
        if ticker.startswith('CASH_KRW'):
            return '국내'
        if ticker.startswith('CASH_FX'):
            return '해외'
        # 일반 종목: 통화로 판단
        currency = str(row.get('currency', '')).strip().upper()
        if currency == 'KRW':
            return '국내'
        if currency in ('', 'NAN', 'NONE'):
            return '미분류'
        return '해외'  # USD, HKD, CNY, JPY 등

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
            '국내 (KRW)': domestic,
            '해외 (외화→KRW)': foreign,
            '미분류': unclassified,
            '합계': total,
        })

    if breakdown_rows:
        df_breakdown = pd.DataFrame(breakdown_rows)
        # 합계 행 추가
        total_row = pd.DataFrame([{
            '계좌': '**합계**',
            '국내 (KRW)': df_breakdown['국내 (KRW)'].sum(),
            '해외 (외화→KRW)': df_breakdown['해외 (외화→KRW)'].sum(),
            '미분류': df_breakdown['미분류'].sum(),
            '합계': df_breakdown['합계'].sum(),
        }])
        df_breakdown = pd.concat([df_breakdown, total_row], ignore_index=True)

        # 미분류가 0이면 컬럼 숨김
        show_cols = ['계좌', '국내 (KRW)', '해외 (외화→KRW)', '합계']
        if df_breakdown['미분류'].sum() > 0:
            show_cols = ['계좌', '국내 (KRW)', '해외 (외화→KRW)', '미분류', '합계']

        st.markdown(
            "**📊 계좌별 자산 분포** "
            "(국내 = KRW 보유분 / 해외 = 외화 보유분, KRW 환산. "
            "한국 계좌에서 산 미국 추종 ETF 는 국내로 잡힘)"
        )
        st.dataframe(
            df_breakdown[show_cols].style.format({
                '국내 (KRW)': '₩{:,.0f}',
                '해외 (외화→KRW)': '₩{:,.0f}',
                '미분류': '₩{:,.0f}',
                '합계': '₩{:,.0f}',
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
    _kpi_year = str(datetime.now().year)

st.subheader(f"🎯 {_kpi_year} 연간 KPI 진행")

KPI_MIN_PCT = 10.0
KPI_AVG_PCT = 15.0
KPI_MAX_PCT = 20.0

ytd_twr = get_perf_pct('YTD')

if ytd_twr is None:
    st.info("YTD 수익률 데이터 없음")
else:
    # 4 메트릭 카드
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("현재 YTD (TWR)", f"{ytd_twr:+.2f}%")
    with k2:
        diff = ytd_twr - KPI_MIN_PCT
        st.metric("최소 목표 10%", f"{diff:+.2f}p",
                  delta="달성 ✓" if diff >= 0 else "미달성",
                  delta_color="off")
    with k3:
        diff = ytd_twr - KPI_AVG_PCT
        st.metric("평균 목표 15%", f"{diff:+.2f}p",
                  delta="달성 ✓" if diff >= 0 else "미달성",
                  delta_color="off")
    with k4:
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
        # 올해 누적 손익 (월별 합)
        ytd_pl_kpi = sum(
            (get_perf_raw(f'손익_{m}') or 0)
            for m in month_cols_kpi if m.startswith(_kpi_year)
        )
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

# ---------------------------------------------------------
# [블록 2] 비중 도넛 3개
# ---------------------------------------------------------
st.subheader("🥧 비중")

def _prep_for_pie(df, group_col, cash_label='현금', hedge_label='헷지', detect_hedge=False):
    """cash row 와 (옵션) hedge row 의 group_col 값을 명확한 라벨로 변경.
    - cash 는 ticker.startswith('CASH') 로 감지 → '현금' 라벨
    - hedge 는 theme='헷지' / postion='방위군' / military='방위군' 중 하나로 감지 (detect_hedge=True 시)
      → '헷지' 라벨. 국가별 pie 에서 인버스 ETF 등을 한국/미국 에서 분리해 별도 표시할 때 사용.
    """
    if group_col not in df.columns or df.empty:
        return df
    df = df.copy()
    # 1) Cash 라벨링
    mask_cash = df['ticker'].astype(str).str.startswith('CASH')
    df.loc[mask_cash, group_col] = cash_label
    # 2) Hedge 라벨링 (요청된 경우만)
    if detect_hedge:
        mask_hedge = pd.Series(False, index=df.index)
        if 'theme' in df.columns:
            mask_hedge |= df['theme'].astype(str).str.strip() == '헷지'
        if 'postion' in df.columns:
            mask_hedge |= df['postion'].astype(str).str.strip() == '방위군'
        if 'military' in df.columns:
            mask_hedge |= df['military'].astype(str).str.strip() == '방위군'
        # cash 우선 (cash 면 '현금' 유지, hedge 로 덮어쓰지 않음)
        mask_hedge = mask_hedge & ~mask_cash
        df.loc[mask_hedge, group_col] = hedge_label
    return df

def make_pie(df, group_col, title):
    if group_col not in df.columns or df.empty:
        return None
    grouped = (
        df.groupby(group_col, dropna=False)['market_value_krw']
        .sum()
        .reset_index()
    )
    grouped = grouped[grouped['market_value_krw'] > 0]
    grouped[group_col] = grouped[group_col].astype(str).replace('', '미분류')
    if grouped.empty:
        return None
    fig = px.pie(grouped, values='market_value_krw', names=group_col, hole=0.5)
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

pc1, pc2, pc3 = st.columns(3)
with pc1:
    # cash row → '현금', 헷지 ETF → '헷지' (theme/postion/military 로 자동 감지)
    fig = make_pie(_prep_for_pie(df_view, 'country', detect_hedge=True), 'country', '국가별')
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info("국가별 데이터 없음")

with pc2:
    fig = make_pie(df_view, 'theme', '테마별')
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info("테마별 데이터 없음")

with pc3:
    # 군종별: cash row → '현금'. 그룹별 (전체뷰) 는 cash 변환 안 함
    if third_pie_col == 'group_name':
        fig = make_pie(df_view, third_pie_col, third_pie_title)
    else:
        fig = make_pie(_prep_for_pie(df_view, third_pie_col), third_pie_col, third_pie_title)
    if fig: st.plotly_chart(fig, use_container_width=True)
    else: st.info(f"{third_pie_title} 데이터 없음")

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

    # 5/14~ (멘토 inception)
    pl_5_14 = get_perf_raw('지정_손익(25-05-14~)')
    if pl_5_14 is not None:
        pl_labels.append('5/14~')
        pl_values.append(pl_5_14)

    # 7/21~ (HS inception)
    pl_7_21 = get_perf_raw('지정_손익(25-07-21~)')
    if pl_7_21 is not None:
        pl_labels.append('7/21~')
        pl_values.append(pl_7_21)

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
# [블록 4] 벤치마크 비교
# ---------------------------------------------------------
st.subheader("🆚 벤치마크 비교")

BM_LABELS = ['2026 MAIN BM', 'KOSPI', 'KOSDAQ', 'Shanghai', 'S&P 500', 'NASDAQ']

# 비교 가능한 기간 후보 (df_perf 컬럼에 있는 것만 노출)
period_candidates = [
    '1일', 'WTD(이번주)', 'W-1(저번주)', 'MTD(이번달)', 'M-1(지난달)', 'M-2(2달전)',
    'YTD', '누적수익률(%)', '지정(25-05-14~)', '지정(25-07-21~)',
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
# [블록 5.5] 퇴직연금 가드 (HS 뷰 전용)
# 220914426167, 717190227129 의 위험/안전 비중을 실시간 dashboard_data 로 계산.
# 호섭님 시트 가드와 동일 로직, 폰에서도 한눈에 확인 가능.
# ---------------------------------------------------------
PENSION_ACCS = ['220914426167', '717190227129']

if view == "HS 포폴":
    st.divider()
    st.subheader("🛡️ 퇴직연금 가드")
    st.caption(
        "**규제**: 위험자산 ≤ 70% / 안전자산 ≥ 30% (계좌 단위). "
        "분류: 위험 = 채권혼합·안전자산·방위군 외 모든 종목 / 안전 = 채권혼합 + 안전자산 (현금 포함)"
    )

    pension_rows = []
    for acc in PENSION_ACCS:
        sub = df_view[df_view['account_clean'] == acc]
        if sub.empty:
            continue

        themes = sub.get('theme', pd.Series([''] * len(sub))).astype(str).str.strip()
        positions = sub.get('postion', pd.Series([''] * len(sub))).astype(str).str.strip()

        is_safe = (themes == '채권혼합') | (themes == '안전 자산')
        is_hedge = (positions == '방위군')
        is_risk = ~is_safe & ~is_hedge

        risk_mv = sub.loc[is_risk, 'market_value_krw'].sum()
        safe_mv = sub.loc[is_safe, 'market_value_krw'].sum()
        hedge_mv = sub.loc[is_hedge, 'market_value_krw'].sum()
        total_mv = sub['market_value_krw'].sum()

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
                # ---- 표시용 DataFrame 빌드 ----
                display = pd.DataFrame({
                    '계좌': df_show['account'],
                    'Ticker': df_show['ticker'],
                    '종목명': df_show['name'],
                    '매매기준(현재가)': df_show['current_price_krw'],
                    '현재수량': df_show['quantity'],
                    '현재 평가액': df_show['market_value_krw'],
                    '현재비중': df_show['current_ratio'],
                    '목표비중': df_show['target_ratio'],
                    '목표 평가액': df_show['target_value_krw'],
                    '매매 필요수량': df_show['rebalancing_quantity'],
                    '리밸런싱 금액': df_show['rebalancing_value_krw'],
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

                styled = (
                    display.style
                    .format({
                        '매매기준(현재가)': '₩{:,.0f}',
                        '현재수량': '{:,.0f}',
                        '현재 평가액': '₩{:,.0f}',
                        '현재비중': '{:.2f}%',
                        '목표비중': '{:.2f}%',
                        '목표 평가액': '₩{:,.0f}',
                        '매매 필요수량': '{:+,.0f}',
                        '리밸런싱 금액': '₩{:+,.0f}',
                    })
                    .map(color_action, subset=['리밸런싱 금액', '매매 필요수량'])
                )

                st.dataframe(
                    styled,
                    use_container_width=True,
                    hide_index=True,
                    height=min(600, 50 + 35 * len(display)),
                )

                st.caption(
                    "💡 **정렬**: 리밸런싱 금액 절대값 큰 순. 헤더 클릭하면 다른 기준으로 정렬 가능.  "
                    "**색상**: 🟢 매수 (target > current) / 🔴 매도 (target < current) / ⚪ 보유 (차이 ±₩100 이내).  "
                    "**리밸런싱 금액 = 목표 평가액 − 현재 평가액** (양수=매수, 음수=매도)"
                )

# ---------------------------------------------------------
# [블록 7] 월별 / 분기별 수익률 (모든 뷰 공통, 가장 하단)
# performance_summary 시트에 자동 생성된 월/분기 컬럼 surface
# ---------------------------------------------------------
st.divider()
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