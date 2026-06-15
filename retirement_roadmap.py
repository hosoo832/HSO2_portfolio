# retirement_roadmap.py — 호섭님 은퇴 로드맵 뷰
#
# Dashboard.py 의 "🎯 은퇴 로드맵" 뷰가 호출하는 독립 모듈.
# 목표 은퇴자산까지 매년 필요한 수익률 + 올해 YTD 실제 페이스로 얼마나 잘하고 있는지 비교.
# 전세 유지 vs 반전세 전환 두 버전을 토글/표로 동시 비교.
#
# ── 핵심 모델 (실질 = 오늘 구매력 기준, 모든 금액 '억원') ──
#   - 거주 버킷: 전세/반전세 보증금 → 미래 과천 아파트. 실질 불변(물가만큼만 상승) 가정.
#   - 투자 버킷: 주식 + 경매(선택) + 반전세 전환차액. 매년 실질수익률로 복리 + 추가납입.
#   - 목표 총자산(오늘) = 과천 아파트값 + (생활비 + 여행비) / 안전인출률(SWR)
#   - 명목수익률 → 실질수익률: (1+명목)/(1+물가) - 1
#   - 필요수익률 = 투자버킷이 목표 도달에 매년 내야 하는 명목 CAGR (전세는 안 굴러감)
#
# Dashboard.py 와의 결합: render(df_dashboard, df_perf, now_kst) 한 함수만 노출.
# df 인자는 optional — 없으면 수동입력 기본값으로 동작(독립 실행/테스트 가능).

from datetime import date, datetime

import plotly.graph_objects as go
import streamlit as st

# ── 호섭님 기준 기본값 (2026-06) ──
DEFAULTS = dict(
    birth_year=1986,    # 86년생
    jeonse=8.1,         # 현재 전세보증금 (억)
    auction=3.5,        # 경매수익 (와이프, 11월 매도 예상)
    half_deposit=4.0,   # 반전세 보증금
    half_rent_m=260,    # 반전세 월세 (만원/월)
    home=22.0,          # 목표 과천 84㎡ (보수, 구축~중간)
    living_m=450,       # 생활비 (만원/월)
    travel_y=1750,      # 여행비 (만원/년, 1500~2000 중간값)
    swr=4.0,            # 안전인출률 %
    infl=2.5,           # 물가상승률 %
    exp_return=12.0,    # 차트 시뮬레이션 수익률 (명목) %
    contrib=0.0,        # 연 추가납입 (억)
    jan1_jeonse=8.1,    # 올해 연초(1/1) 전세보증금
    jan1_stock=3.0,     # 올해 연초 주식자산
    jan1_auction=2.0,   # 올해 연초 경매
)
INCEPTION = date(2025, 5, 14)  # 실적 TWR 연환산 기준일 (멘토 2기 시작)
KPI_MIN, KPI_AVG, KPI_MAX = 10.0, 15.0, 20.0  # 호섭 연수익률 목표 (명목 %)


# =========================================================
# 순수 계산 함수 (streamlit 무관 — 단위테스트 가능)
# =========================================================
def real_rate(nom_pct, infl_pct):
    """명목 연수익률(%) → 실질 연수익률(소수)."""
    nom, infl = nom_pct / 100.0, infl_pct / 100.0
    return (1 + nom) / (1 + infl) - 1


def project_path(invest0, resid0, r_real, years, contrib=0.0, outflow=0.0):
    """연도별 총자산(오늘가치) 궤적. 길이 years+1.
    invest0: 투자버킷 시작액 / resid0: 거주버킷(실질 불변)
    contrib: 연 추가납입 / outflow: 연 유출(예: 월세를 투자에서 인출)."""
    path, inv = [], invest0
    for _ in range(int(years) + 1):
        path.append(resid0 + inv)
        inv = inv * (1 + r_real) + contrib - outflow
    return path


def required_cagr(target, invest0, resid0, years, contrib=0.0, outflow=0.0):
    """목표도달에 필요한 실질 CAGR(소수)을 이분법으로 역산 (투자버킷 기준)."""
    if years is None or years <= 0:
        return float("nan")
    lo, hi = -0.9, 2.0
    for _ in range(200):
        mid = (lo + hi) / 2
        end = project_path(invest0, resid0, mid, years, contrib, outflow)[-1]
        if end < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def need_nominal(target, invest0, resid0, years, infl_pct, contrib=0.0, outflow=0.0):
    """목표도달에 필요한 명목 연수익률(소수). years<=0 이면 None."""
    if years is None or years <= 0:
        return None
    rr = required_cagr(target, invest0, resid0, years, contrib, outflow)
    return (1 + rr) * (1 + infl_pct / 100) - 1


def reach_age(path, target, start_age):
    """궤적에서 목표를 처음 넘는 나이. 못 넘으면 None."""
    for i, v in enumerate(path):
        if v >= target:
            return start_age + i
    return None


def twr_annualized(df_perf):
    """performance_summary '전체' 누적TWR → 연환산 명목 %. 실패 시 None."""
    try:
        if df_perf is None or df_perf.empty or "상세" not in df_perf.columns:
            return None
        row = df_perf[df_perf["상세"].astype(str).str.strip() == "전체"]
        if row.empty:
            return None
        col = "누적수익률(%)"
        if col not in row.columns:
            return None
        import re
        s = str(row.iloc[0][col])
        has_pct = "%" in s
        cleaned = re.sub(r"[^\d.\-]", "", s)
        if cleaned in ("", "-", "."):
            return None
        v = float(cleaned)
        if not has_pct:
            v *= 100
        cum = v / 100.0
        days = (datetime.now().date() - INCEPTION).days
        t = max(days / 365.25, 0.25)
        return ((1 + cum) ** (1 / t) - 1) * 100
    except Exception:
        return None


# =========================================================
# 메인 렌더 (Dashboard.py 가 호출)
# =========================================================
def render(df_dashboard=None, df_perf=None, now_kst=None):
    d = DEFAULTS
    st.title("🎯 은퇴 로드맵")
    st.caption(
        "목표 은퇴자산까지 **매년 얼마의 수익률**이 필요한지, **올해 내 실제 페이스가 그 목표를 넘고 있는지** 비교. "
        "전세 유지 vs 반전세 전환 두 버전. (모든 금액 '억원', **오늘 구매력** 기준)"
    )

    cur_year = (now_kst() if now_kst else datetime.now()).year
    kor_age = cur_year - d["birth_year"] + 1

    # 현재 투자자산 자동 추출 (대시보드 총평가액)
    auto_invest = None
    try:
        if df_dashboard is not None and "market_value_krw" in df_dashboard.columns:
            auto_invest = float(df_dashboard["market_value_krw"].sum()) / 1e8
    except Exception:
        auto_invest = None

    twr_ann = twr_annualized(df_perf)

    # ──────────────────────────────────────────────
    # 입력
    # ──────────────────────────────────────────────
    with st.expander("⚙️ 가정 설정 (값을 바꾸면 즉시 반영)", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**🏠 거주 (살 집)**")
            scenario = st.radio(
                "전세 처리", ["전세 유지", "반전세 전환"], horizontal=True,
                help="반전세로 옮기면 보증금 차액을 투자버킷에 추가로 굴립니다.",
            )
            home = st.number_input("목표 과천 84㎡ (억)", 15.0, 30.0, d["home"], 0.5)
            jeonse = st.number_input("현재 전세보증금 (억)", 0.0, 15.0, d["jeonse"], 0.1)
            if scenario == "반전세 전환":
                half_dep = st.number_input("반전세 보증금 (억)", 0.0, float(jeonse), d["half_deposit"], 0.1)
                rent_m = st.number_input("반전세 월세 (만원/월)", 0, 500, d["half_rent_m"], 10)
            else:
                half_dep, rent_m = float(jeonse), 0
        with c2:
            st.markdown("**📈 투자 (생활비 재원)**")
            inv_default = round(auto_invest, 1) if auto_invest else 3.4
            invest = st.number_input(
                "현재 주식자산 (억)", 0.0, 50.0, float(inv_default), 0.1,
                help=(f"대시보드 자동값: {auto_invest:.2f}억" if auto_invest else "수동 입력"),
            )
            inc_auction = st.checkbox(f"경매수익 {d['auction']}억 투자 합류 (11월 매도)", value=True)
            auction = st.number_input("경매수익 (억)", 0.0, 15.0, d["auction"], 0.1) if inc_auction else 0.0
            contrib = st.number_input(
                "연 추가납입 (억)", 0.0, 5.0, d["contrib"], 0.1,
                help="근로소득에서 매년 새로 투입. 0이면 순수 운용수익만으로 달성.",
            )
        with c3:
            st.markdown("**🎯 목표 · 가정**")
            swr = st.slider("안전인출률 SWR (%)", 2.5, 5.0, d["swr"], 0.1,
                            help="은퇴자산의 몇 %를 매년 빼 쓸지. 조기은퇴(40년+)는 3~3.5% 권장.")
            infl = st.slider("물가상승률 (%)", 1.0, 4.0, d["infl"], 0.1)
            exp_ret = st.slider(
                "차트 시뮬레이션 수익률 (명목 %)", 0.0, 20.0, float(d["exp_return"]), 0.5,
                help="아래 '자산 궤적 차트' 를 그릴 때만 쓰는 가정 수익률. 결과요약·판정과는 무관.",
            )
            rent_from_invest = False
            if scenario == "반전세 전환":
                rent_from_invest = st.checkbox(
                    "월세를 투자에서 인출 (보수적)", value=False,
                    help="끄면 월세는 근로소득 충당으로 보고 투자버킷에 영향 없음.",
                )

        cc1, cc2 = st.columns(2)
        with cc1:
            living_m = st.number_input("생활비 (만원/월)", 100, 1500, d["living_m"], 10)
        with cc2:
            travel_y = st.number_input("여행비 (만원/년)", 0, 5000, d["travel_y"], 100)

        st.markdown("**📅 올해 진척 비교용 — 연초(1/1) 순자산**")
        j1, j2, j3 = st.columns(3)
        with j1:
            jan1_je = st.number_input("연초 전세 (억)", 0.0, 15.0, d["jan1_jeonse"], 0.1)
        with j2:
            jan1_st = st.number_input("연초 주식 (억)", 0.0, 50.0, d["jan1_stock"], 0.1)
        with j3:
            jan1_au = st.number_input("연초 경매 (억)", 0.0, 15.0, d["jan1_auction"], 0.1)

    # ──────────────────────────────────────────────
    # 계산
    # ──────────────────────────────────────────────
    living_y = living_m * 12 / 10000.0      # 만원/월 → 억/년
    travel = travel_y / 10000.0             # 만원/년 → 억
    cashflow = living_y + travel            # 억/년
    invest_target = cashflow / (swr / 100)  # 투자버킷 목표 (억)
    total_target = home + invest_target     # 총 목표자산 (억)

    resid = jeonse if scenario == "전세 유지" else half_dep
    extra = 0.0 if scenario == "전세 유지" else max(jeonse - half_dep, 0.0)
    invest0 = invest + auction + extra
    rent_y = rent_m * 12 / 10000.0          # 억/년
    outflow = rent_y if rent_from_invest else 0.0

    r_real = real_rate(exp_ret, infl)

    yrs_50 = (d["birth_year"] + 50 - 1) - cur_year   # 한국나이 50세까지 연수
    yrs_55 = (d["birth_year"] + 55 - 1) - cur_year
    horizon = max(yrs_55, 1) + 3

    # 현재 시나리오 기준 필요수익률 (명목)
    need_50 = need_nominal(total_target, invest0, resid, yrs_50, infl, contrib, outflow)
    need_55 = need_nominal(total_target, invest0, resid, yrs_55, infl, contrib, outflow)

    # 올해 진척 (YTD)
    jan1_total = jan1_je + jan1_st + jan1_au          # 연초 순자산
    now_total = jeonse + invest + auction             # 현재 순자산 (시나리오 무관)
    inv_jan1 = jan1_st + jan1_au                       # 연초 투자버킷 (주식+경매)
    inv_now = invest + auction                         # 현재 투자버킷
    _today = (now_kst() if now_kst else datetime.now()).date()
    _elapsed = max((_today - date(_today.year, 1, 1)).days / 365.25, 0.05)
    ytd_chg = (now_total / jan1_total - 1) if jan1_total > 0 else 0.0       # 순자산 YTD
    inv_ytd = (inv_now / inv_jan1 - 1) if inv_jan1 > 0 else 0.0             # 투자버킷 YTD
    stock_ytd = (invest / jan1_st - 1) if jan1_st > 0 else 0.0             # 주식만 YTD

    # ──────────────────────────────────────────────
    # 결과 요약 — 목표와 필요수익률 (가정 수익률과 무관)
    # ──────────────────────────────────────────────
    st.markdown("### 📊 결과 요약")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("목표 총자산", f"{total_target:.1f}억",
              help=f"과천 {home:.0f}억 + 투자 {invest_target:.1f}억 (연 {cashflow*10000:,.0f}만 ÷ {swr:.1f}%)")
    m2.metric("현재 순자산", f"{now_total:.1f}억",
              help=f"전세 {jeonse:.1f} + 주식 {invest:.1f} + 경매 {auction:.1f}")
    m3.metric(f"필요수익률 (50세·{yrs_50}년)", f"{need_50*100:.1f}%/년" if need_50 else "-",
              help=f"{scenario} 기준, 투자버킷이 내야 할 명목 CAGR")
    m4.metric(f"필요수익률 (55세·{yrs_55}년)", f"{need_55*100:.1f}%/년" if need_55 else "-",
              help=f"{scenario} 기준, 투자버킷이 내야 할 명목 CAGR")

    if scenario == "반전세 전환":
        st.info(
            f"💡 반전세 월세 {rent_m}만원/월 = 연 {rent_y:.2f}억. "
            f"50세까지 누적 **{rent_y*yrs_50:.1f}억** / 55세까지 **{rent_y*yrs_55:.1f}억**. "
            + ("(투자에서 인출 반영 중)" if rent_from_invest else "(근로소득 충당 가정 — 투자버킷 영향 없음)")
        )

    # ──────────────────────────────────────────────
    # 핵심: 목표 필요수익률 vs 내 올해 페이스
    # ──────────────────────────────────────────────
    st.markdown("### 🆚 목표 필요수익률 vs 내 올해 페이스 (YTD)")

    cpa, cpb, cpc = st.columns(3)
    cpa.metric("순자산 YTD", f"{ytd_chg*100:+.1f}%", help="전세 포함 전체 자산")
    cpb.metric("투자버킷 YTD", f"{inv_ytd*100:+.1f}%", help="주식+경매 (경매 일회성 포함)")
    cpc.metric("주식만 YTD", f"{stock_ytd*100:+.1f}%", help="내가 운용하는 순수 실력")

    # 4개 목표(전세/반전세 × 50/55세) 필요수익률을 올해 경과분으로 환산해 내 투자버킷 YTD 와 비교
    rows = []
    for sc_name, resid_v, extra_v, out_v in [
        ("전세 유지", jeonse, 0.0, 0.0),
        ("반전세 전환", d["half_deposit"], max(jeonse - d["half_deposit"], 0.0),
         (rent_y if rent_from_invest else 0.0)),
    ]:
        inv0 = invest + auction + extra_v
        for yrs, age in [(yrs_50, 50), (yrs_55, 55)]:
            need = need_nominal(total_target, inv0, resid_v, yrs, infl, contrib, out_v)
            if need is None:
                continue
            this_yr_goal = (1 + need) ** _elapsed - 1     # 올해 경과분까지 목표 누적
            ok = inv_ytd >= this_yr_goal
            rows.append({
                "시나리오·은퇴": f"{sc_name} · {age}세",
                "연 필요수익률": f"{need*100:.1f}%",
                "올해까지 목표": f"+{this_yr_goal*100:.1f}%",
                "내 투자 YTD": f"+{inv_ytd*100:.1f}%",
                "판정": "🟢 초과" if ok else "🔴 미달",
            })

    import pandas as pd
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption(
        f"⚠️ 투자버킷 YTD **+{inv_ytd*100:.0f}%** 중 경매 차익(연초 {jan1_au:.0f}→{auction:.1f}억)이 대부분 — **일회성**입니다. "
        f"경매 매도 후엔 이 페이스가 지속되기 어려워요. 지속가능한 **순수 주식 운용은 +{stock_ytd*100:.1f}%** (YTD). "
        f"'올해까지 목표'는 연 필요수익률을 올해 경과분({_elapsed:.2f}년)만큼만 환산한 값이라 직접 비교됩니다."
    )

    # ──────────────────────────────────────────────
    # 자산 궤적 차트 (차트 시뮬레이션 수익률 기준)
    # ──────────────────────────────────────────────
    st.markdown(f"### 📈 자산 성장 궤적 (시뮬레이션 수익률 {exp_ret:.1f}% 가정, 오늘 가치)")
    ages = [kor_age + i for i in range(horizon + 1)]
    path = project_path(invest0, resid, r_real, horizon, contrib, outflow)
    reach = reach_age(path, total_target, kor_age)

    # 반대 시나리오도 같은 가정으로 그려 비교
    if scenario == "전세 유지":
        alt_resid = d["half_deposit"]
        alt_extra = max(jeonse - d["half_deposit"], 0.0)
        alt_name = "반전세 전환(참고)"
    else:
        alt_resid = jeonse
        alt_extra = 0.0
        alt_name = "전세 유지(참고)"
    alt_path = project_path(invest + auction + alt_extra, alt_resid, r_real, horizon, contrib, 0.0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ages, y=path, name=f"{scenario} (선택)",
                             mode="lines+markers", line=dict(width=3, color="#2563eb")))
    fig.add_trace(go.Scatter(x=ages, y=alt_path, name=alt_name,
                             mode="lines", line=dict(width=2, dash="dot", color="#9ca3af")))
    fig.add_hline(y=total_target, line_dash="dash", line_color="#dc2626",
                  annotation_text=f"목표 {total_target:.0f}억", annotation_position="top left")
    for mark_age in (50, 55):
        if ages[0] <= mark_age <= ages[-1]:
            fig.add_vline(x=mark_age, line_dash="dot", line_color="#f59e0b",
                          annotation_text=f"{mark_age}세")
    fig.update_layout(
        height=420, margin=dict(l=10, r=10, t=30, b=10),
        xaxis_title="한국나이", yaxis_title="총자산 (억, 오늘가치)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)
    if reach:
        st.caption(f"※ 시뮬레이션 {exp_ret:.1f}% 가정 시 한국나이 **{reach}세**에 목표 도달. (슬라이더를 바꾸면 달라집니다)")
    else:
        st.caption(f"※ 시뮬레이션 {exp_ret:.1f}% 가정으론 {ages[-1]}세까지 목표 미도달. 슬라이더를 올려보세요.")
