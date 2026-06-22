# retirement_roadmap.py — 호섭님 은퇴 로드맵 뷰
#
# Dashboard.py 의 "🎯 은퇴 로드맵" 뷰가 호출하는 독립 모듈.
# ① 올해 성적: 올해까지 목표 vs 내 YTD 막대 비교
# ② 오늘 기준 매년 필요 수익률 + 연초 대비 올해 성과가 얼마나 낮췄는지
# 전세 유지 vs 반전세 전환 두 버전.
#
# 핵심 모델 (실질=오늘 구매력, 억원):
#   거주 버킷(전세/반전세 보증금, 실질 불변) + 투자 버킷(주식+경매+전환차액, 복리)
#   목표 = 과천 아파트값 + (생활비+여행)/SWR
#   필요수익률 = 투자버킷이 목표 도달에 매년 내야 하는 명목 CAGR (전세는 안 굴러감)
#   - 연초 기준 = 연초 투자버킷 출발 / 오늘 기준 = 현재 투자버킷 출발. 둘 다 은퇴까지 동일 기간.
#   - 오늘 기준은 자산이 매일 바뀌면 함께 갱신됨.

from datetime import date, datetime

import plotly.graph_objects as go
import streamlit as st

DEFAULTS = dict(
    birth_year=1986, jeonse=8.1, auction=3.5, half_deposit=4.0, half_rent_m=260,
    home=22.0, living_m=450, travel_y=1750, swr=4.0, infl=2.5, exp_return=12.0,
    contrib=0.0, jan1_jeonse=8.1, jan1_stock=3.0, jan1_auction=2.0,
)
INCEPTION = date(2025, 5, 14)
KPI_MIN, KPI_AVG, KPI_MAX = 10.0, 15.0, 20.0


def real_rate(nom_pct, infl_pct):
    return (1 + nom_pct / 100.0) / (1 + infl_pct / 100.0) - 1


def project_path(invest0, resid0, r_real, years, contrib=0.0, outflow=0.0):
    path, inv = [], invest0
    for _ in range(int(years) + 1):
        path.append(resid0 + inv)
        inv = inv * (1 + r_real) + contrib - outflow
    return path


def required_cagr(target, invest0, resid0, years, contrib=0.0, outflow=0.0):
    if years is None or years <= 0:
        return float("nan")
    lo, hi = -0.9, 2.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if project_path(invest0, resid0, mid, years, contrib, outflow)[-1] < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def need_nominal(target, invest0, resid0, years, infl_pct, contrib=0.0, outflow=0.0):
    if years is None or years <= 0 or invest0 <= 0:
        return None
    rr = required_cagr(target, invest0, resid0, years, contrib, outflow)
    return (1 + rr) * (1 + infl_pct / 100) - 1


def reach_age(path, target, start_age):
    for i, v in enumerate(path):
        if v >= target:
            return start_age + i
    return None


def twr_annualized(df_perf):
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


def render(df_dashboard=None, df_perf=None, now_kst=None):
    d = DEFAULTS
    st.title("🎯 은퇴 로드맵")
    st.caption(
        "올해 내 실제 페이스가 목표를 넘고 있는지, 그리고 오늘 자산 기준으로 은퇴까지 매년 몇 % 내면 되는지. "
        "(모든 금액 '억원', 오늘 구매력 기준)"
    )

    cur_year = (now_kst() if now_kst else datetime.now()).year
    kor_age = cur_year - d["birth_year"] + 1

    auto_invest = None
    try:
        if df_dashboard is not None and "market_value_krw" in df_dashboard.columns:
            auto_invest = float(df_dashboard["market_value_krw"].sum()) / 1e8
    except Exception:
        auto_invest = None

    with st.expander("⚙️ 가정 설정 (값을 바꾸면 즉시 반영)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**🏠 거주 (살 집)**")
            scenario = st.radio("전세 처리", ["전세 유지", "반전세 전환"], horizontal=True)
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
            invest = st.number_input("현재 주식자산 (억)", 0.0, 50.0, float(inv_default), 0.1)
            inc_auction = st.checkbox("경매수익 투자 합류 (11월 매도)", value=True)
            auction = st.number_input("경매수익 (억)", 0.0, 15.0, d["auction"], 0.1) if inc_auction else 0.0
            contrib = st.number_input("연 추가납입 (억)", 0.0, 5.0, d["contrib"], 0.1,
                                      help="근로소득에서 매년 새로 투입. 0이면 순수 운용수익만.")
        with c3:
            st.markdown("**🎯 목표 · 가정**")
            swr = st.slider("안전인출률 SWR (%)", 2.5, 5.0, d["swr"], 0.1,
                            help="은퇴자산의 몇 %를 매년 빼 쓸지. 조기은퇴는 3~3.5% 권장.")
            infl = st.slider("물가상승률 (%)", 1.0, 4.0, d["infl"], 0.1)
            exp_ret = st.slider("차트 시뮬레이션 수익률 (명목 %)", 0.0, 20.0, float(d["exp_return"]), 0.5,
                                help="맨 아래 자산 궤적 차트용 가정 수익률.")
            rent_from_invest = False
            if scenario == "반전세 전환":
                rent_from_invest = st.checkbox("월세를 투자에서 인출 (보수적)", value=False)

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

    # ── 계산 ──
    cashflow = living_m * 12 / 10000.0 + travel_y / 10000.0
    invest_target = cashflow / (swr / 100)
    total_target = home + invest_target

    resid = jeonse if scenario == "전세 유지" else half_dep
    extra = 0.0 if scenario == "전세 유지" else max(jeonse - half_dep, 0.0)
    invest0 = invest + auction + extra
    rent_y = rent_m * 12 / 10000.0
    outflow = rent_y if rent_from_invest else 0.0

    yrs_50 = (d["birth_year"] + 50 - 1) - cur_year
    yrs_55 = (d["birth_year"] + 55 - 1) - cur_year

    _today = (now_kst() if now_kst else datetime.now()).date()
    _elapsed = max((_today - date(_today.year, 1, 1)).days / 365.25, 0.05)
    inv_jan1 = jan1_st + jan1_au
    inv_now = invest + auction
    jan1_total = jan1_je + jan1_st + jan1_au
    now_total = jeonse + invest + auction
    ytd_chg = (now_total / jan1_total - 1) if jan1_total > 0 else 0.0
    inv_ytd = (inv_now / inv_jan1 - 1) if inv_jan1 > 0 else 0.0
    stock_ytd = (invest / jan1_st - 1) if jan1_st > 0 else 0.0

    # 4 시나리오 (라벨, 거주보증금, 전환차액, 은퇴까지연수)
    SCEN = [
        ("전세·50세", jeonse, 0.0, yrs_50),
        ("전세·55세", jeonse, 0.0, yrs_55),
        ("반전세·50세", d["half_deposit"], max(jeonse - d["half_deposit"], 0.0), yrs_50),
        ("반전세·55세", d["half_deposit"], max(jeonse - d["half_deposit"], 0.0), yrs_55),
    ]
    labels, goal_ytd_list, cur_need_list, plan_need_list = [], [], [], []
    for nm, rv, ex, yrs in SCEN:
        inv0_now = invest + auction + ex      # 오늘 투자버킷
        inv0_jan = inv_jan1 + ex              # 연초 투자버킷
        nn_now = need_nominal(total_target, inv0_now, rv, yrs, infl, contrib, outflow)   # 오늘 기준
        nn_plan = need_nominal(total_target, inv0_jan, rv, yrs, infl, contrib, outflow)  # 연초 기준
        labels.append(nm)
        # ① 올해까지 목표 = 연초에 세운 페이스(연초 기준)를 올해 경과분만큼 환산
        goal_ytd_list.append(((1 + nn_plan) ** _elapsed - 1) * 100 if nn_plan else 0.0)
        cur_need_list.append(nn_now * 100 if nn_now else 0.0)    # 오늘 기준 (메인)
        plan_need_list.append(nn_plan * 100 if nn_plan else 0.0)  # 연초 기준 (비교)

    # ── 결과 요약 ──
    st.markdown("### 📊 결과 요약")
    m1, m2, m3 = st.columns(3)
    m1.metric("목표 총자산", f"{total_target:.1f}억", help=f"과천 {home:.0f}억 + 투자 {invest_target:.1f}억")
    m2.metric("현재 순자산", f"{now_total:.1f}억", help=f"전세 {jeonse:.1f} + 주식 {invest:.1f} + 경매 {auction:.1f}")
    m3.metric("올해 투자 성과 (YTD)", f"+{inv_ytd*100:.1f}%", help="주식+경매, 연초 대비")

    if scenario == "반전세 전환":
        st.info(f"💡 반전세 월세 {rent_m}만원/월 = 연 {rent_y:.2f}억. "
                + ("(투자에서 인출 반영)" if rent_from_invest else "(근로소득 충당 — 투자버킷 영향 없음)"))

    # ── ① 올해 성적 ──
    st.markdown("### 🆚 ① 올해 성적 — 목표 대비 얼마나 앞섰나")
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(x=labels, y=goal_ytd_list, name="올해까지 목표",
                          marker_color="#94a3b8",
                          text=[f"{g:.1f}%" for g in goal_ytd_list], textposition="outside"))
    fig1.add_trace(go.Bar(x=labels, y=[inv_ytd * 100] * 4, name=f"내 투자 실적 (+{inv_ytd*100:.0f}%)",
                          marker_color="#2563eb",
                          text=[f"+{inv_ytd*100:.0f}%"] * 4, textposition="outside"))
    fig1.add_hline(y=stock_ytd * 100, line_dash="dot", line_color="#16a34a",
                   annotation_text=f"주식만 +{stock_ytd*100:.0f}%", annotation_position="top left")
    fig1.update_layout(barmode="group", height=380, margin=dict(l=10, r=10, t=30, b=10),
                       yaxis_title="올해 수익률 (%)", legend=dict(orientation="h", y=1.12, x=1, xanchor="right"))
    st.plotly_chart(fig1, use_container_width=True)
    st.caption(
        f"회색 = 올해까지 냈어야 할 목표(연초 페이스를 올해 경과분 {_elapsed:.2f}년만큼), "
        f"파랑 = 내 실제 투자 YTD(+{inv_ytd*100:.0f}%). 파랑이 높으면 **목표 초과**. "
        f"※ 투자 YTD엔 경매 차익(일회성) 큼 — 지속가능 주식 운용은 점선(+{stock_ytd*100:.0f}%)."
    )

    # ── ② 오늘 기준 매년 필요 수익률 (연초 대비 영향) ──
    st.markdown("### 🎯 ② 오늘 기준, 은퇴까지 매년 필요한 수익률")
    bar_colors = ["#dc2626" if n > 18 else "#f59e0b" if n > 13 else "#16a34a" for n in cur_need_list]
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=labels, y=plan_need_list, name="연초엔 필요했던",
                          marker_color="#cbd5e1",
                          text=[f"{p:.1f}%" for p in plan_need_list], textposition="outside"))
    fig2.add_trace(go.Bar(x=labels, y=cur_need_list, name="오늘 기준 (올해 벌어서 ↓)",
                          marker_color=bar_colors,
                          text=[f"{c:.1f}%" for c in cur_need_list], textposition="outside"))
    fig2.add_hline(y=KPI_AVG, line_dash="dot", line_color="#9ca3af",
                   annotation_text=f"KPI 평균 {KPI_AVG:.0f}%", annotation_position="top left")
    fig2.update_layout(barmode="group", height=380, margin=dict(l=10, r=10, t=40, b=10),
                       yaxis_title="매년 필요 (명목 %)", legend=dict(orientation="h", y=1.12, x=1, xanchor="right"))
    st.plotly_chart(fig2, use_container_width=True)

    import pandas as pd
    tbl = pd.DataFrame({
        "시나리오": labels,
        "연초엔 필요했던": [f"{p:.1f}%/년" for p in plan_need_list],
        "오늘 기준 매년 필요": [f"{c:.1f}%/년" for c in cur_need_list],
        "올해 성과로 낮춘 폭": [f"-{p-c:.1f}%p" if p >= c else f"+{c-p:.1f}%p" for p, c in zip(plan_need_list, cur_need_list)],
    })
    st.dataframe(tbl, use_container_width=True, hide_index=True)
    st.caption(
        f"올해 +{inv_ytd*100:.0f}% 벌어서 자산이 앞당겨진 만큼, **매년 필요수익률이 연초보다 낮아졌습니다** "
        f"(전세·50세: {plan_need_list[0]:.1f}% → {cur_need_list[0]:.1f}%). 이게 '잘하고 있다'의 증거예요. "
        f"가장 현실적인 길은 **반전세·55세 = 연 {cur_need_list[3]:.1f}%** (KPI 평균 {KPI_AVG:.0f}% 안쪽). "
        f"이 값은 **매일 자산이 바뀌면 함께 갱신**됩니다. 막대 색: 초록 ≤13% 무난 / 주황 13~18% / 빨강 >18% 공격적."
    )

    # ── 자산 궤적 차트 (시뮬레이션, 참고) ──
    with st.expander("📈 자산 성장 궤적 시뮬레이션 (참고)", expanded=False):
        r_real = real_rate(exp_ret, infl)
        horizon = max(yrs_55, 1) + 3
        ages = [kor_age + i for i in range(horizon + 1)]
        path = project_path(invest0, resid, r_real, horizon, contrib, outflow)
        reach = reach_age(path, total_target, kor_age)
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=ages, y=path, name=f"{scenario}", mode="lines+markers",
                                  line=dict(width=3, color="#2563eb")))
        fig3.add_hline(y=total_target, line_dash="dash", line_color="#dc2626",
                       annotation_text=f"목표 {total_target:.0f}억")
        for ma in (50, 55):
            if ages[0] <= ma <= ages[-1]:
                fig3.add_vline(x=ma, line_dash="dot", line_color="#f59e0b", annotation_text=f"{ma}세")
        fig3.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10),
                           xaxis_title="한국나이", yaxis_title="총자산 (억, 오늘가치)")
        st.plotly_chart(fig3, use_container_width=True)
        if reach:
            st.caption(f"시뮬레이션 {exp_ret:.1f}% 가정 시 한국나이 {reach}세 도달.")
        else:
            st.caption(f"시뮬레이션 {exp_ret:.1f}% 가정으론 {ages[-1]}세까지 미도달.")
