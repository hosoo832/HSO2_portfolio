# retirement_roadmap.py — 호섭님 은퇴 로드맵 뷰
#
# Dashboard.py 의 "🎯 은퇴 로드맵" 뷰가 호출하는 독립 모듈.
# ① 올해 성적  ② 오늘 기준 매년 필요수익률  ③ 수익률 3종(나/와이프/합산)  ④ 연도별 자산 로드맵
#
# 핵심 모델 (실질=오늘 구매력, 억원):
#   거주 버킷(전세/반전세 보증금, 실질 불변) + 투자 버킷(주식+경매+전환차액, 복리)
#   목표 = 과천 아파트값 + (생활비+여행)/SWR
#   - 와이프 송금(약 4,823만)은 주식→경매 내부이동 → 합산 수익률엔 중립.
#   - 경매 = 와이프 세후 실현가(기본 3.7억). 와이프 수익률 = 경매 원금(2.48억) 대비.
#   - 은퇴 도달은 '합산 자산 금액', 실력 평가는 각자 수익률.

from datetime import date, datetime

import plotly.graph_objects as go
import streamlit as st

DEFAULTS = dict(
    birth_year=1986, jeonse=8.1,
    auction=3.7, auction_cost=2.48, wife_funding=0.4823,
    half_deposit=4.0, half_rent_m=260,
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
        "올해 페이스가 목표를 넘는지, 은퇴까지 매년 몇 % 필요한지, 나·와이프·합산 수익률, "
        "그리고 연도별로 자산이 얼마가 되어야 하는지. (모든 금액 '억원', 오늘 구매력 기준)"
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
            inc_auction = st.checkbox("경매 투자 합류 (11월 매도)", value=True)
            if inc_auction:
                auction = st.number_input("경매 세후 실현가 (억)", 0.0, 15.0, d["auction"], 0.1,
                                          help="와이프가 4억에 팔고 세금 등 뺀 실수령액 (≈3.7)")
                auction_cost = st.number_input("└ 경매 원금 (억)", 0.0, 15.0, d["auction_cost"], 0.01,
                                               help="와이프 경매 종잣돈(연초 평가+올해 송금). 와이프 수익률 계산용")
            else:
                auction, auction_cost = 0.0, 0.0
            contrib = st.number_input("연 추가납입 (억)", 0.0, 5.0, d["contrib"], 0.1,
                                      help="근로소득에서 매년 새로 투입. 0이면 순수 운용수익만.")
        with c3:
            st.markdown("**🎯 목표 · 가정**")
            swr = st.slider("안전인출률 SWR (%)", 2.5, 5.0, d["swr"], 0.1,
                            help="은퇴자산의 몇 %를 매년 빼 쓸지. 조기은퇴는 3~3.5% 권장.")
            infl = st.slider("물가상승률 (%)", 1.0, 4.0, d["infl"], 0.1)
            exp_ret = st.slider("기대/시뮬 수익률 (명목 %)", 0.0, 20.0, float(d["exp_return"]), 0.5,
                                help="연도별 로드맵·차트의 '예상' 곡선용 가정 수익률.")
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
    inv_ytd = (inv_now / inv_jan1 - 1) if inv_jan1 > 0 else 0.0

    me_ret = ((invest + d["wife_funding"]) / jan1_st - 1) * 100 if jan1_st > 0 else 0.0
    me_asset = (invest / jan1_st - 1) * 100 if jan1_st > 0 else 0.0
    wife_ret = (auction / auction_cost - 1) * 100 if auction_cost > 0 else 0.0
    comb_ret = (inv_now / inv_jan1 - 1) * 100 if inv_jan1 > 0 else 0.0

    SCEN = [
        ("전세·50세", jeonse, 0.0, yrs_50),
        ("전세·55세", jeonse, 0.0, yrs_55),
        ("반전세·50세", d["half_deposit"], max(jeonse - d["half_deposit"], 0.0), yrs_50),
        ("반전세·55세", d["half_deposit"], max(jeonse - d["half_deposit"], 0.0), yrs_55),
    ]
    labels, goal_ytd_list, cur_need_list, plan_need_list = [], [], [], []
    for nm, rv, ex, yrs in SCEN:
        inv0_now = invest + auction + ex
        inv0_jan = inv_jan1 + ex
        nn_now = need_nominal(total_target, inv0_now, rv, yrs, infl, contrib, outflow)
        nn_plan = need_nominal(total_target, inv0_jan, rv, yrs, infl, contrib, outflow)
        labels.append(nm)
        goal_ytd_list.append(((1 + nn_plan) ** _elapsed - 1) * 100 if nn_plan else 0.0)
        cur_need_list.append(nn_now * 100 if nn_now else 0.0)
        plan_need_list.append(nn_plan * 100 if nn_plan else 0.0)

    # ── 결과 요약 ──
    st.markdown("### 📊 결과 요약")
    m1, m2, m3 = st.columns(3)
    m1.metric("목표 총자산", f"{total_target:.1f}억", help=f"과천 {home:.0f}억 + 투자 {invest_target:.1f}억")
    m2.metric("현재 순자산", f"{now_total:.1f}억", help=f"전세 {jeonse:.1f} + 주식 {invest:.1f} + 경매 {auction:.1f}")
    m3.metric("올해 합산 투자성과", f"+{comb_ret:.0f}%", help="주식+경매 합산, 연초 대비")

    # ── 🏅 수익률 3종 ──
    st.markdown("### 🏅 올해 수익률 — 🧑 나 / 👩 와이프 / 🤝 합산")
    def tag(v):
        return "🟢" if v >= KPI_AVG else ("🟡" if v >= KPI_MIN else "🔴")
    rr1, rr2, rr3 = st.columns(3)
    rr1.metric("🧑 나 (주식)", f"+{me_ret:.0f}%", help=f"와이프 송금 복원 기준. 통장 자산만 보면 +{me_asset:.0f}%")
    rr2.metric("👩 와이프 (경매)", f"+{wife_ret:.0f}%" if auction_cost > 0 else "-",
               help=f"원금 {auction_cost:.2f}억 → 실현 {auction:.1f}억")
    rr3.metric("🤝 합산 (투자버킷)", f"+{comb_ret:.0f}%", help="주식+경매 전체. 송금은 내부이동이라 중립")
    st.caption(
        f"KPI {KPI_MIN:.0f}/{KPI_AVG:.0f}/{KPI_MAX:.0f}% 대비 → 나 {tag(me_ret)} / 와이프 {tag(wife_ret)} / 합산 {tag(comb_ret)}. "
        "**은퇴 도달은 '합산 자산 금액'으로, 실력은 각자 수익률로** 봅니다. (나의 +%는 송금 복원 근사)"
    )

    if scenario == "반전세 전환":
        st.info(f"💡 반전세 월세 {rent_m}만원/월 = 연 {rent_y:.2f}억. "
                + ("(투자에서 인출 반영)" if rent_from_invest else "(근로소득 충당 — 투자버킷 영향 없음)"))

    # ── ① 올해 성적 ──
    st.markdown("### 🆚 ① 올해 성적 — 목표 대비 얼마나 앞섰나")
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(x=labels, y=goal_ytd_list, name="올해까지 목표", marker_color="#94a3b8",
                          text=[f"{g:.1f}%" for g in goal_ytd_list], textposition="outside"))
    fig1.add_trace(go.Bar(x=labels, y=[inv_ytd * 100] * 4, name=f"내 합산 투자 (+{inv_ytd*100:.0f}%)",
                          marker_color="#2563eb", text=[f"+{inv_ytd*100:.0f}%"] * 4, textposition="outside"))
    fig1.update_layout(barmode="group", height=380, margin=dict(l=10, r=10, t=30, b=10),
                       yaxis_title="올해 수익률 (%)", legend=dict(orientation="h", y=1.12, x=1, xanchor="right"))
    st.plotly_chart(fig1, use_container_width=True)
    st.caption(f"회색 = 올해까지 목표(연초 페이스 × {_elapsed:.2f}년), 파랑 = 내 합산 YTD(+{inv_ytd*100:.0f}%). 파랑이 높으면 목표 초과.")

    # ── ② 오늘 기준 매년 필요 수익률 ──
    st.markdown("### 🎯 ② 오늘 기준, 은퇴까지 매년 필요한 수익률")
    bar_colors = ["#dc2626" if n > 18 else "#f59e0b" if n > 13 else "#16a34a" for n in cur_need_list]
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=labels, y=plan_need_list, name="연초엔 필요했던", marker_color="#cbd5e1",
                          text=[f"{p:.1f}%" for p in plan_need_list], textposition="outside"))
    fig2.add_trace(go.Bar(x=labels, y=cur_need_list, name="오늘 기준 (올해 벌어서 ↓)", marker_color=bar_colors,
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
    st.caption(f"올해 합산 +{inv_ytd*100:.0f}% 벌어서 매년 필요수익률이 연초보다 낮아졌습니다. 가장 현실적인 길은 반전세·55세 = 연 {cur_need_list[3]:.1f}%.")

    # ── ④ 연도별 자산 로드맵 (매년 얼마가 되어야 하나) ──
    st.markdown("### 📅 ④ 연도별 자산 로드맵 — 매년 얼마가 되어야 하나")
    target_need = cur_need_list[1] if scenario == "전세 유지" else cur_need_list[3]   # 현 시나리오 55세 필요(%)
    r_target = real_rate(target_need, infl)
    r_exp = real_rate(exp_ret, infl)
    path_t = project_path(invest0, resid, r_target, yrs_55, contrib, outflow)   # 목표 궤도
    path_e = project_path(invest0, resid, r_exp, yrs_55, contrib, outflow)      # 예상 궤도
    ages = [kor_age + i for i in range(len(path_t))]

    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=ages, y=path_t, name=f"목표 궤도 ({target_need:.1f}%/년)",
                              mode="lines+markers", line=dict(width=3, color="#16a34a")))
    fig4.add_trace(go.Scatter(x=ages, y=path_e, name=f"예상 (@{exp_ret:.0f}%)",
                              mode="lines", line=dict(width=2, dash="dot", color="#2563eb")))
    fig4.add_hline(y=total_target, line_dash="dash", line_color="#dc2626",
                   annotation_text=f"목표 {total_target:.0f}억", annotation_position="top left")
    for ma in (50, 55):
        if ages[0] <= ma <= ages[-1]:
            fig4.add_vline(x=ma, line_dash="dot", line_color="#f59e0b", annotation_text=f"{ma}세")
    fig4.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10),
                       xaxis_title="한국나이", yaxis_title="총자산 (억, 오늘가치)",
                       legend=dict(orientation="h", y=1.1, x=1, xanchor="right"))
    st.plotly_chart(fig4, use_container_width=True)

    road = pd.DataFrame({
        "나이": [f"{a}세" for a in ages],
        "연도": [cur_year + i for i in range(len(ages))],
        "목표 궤도(억)": [f"{v:.1f}" for v in path_t],
        f"예상@{exp_ret:.0f}%(억)": [f"{v:.1f}" for v in path_e],
        "목표 대비": ["✅" if e >= t else "❌" for t, e in zip(path_t, path_e)],
    })
    st.dataframe(road, use_container_width=True, hide_index=True)
    st.caption(
        f"**목표 궤도** = {scenario}·55세 목표({total_target:.0f}억) 도달에 매년 {target_need:.1f}%씩 갈 때 연말 총자산. "
        f"**예상** = 기대/시뮬 수익률({exp_ret:.0f}%)로 갈 때. 거주(전세·보증금) 포함 오늘 가치. "
        "예상이 목표 궤도보다 높으면 ✅."
    )
