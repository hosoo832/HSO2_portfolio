# retirement_roadmap.py — 호섭님 은퇴 로드맵 뷰
#
# Dashboard.py 의 "🎯 은퇴 로드맵" 뷰가 호출하는 독립 모듈.
# 목표 은퇴자산까지 매년 필요한 수익률 + 현재 페이스로 몇 살에 닿는지 비교.
# 전세 유지 vs 반전세 전환 두 버전을 토글/표로 동시 비교.
#
# ── 핵심 모델 (실질 = 오늘 구매력 기준, 모든 금액 '억원') ──
#   - 거주 버킷: 전세/반전세 보증금 → 미래 과천 아파트. 실질 불변(물가만큼만 상승) 가정.
#   - 투자 버킷: 주식 + 경매(선택) + 반전세 전환차액. 매년 실질수익률로 복리 + 추가납입.
#   - 목표 총자산(오늘) = 과천 아파트값 + (생활비 + 여행비) / 안전인출률(SWR)
#   - 명목수익률 → 실질수익률: (1+명목)/(1+물가) - 1
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
    exp_return=12.0,    # 기대 연수익률 (명목) %
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
    """목표도달에 필요한 실질 CAGR(소수)을 이분법으로 역산."""
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
        "목표 은퇴자산까지 **매년 얼마의 수익률**이 필요한지, 현재 페이스로 **몇 살에** 닿는지. "
        "전세 유지 vs 반전세 전환 두 버전 비교. (모든 금액 '억원', **오늘 구매력** 기준)"
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
            st.markdown("**🎯 목표 · 수익률**")
            ret_default = round(twr_ann, 1) if (twr_ann and 0 < twr_ann < 40) else d["exp_return"]
            exp_ret = st.slider(
                "기대 연수익률 (명목 %)", 0.0, 20.0, float(ret_default), 0.5,
                help=(f"현재 실적 연환산 TWR: {twr_ann:.1f}%" if twr_ann else "실적 연동 실패 — 수동 입력"),
            )
            swr = st.slider("안전인출률 SWR (%)", 2.5, 5.0, d["swr"], 0.1,
                            help="은퇴자산의 몇 %를 매년 빼 쓸지. 조기은퇴(40년+)는 3~3.5% 권장.")
            infl = st.slider("물가상승률 (%)", 1.0, 4.0, d["infl"], 0.1)
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

    path = project_path(invest0, resid, r_real, horizon, contrib, outflow)
    reach = reach_age(path, total_target, kor_age)

    def need_nom(years):
        if years is None or years <= 0:
            return None
        rr = required_cagr(total_target, invest0, resid, years, contrib, outflow)
        return (1 + rr) * (1 + infl / 100) - 1   # 실질 → 명목

    need_50 = need_nom(yrs_50)
    need_55 = need_nom(yrs_55)

    # 올해 진척 (YTD) — 순자산은 시나리오 무관 (전세 = 보증금 + 전환차액)
    jan1_total = jan1_je + jan1_st + jan1_au
    now_total = jeonse + invest + auction
    _today = (now_kst() if now_kst else datetime.now()).date()
    _elapsed = max((_today - date(_today.year, 1, 1)).days / 365.25, 0.05)
    ytd_chg = (now_total / jan1_total - 1) if jan1_total > 0 else 0.0
    ytd_ann = ((1 + ytd_chg) ** (1 / _elapsed) - 1) if ytd_chg > -1 else 0.0

    # ──────────────────────────────────────────────
    # 결과 요약
    # ──────────────────────────────────────────────
    st.markdown("### 📊 결과 요약")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("목표 총자산", f"{total_target:.1f}억",
              help=f"과천 {home:.0f}억 + 투자 {invest_target:.1f}억 (연 {cashflow*10000:,.0f}만 ÷ {swr:.1f}%)")
    m2.metric("현재 자산", f"{resid + invest0:.1f}억",
              help=f"거주 {resid:.1f}억 + 투자 {invest0:.1f}억")
    m3.metric(f"필요수익률 (50세·{yrs_50}년)",
              f"{need_50*100:.1f}%" if need_50 else "-",
              help="추가납입·시나리오 반영, 명목 기준")
    m4.metric("예상 도달 나이", f"{reach}세" if reach else "미도달",
              delta=f"기대 {exp_ret:.1f}% 운용 시", delta_color="off")

    # 신호등 판정 (현재 기대수익률 기준)
    if reach and reach <= 50:
        st.success(f"🟢 기대수익률 **{exp_ret:.1f}%**면 한국나이 **{reach}세**에 목표 도달 — 50세 은퇴 가능권!")
    elif reach and reach <= 55:
        st.warning(f"🟡 기대수익률 **{exp_ret:.1f}%**면 **{reach}세** 도달 — 50세는 빠듯, 55세 안에는 가능.")
    elif reach:
        st.error(f"🔴 기대수익률 **{exp_ret:.1f}%**면 **{reach}세**에야 도달 — 수익률/목표/은퇴시점 조정 필요.")
    else:
        st.error(f"🔴 기대수익률 **{exp_ret:.1f}%**로는 {kor_age + horizon}세까지도 목표 미도달.")

    if scenario == "반전세 전환":
        st.info(
            f"💡 반전세 월세 {rent_m}만원/월 = 연 {rent_y:.2f}억. "
            f"50세까지 누적 **{rent_y*yrs_50:.1f}억** / 55세까지 **{rent_y*yrs_55:.1f}억**. "
            + ("(투자에서 인출 반영 중)" if rent_from_invest else "(근로소득 충당 가정 — 투자버킷 영향 없음)")
        )

    # ──────────────────────────────────────────────
    # 올해 진척 (YTD) — 지금 페이스 점검
    # ──────────────────────────────────────────────
    st.markdown("### 📅 올해 진척 (YTD) — 지금 잘하고 있나?")
    pace = need_55 if need_55 else need_50
    y1, y2, y3, y4 = st.columns(4)
    y1.metric("연초 순자산 (1/1)", f"{jan1_total:.1f}억")
    y2.metric("현재 순자산", f"{now_total:.1f}억", delta=f"{now_total - jan1_total:+.1f}억")
    y3.metric("YTD 성장률", f"{ytd_chg*100:+.1f}%",
              delta=f"연율 환산 {ytd_ann*100:.0f}%", delta_color="off")
    y4.metric("목표 필요수익률 (55세)", f"{pace*100:.1f}%/년" if pace else "-",
              help="이 속도 이상이면 55세 목표 궤도 위")

    # (1) 은퇴 목표 페이스 판정 — 순자산 전체 기준
    if pace:
        if ytd_ann >= pace:
            st.success(
                f"🟢 올해 순자산 연율 **{ytd_ann*100:.0f}%** ≥ 목표 필요 **{pace*100:.1f}%** "
                "— 목표 궤도 위에 있습니다."
            )
        else:
            gap_amt = jan1_total * (1 + pace) ** _elapsed - now_total
            st.warning(
                f"🟡 올해 순자산 연율 **{ytd_ann*100:.0f}%** < 목표 필요 **{pace*100:.1f}%** "
                f"— 목표 궤도까지 약 **{gap_amt:.1f}억** 부족."
            )
    st.caption(
        "⚠️ 순자산 성장엔 경매 평가차익 등 **일회성**이 섞여 연율이 과장될 수 있음. "
        "순수 운용 실력은 아래 '주식 TWR' 로 판단하세요."
    )

    # (2) 운용 실력 — 주식 TWR vs KPI 목표
    if twr_ann is not None:
        if twr_ann >= KPI_AVG:
            tag = f"🟢 평균목표({KPI_AVG:.0f}%) 초과 — 아주 잘하고 있음"
        elif twr_ann >= KPI_MIN:
            tag = f"🟡 최소목표({KPI_MIN:.0f}%) 달성 — 평균({KPI_AVG:.0f}%)까진 더"
        else:
            tag = f"🔴 최소목표({KPI_MIN:.0f}%) 미달 — 분발 필요"
        st.markdown(
            f"**📈 운용 실력 (주식 TWR 연환산): {twr_ann:.1f}%** "
            f"— KPI {KPI_MIN:.0f}/{KPI_AVG:.0f}/{KPI_MAX:.0f}% 중 {tag}"
        )
    else:
        st.caption("주식 TWR 자동연동 실패 — performance_summary '전체' 행을 확인하세요.")

    # ──────────────────────────────────────────────
    # 자산 궤적 차트
    # ──────────────────────────────────────────────
    st.markdown("### 📈 자산 성장 궤적 (오늘 가치)")
    ages = [kor_age + i for i in range(horizon + 1)]

    # 반대 시나리오도 같은 가정으로 그려 비교
    if scenario == "전세 유지":
        alt_resid = d["half_deposit"]
        alt_extra = max(jeonse - d["half_deposit"], 0.0)
        alt_out = 0.0
        alt_name = "반전세 전환(참고)"
    else:
        alt_resid = jeonse
        alt_extra = 0.0
        alt_out = 0.0
        alt_name = "전세 유지(참고)"
    alt_path = project_path(invest + auction + alt_extra, alt_resid, r_real, horizon, contrib, alt_out)

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

    # ──────────────────────────────────────────────
    # 두 시나리오 × 은퇴시점 비교표
    # ──────────────────────────────────────────────
    st.markdown("### 🆚 시나리오 비교 (목표 = 현재 설정)")

    def scen_metrics(resid_v, extra_v, out_v):
        inv0 = invest + auction + extra_v
        out = dict()
        for yrs, age in [(yrs_50, 50), (yrs_55, 55)]:
            rr = required_cagr(total_target, inv0, resid_v, yrs, contrib, out_v)
            nom = (1 + rr) * (1 + infl / 100) - 1
            fv = project_path(inv0, resid_v, r_real, yrs, contrib, out_v)[-1]
            out[age] = (nom * 100, fv)
        return out, inv0

    v1, v1_inv = scen_metrics(jeonse, 0.0, 0.0)
    v2_dep = half_dep if scenario == "반전세 전환" else d["half_deposit"]
    v2_out = (rent_y if rent_from_invest else 0.0) if scenario == "반전세 전환" else 0.0
    v2, v2_inv = scen_metrics(v2_dep, max(jeonse - v2_dep, 0.0), v2_out)

    import pandas as pd
    tbl = pd.DataFrame([
        {"시나리오": f"전세 유지 (투자 {v1_inv:.1f}억)",
         "50세 필요수익률": f"{v1[50][0]:.1f}%", "55세 필요수익률": f"{v1[55][0]:.1f}%",
         f"55세 예상자산(@{exp_ret:.0f}%)": f"{v1[55][1]:.1f}억"},
        {"시나리오": f"반전세 전환 (투자 {v2_inv:.1f}억)",
         "50세 필요수익률": f"{v2[50][0]:.1f}%", "55세 필요수익률": f"{v2[55][0]:.1f}%",
         f"55세 예상자산(@{exp_ret:.0f}%)": f"{v2[55][1]:.1f}억"},
    ])
    st.dataframe(tbl, use_container_width=True, hide_index=True)
    st.caption(
        f"※ 목표 {total_target:.1f}억 · 물가 {infl:.1f}% · 추가납입 {contrib:.1f}억/년 기준. "
        "'필요수익률'=그 나이에 목표 도달하는 명목 CAGR, '예상자산'=기대수익률로 굴렸을 때 도달액. "
        "현재 실적·자산은 대시보드 자동 연동, 그 외는 위 가정값."
    )
