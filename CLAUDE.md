# 포트폴리오 운영 시스템 — Claude 가이드

> 호섭(Hosub)의 개인 포트폴리오 관리 시스템. Python + Google Sheets + Streamlit + GitHub Actions 로 구성.
> 이 문서는 새 Claude 세션이 빠르게 컨텍스트 파악하라고 만든 가이드.

---

## 🎯 프로젝트 개요

**오너**: 호섭 (hosubkim832@gmail.com)
**구성**: 두 포트폴리오 그룹 운영
- **멘토 포폴** — 멘토 강의 수강 계좌. 한국 위주.
- **HS 포폴** — 본인+와이프 자산. 일반 위탁 + 퇴직연금 혼합.

**기술 스택**:
- 백엔드: Python (pandas, gspread, yfinance, pykrx 일부)
- 데이터: Google Sheets (파일명: `거래내역`)
- 프론트엔드: Streamlit Cloud (`hsportfolio.streamlit.app`)
- 자동화: GitHub Actions (daily cron + manual trigger)
- 외부 API: ECOS (한국은행), yfinance, Naver 모바일 주식 API

---

## 📂 계좌 그룹 (`Dashboard.py` 상단 + `main.py` 에서 사용)

```python
MENTOR_ACCS = ['60271589', '53648897']
HS_ACCS = ['53649012', '856045053982', '220914426167', '717190227129']
PENSION_ACCS = ['220914426167', '717190227129']  # HS 포폴 안의 퇴직연금
```

- **60271589** (멘토) — 일반 위탁, ISA 위주
- **53648897** (멘토) — 일반 위탁, 외화 가능
- **53649012** (HS) — 일반 위탁, USD 가능
- **856045053982** (HS) — 일반 위탁
- **220914426167** (HS) — 퇴직연금 DC, 큰 계좌
- **717190227129** (HS) — 퇴직연금 DC, 작은 계좌

> ⚠️ 퇴직연금 계좌는 **현금 이동 불가** (외부에서 입금 X). 리밸런싱 시 계좌 격벽 고려 필수.

---

## 🗂️ 코드 파일 구조

### Core Pipeline (main.py 실행 시 흐름)
| 파일 | 역할 |
|---|---|
| `main.py` | 메인 실행 (STEP 1~13). raw → 변환 → 보유/손익 계산 → 시트 갱신 → 성과 분석 |
| `config.py` | 시트명, 그룹 상수 |
| `google_api.py` | gspread 헬퍼 (read/write Google Sheets) |
| `data_transformer.py` | raw_domestic/international/체결 → 총계정원장 변환 (`classify_domestic_action`, `transform_international`, `transform_chey`) |
| `account_manager.py` | 계좌별 NIC (Net Invested Capital) 계산 |
| `finance_core.py` | 보유 현황 + 현재가 + 현금 계산 + `fetch_daily_market_data()` (ECOS+yfinance) |
| `rebalancing.py` | 리밸런싱 계산 (target vs current, 매매 필요수량) |
| `backfill.py` | portfolio_log 백필 |
| `report_cash_flow.py` | 자금 흐름 보고서 |
| `performance.py` | 성과 분석 (TWR/MWR, 월/분기, inception 기준) |

### Daily Market Cron
| 파일 | 역할 |
|---|---|
| `update_market_data.py` | `daily-market.yml` 워크플로가 호출. market_data 시트에 한 행 append |
| `backfill_market_data.py` | 과거 시장 데이터 일괄 백필 (수동 실행, 시트 통째로 덮어씀) |
| `ecos_helpers.py` | ECOS API 헬퍼 (KR 10Y, KOSPI/KOSDAQ 거래대금) |

### Dashboard (Streamlit Cloud 배포)
| 파일 | 역할 |
|---|---|
| `Dashboard.py` | 대시보드 메인. 5개 뷰: 전체 / 멘토 포폴 / HS 포폴 / 장중 실시간 / 작전 일지 |

### GitHub Actions
| 파일 | 역할 |
|---|---|
| `.github/workflows/daily-market.yml` | 매일 07:00 KST cron. update_market_data.py 실행 |
| `.github/workflows/run-main.yml` | 수동 트리거 (`workflow_dispatch`). main.py 실행 |

### 일회성 / 디버그 (gitignore'd, GitHub 에 안 올림)
- `theme_remap.py` — master_data 의 theme 일괄 갱신 (이미 47→16 완료)
- `pykrx_diagnose.py`, `ecos_explore.py`, `debug_*.py` 등 — 1회성 진단 스크립트

---

## 📊 Google Sheets 구조 (파일: `거래내역`)

### 사용자가 채우는 시트
| 시트 | 주요 컬럼 | 비고 |
|---|---|---|
| `raw_domestic` | 계좌번호, 거래일자, 거래종류, 적요명, 종목코드, 종목명, 거래수량, 정산금액, **Z=체결, AA=체결일** | **모든 국내 거래 단일 소스**. 체결내역도 여기 paste(Z=Y), 거래내역도 여기 paste(Z=빈칸). 자동 dedup. 설계결정 #12 |
| ~~`raw_체결`~~ | ~~(폐기)~~ | DEPRECATED (2026-05-28) — 설계결정 #12로 폐기. 데이터는 raw_domestic 에 통합. 시트 삭제 가능 |
| ~~`raw_체결_키움`~~ | ~~(폐기)~~ | DEPRECATED (2026-05-28) — 설계결정 #12로 폐기. 사용자 평탄화 수식 + raw_domestic 직접 paste로 대체 |
| `raw_international` | 계좌번호, 거래일자, 적요명, 종목코드, 종목명, 거래수량, 정산금액(외), 통화 | 해외 거래 |
| `master_data` | ticker, name, **theme**, **pension_class**, postion (오타, 작전분류 공격/방어), maket_phase, exchange, currency, country, **military** (군분류 방위군/공군/육군/해군/특수군) | 종목 마스터. 사용자가 수동 관리 |

### main.py 가 생성/갱신하는 시트
| 시트 | 갱신 시점 | 설명 |
|---|---|---|
| `dashboard_data` | STEP 10 | 계좌별 보유 종목 + 현재 평가 (시트 통째로 덮어씀) |
| `rebalancing_data` | STEP 10 | 리밸런싱 계산 결과 (덮어씀) |
| `rebalancing_master` | STEP 8.5 | 사용자가 H열 (target_ratio) 설정. main 이 **I/J/W/X/Z/AC + 헤더 자동 갱신**, **Y/AB/AD 는 LIVE 시트 수식 적재** |
| `portfolio_log` | STEP 12 | 일별 보유 누적 |
| `performance_summary` | STEP 13 | 기간별 TWR/MWR/손익 |
| `cash_flow_log` | STEP 12.5 | 자금 흐름 |

### Cron 이 갱신하는 시트
| 시트 | 갱신 시점 | 설명 |
|---|---|---|
| `market_data` | 매일 07:00 KST | 시장 지표 일별 누적 (39 컬럼: KOSPI/KOSDAQ/SP500/NASDAQ/NIKKEI/Shanghai/DAX/환율/채권/원자재/VIX/BTC/거래대금/KR10Y 등) |

### Dashboard 가 생성/갱신하는 시트
| 시트 | 갱신 시점 | 설명 |
|---|---|---|
| `journal_log` | 작전 일지 저장 시 | date, updated_at, 시장요약, 경제지표, 시장이슈, **매매내역**, 전투일지, 전투계획 |

### `rebalancing_master` 컬럼 상세 (자주 참조)
| 열 | 이름 | 출처 |
|---|---|---|
| A | Num | 수동 |
| B | account | 수동 |
| C | ticker | 수동 |
| D | name | 수동 |
| E | theme | 수동 (master_data 와 일치) |
| F | **military** | 수동 또는 수식 (master_data L열에서 가져옴) — 방위군/공군/육군/해군/특수군 |
| G | Country | 수식 (master_data D열에서 가져옴) — 한국/미국/중국/코인/헷지 |
| H | target_ratio | **수동** (사용자가 조정) |
| I | Actual_Ratio | main.py 자동 |
| J | Drift | main.py 자동 (= H - I) |
| K | 국가별 Gross (목표) | 시트 수식 (= SUMIF(G, country, H)) |
| L | 국가별 Gross (현재) | 시트 수식 (= SUMIF(G, country, I)). 헷지 그룹 = `=SUM(I71:I74)` 등 |
| M | 국가별 Long (목표) | 시트 수식 = `SUMPRODUCT((G=country)*H*AA) / S` |
| N | 국가별 Long (현재) | 시트 수식 = `SUMPRODUCT((G=country)*I*AA) / T` |
| O/P | 현금 (목표/현재) | 시트 수식 (`=1 - SUM(L 그룹별)`) |
| Q/R | Gross (목표/현재) | 시트 수식 (= Long + 방위군 자본) |
| S/T | **Long (목표/현재)** | 시트 수식 = `SUMPRODUCT($H or $I, $AA)` |
| U/V | **Net (목표/현재)** | 시트 수식 = `Long − SUMIF(VIX)*3 − SUMIF(방위군 ¬VIX)*1` |
| W | 계좌 AUM (%) | main.py 자동 |
| X | 계좌 가용현금 (%) | main.py 자동 |
| Y | 계좌 공간 (%p · +면 초과) | **LIVE 시트 수식** = `SUMIF($B,$B,$H)−W` (H 수정 시 즉시 갱신) |
| Z | 퇴직연금 위험% 현재 | main.py 자동 (퇴직연금 행만) |
| **AA** | **Long_weight** | main.py 자동 — pension_class+military 기반 0.0~1.0 |
| AB | 퇴직연금 타겟 위험% (≤70%) | **LIVE 시트 수식** = `SUMPRODUCT(--($B=$B),$H,$AC)/W` (퇴직연금 행만) |
| AC | risk_weight | main.py 자동 — `_risk_ratio_from_pc` 기반 0.0~1.0 (AB 수식용 헬퍼) |
| **AD** | **🎯 타겟 조정 여력** | **LIVE 시트 수식** — Y·AB 통합 판정: 🟢+X%p / 🟡꽉참 / 🔴초과 |

> ⚠️ F열 헤더 명명: 원래 `postion` (오타) → `military` 로 변경 완료 (2026-05-19).
> main.py 가 `row.get('military') or row.get('postion')` fallback 으로 둘 다 매칭.
> ⚠️ Y/AB/AD 는 main.py 가 **수식**을 적재 (USER_ENTERED). W/X/Z/AC 는 값. 자세한 건 설계결정 #9 참고.

### `journal_log` 매매내역 컬럼 형식
파이프(`|`) 구분 8필드, 줄바꿈으로 여러 거래:
```
계좌|그룹|매매|종목명|가격|정산금액|그룹비중|이유
```
예시:
```
60271589|멘토|매수|삼성전자|₩59,100|₩590,100|0.45%|목표 비중 도달
60271589|멘토|매도|바이오ETF|₩23,100|₩2,310,000||실적 미달
```
※ Backward compat: 5필드 (구) / 6필드 (중간) / 8필드 (현재) 모두 파싱 가능.

---

## 🎨 핵심 설계 결정

### 1. Theme 16 그룹 (master_data E열)
47개 → 16개로 통합. master_data 의 theme 은 다음 중 하나:
```
AI/테크 | 반도체 | 자동차/로봇 | 2차전지 | 전력/인프라 | 화학/소재 |
K-컬쳐 | 소비재 | 레저 | 금융/밸류업 | 방산/우주 | 조선 |
바이오 | 헷지 | 패시브 | 크립토 (+ 시스템: 현금)
```
> 자세한 매핑은 `theme_remap.py` 참고.

### 2. Pension Class (master_data F열) — 두 가지 측면 동시 사용

**(a) 한국 퇴직연금 규제용** (rebalancing_master Z열·AC열, `_risk_ratio_from_pc`):
| 값 | 위험비중 |
|---|---|
| `안전`/`채권`/`국채`/`MMF`/`현금` | 0% |
| `채권혼합` | 0% (한국 규정: 주식 ≤40% 채권혼합 ETF 는 100% 안전) |
| `헷지` (인버스/VIX) | 100% |
| `위험`/`주식` | 100% |
| **빈칸** | **100% (보수적 default)** |
| 숫자 (`15`, `채권혼합50`) | 그 % 만큼 위험 |

**(b) Long 자본 분포용** (rebalancing_master AA열, `_long_weight_from_pc`):
| 값 | Long weight |
|---|---|
| `안전`/`채권`/`국채`/`MMF`/`현금` | 0.0 |
| `헷지`/`인버스`/`VIX`/`레버리지` | 0.0 (방위군) |
| `채권혼합` | **0.3** (호섭 정책: 30% 만 Long 자산으로 인정) |
| `위험`/`주식`/`공격` | 1.0 |
| 빈칸 | 1.0 (보수적 default) |
| 숫자 (`15`, `채권혼합20`) | 그 % / 100 |

> 두 함수가 같은 pension_class 값을 다르게 해석함. 이건 의도된 설계:
> - 헷지 = 퇴직연금 규제상 "위험 100%" + Long 자본 측면 "0" (방위 효과)
> - 채권혼합 = 퇴직연금 규제상 "안전 0%" + Long 자본 측면 "30% Long"

### 3. 한국 퇴직연금 규제 (220914426167, 717190227129)
- 위험자산 ≤ 70%, 안전자산 ≥ 30% (계좌 단위, 분모 = 계좌 AUM 현금 포함)
- Dashboard 의 "퇴직연금 가드" 섹션 + rebalancing_master Z열에서 모니터링

### 4. Inception Dates (`performance.py`)
```python
CUSTOM_START_DATE = '2025-05-14'  # 멘토 2기 시작
GROUP_INCEPTION_DATES = {
    '멘토 포트폴리오': '2025-05-14',
    'HS 포트폴리오':   '2025-07-21',
}
MILESTONE_DATES = {
    '3기 시작': '2025-10-29',
    '4기 시작': '2026-05-18',
}
```
→ performance_summary 에 `지정(25-05-14~)`, `지정(25-07-21~)`, `지정(25-10-29~)`, `지정(26-05-18~)` 컬럼 자동 생성.

### 5. KST 타임존
Streamlit Cloud 서버는 UTC 기본. `Dashboard.py` 에 `now_kst()` 헬퍼 정의 — 모든 시간 표시는 이걸로 통일.

### 6. KPI 목표
연간 수익률 목표: 최소 10% / 평균 15% / 최대 20%. TWR 기준 (자금흐름 자동 제거).
- Dashboard 의 "2026 연간 KPI 진행" 섹션: 5개 메트릭 카드 (현재 YTD TWR, **YTD 손익 ₩**, 최소/평균/최대 목표 진척).

### 7. 시장 데이터 (market_data) 컨벤션
- **row 의 date 라벨 = 관찰일 (KST)**, **row 의 값 = 직전 거래일 종가**.
- 즉 5/15 행 = 5/14 종가 데이터. cron 이 KST 07:00 에 돌며 today 라벨 + yesterday 종가 적재.
- `finance_core.py` 의 `fetch_daily_market_data()`:
  - `datetime.now(KST)` 로 today 결정 (UTC 함정 회피)
  - yfinance period="5d" → today 행 intraday 필터로 제외
  - `get_naver_index_previous_close()` 로 KOSPI/KOSDAQ 누락 시 Naver 보충 (price + chg_pct)
  - **휴장일 판별**: Naver traded_date 가 yesterday_kst 와 정확히 같을 때만 fallback 작동
- `fill_single_day_market_data.py` 도 같은 컨벤션 (`X 행 = X−1 거래일 종가`).

### 8. Long / Net / Gross 시스템 (rebalancing_master)
시트 K~V + AA 컬럼이 함께 자본 분포 계산:
- **Long** = `SUMPRODUCT(H or I, AA)` — AA 가중치 적용 (채권혼합 30%, 방위군 0)
- **Gross** = Long + 방위군 자본 (위험 노출 자본 = Risk Gross)
- **Net** = Long − VIX×3 − 일반 인버스×1 (헷지 후 순 노출)
- **현금 (O/P열)** = 100% − Gross 그룹별 합
- AA (Long_weight) 는 main.py 가 자동 적재 — 사용자 손 안 댐
- 시트의 K (국가별 Gross) 와 M (국가별 Long) 는 의미 다름:
  - K/L = % of NAV (헷지 + 채권혼합 100% 다 포함)
  - M/N = % of Long (헷지 제외, 채권혼합 30% 만)

### 9. 계좌 capacity / 퇴직연금 위험 모니터링 (rebalancing_master W~AD)
시트 W~AD 컬럼이 "이 종목 타겟을 더 올려도 되나" 를 판정:
- **W (계좌 AUM %)** / **X (가용현금 %)** — main.py 자동, 실제 보유 스냅샷
- **Y (계좌 공간)** = `SUM(H) − W` — LIVE 수식. + 면 격벽 초과, − 면 여유. "계좌 총금액 초과" 와 "현금 동났는데 타겟 계속 올림" 은 수학적으로 같은 조건 (둘 다 Y>0)
- **Z (퇴직연금 위험% 현재)** — main.py 자동, **실제 보유** 기준 위험비중
- **AB (퇴직연금 타겟 위험%)** = `SUM(H×AC)/W` — LIVE 수식. **target 대로 다 채웠을 때** 위험비중. 70% 넘으면 규제 위반
- **AC (risk_weight)** — main.py 자동 (`_risk_ratio_from_pc`), AB 수식용 헬퍼 (헷지=1.0, 안전/채권혼합=0.0)
- **AD (🎯 타겟 조정 여력)** — LIVE 수식. Y·AB 를 한 줄로 통합 판정. 위험 종목(AC>0)+퇴직연금이면 둘 다, 안전 종목/비퇴직연금이면 Y만 따져 더 빡빡한 값. 🟢+X%p / 🟡꽉참 / 🔴초과
- 사용자는 H 편집 시 **AD열만** 보면 됨 (Y/AB 직접 안 봐도 됨)
- ⚠️ AB/AD 는 **타겟(목표)** 기준. **Z(현재)** 는 별개 — 이미 70% 넘은 계좌는 AD 가 🟢 여도 매도해서 내려야 함
- Y/AB/AD 가 LIVE 수식인 이유: H 만 수정해도 main.py 안 돌리고 즉시 반영. W/Z 는 실제 보유 기준이라 main.py 가 갱신 (시장 크게 움직이면 main.py 재실행 필요)
- AB 수식은 SUMPRODUCT **쉼표(,) 형태** — H 범위에 텍스트 섞여도 0 처리 (`*` 곱하기형은 #VALUE! 터짐)

### 10. 국가별 Net 차트 (Dashboard 비중 섹션)
Dashboard "🥧 비중" 섹션 5개 차트 중 3번째. **헷지를 대상 국가에서 음수 차감한 순노출**.
- 차트 순서: 국가별 NAV → 국가별 Long → **국가별 Net** → 테마별 → 군종/그룹별
- **국가별 Long** = 헷지 0 처리한 순수 베팅 분포 (도넛, `_attach_long_mv`)
- **국가별 Net** = Long − 헷지 (세로 막대그래프, `make_net_bar()` + `_attach_net_mv`)
- 헷지 → 대상국 추론: **종목명 키워드** (`_hedge_target()`)
  - 미국: VIX/S&P/SP500/나스닥/NASDAQ/미국 · 한국: 코스닥/코스피/KOSPI/KOSDAQ/K200/인버스
  - 원자재 인버스(은선물/골드/원유 등)는 제외 → Net 에서 0 처리
- **차감배수**: VIX 류 = ×3 (변동성 레버리지), 그 외 일반 인덱스 인버스 = ×1 (`_HEDGE_X3_KW`)
- 막대값 = **Long 총액 대비 %** — Long 도넛과 분모 같아 직접 비교 가능 (도넛 100% 재정규화 착시 없음)
- 순매도(헷지>Long) 국가는 0선 아래 빨간 막대. 도넛은 음수 슬라이스를 못 그려 사라짐 → 막대로 교체한 이유
- ⚠️ 종목명 추론이라 새 헷지 추가 시 키워드 안 맞으면 누락 — `🔍 Long 도넛 디버그` expander 의 "헷지 종목 → 국가 추론 검증" 표에서 확인

### 11. raw_체결 시스템 — 국내 매매를 체결내역 기반으로 (2026-05-26) — DEPRECATED
> ⚠️ **2026-05-28에 폐기됨 — 설계결정 #12로 대체.** 복잡성 비용이 가시성 이득보다 컸음. 이 섹션은 역사적 기록으로 보존.

국내 보통매매 매수/매도를 거래내역(raw_domestic) 대신 **체결내역(`raw_체결` 시트)** 으로 처리.

**왜:** 거래내역은 결제일(T+2) 기준이라 2영업일 늦게 들어옴. 체결내역은 체결일 당일 확보 → 비중·수익률을 즉시 정확히 봄.

**소스 분리 — 중복/dedup 없음:**
| 거래 종류 | 소스 |
|---|---|
| 국내 보통매매 매수/매도 | `raw_체결` only |
| 입출금·배당·환전·이자·입고·출고·분할·재투자 | `raw_domestic` only |
| 해외 거래 전부 | `raw_international` (변경 없음, T+2 유지) |

- `transform_domestic(exclude_market_trades=True)` — 거래종류에 '보통매매' 있고 'OTC' 없는 행 제외. 재투자·입고·분할은 '보통매매'가 아니라 자동 유지. OTC 매매도 거래내역에 유지.
- `transform_chey()` — `raw_체결` → 총계정원장. 정산금액 있으면 그대로(과거 정확값), 없으면 거래대금(수량×단가)±수수료 추정. 상수 `CHEY_COMMISSION_RATE`(0.015%)·`CHEY_TAX_RATE`(매도 0.15%).
- `main.py`: 매매=raw_체결, **현금 계산(`calculate_cash_balances`)에도 raw_체결 포함**, `audit_chey_vs_domestic()`가 거래내역↔체결 수량 대조해 경고(결과엔 영향 없음).

**분할체결 통합:** 같은 날·종목·방향 여러 체결을 (계좌·체결일·종목·매매구분) 단위로 1행 통합. 수량 합·정산금액 합 보존 → 신·구 결과 동일(`verify_chey.py` 검증 완료).

**날짜 컨벤션:** 체결내역=체결일, 거래내역=결제일(T+2). 섞이지만 매매를 raw_체결 한 곳에서만 읽으므로 무해. 과거 마이그레이션분은 거래내역의 거래일자(결제일) 그대로 유지 → 전환 지점에 1회성 2일 이음새(무시 가능).

**스크립트:** `migrate_chey.py`(과거 매매 raw_domestic→raw_체결 1회 이관, 분할체결 통합) / `verify_chey.py`(신·구 대조 검증, 시트 안 건드림). 둘 다 git 에 포함.

**키움 체결내역 입력 (`raw_체결_키움`):** 키움 체결내역은 2줄/건 raw 포맷이라 별도 시트 `raw_체결_키움`에 **가공 없이 그대로** 붙여넣음(A열에 계좌번호만 수기). `flatten_kiwoom_chey()`가 2줄→1줄 평탄화 + (계좌·체결일·종목·매매구분) 통합. main.py STEP 1·Dashboard 작전일지 둘 다 raw_체결_키움을 읽어 raw_체결과 합침 — **읽기 전용(시트 간 데이터 이동 없음)이라 중복 위험 0**. 미래에셋 퇴직연금 매매는 raw_체결에 직접 수기입력.

**작전일지:** Dashboard 작전일지 '매매 다시 불러오기'는 raw_체결을 읽고 **직전 영업일** 매매를 가져옴 — `_prev_business_day()`가 raw_체결 체결일을 거래일 달력으로 사용해 휴일·주말 자동 처리.

> ⚠️ raw_domestic 엔 체결내역 붙여넣지 말 것 — 거래내역만. 체결은 raw_체결로.
> ⚠️ raw_체결 정산금액 칸은 비워두면 main.py가 계산(수수료 추정). 채워두면 그 값 그대로.

### 12. D 옵션 — 단일 raw_domestic 통합 (2026-05-28) ⭐ CURRENT
설계결정 #11(raw_체결 시스템)은 복잡성 비용이 가시성 이득보다 커서 **롤백 + 단순화**. 모든 국내 거래를 raw_domestic 하나에서 처리.

**구조:**
- raw_domestic 에 컬럼 2개 추가:
  - **Z열 `체결`** — 체결내역 paste 행만 `Y` 표시 (거래내역 paste 행은 빈칸)
  - **AA열 `체결일`** — 사용자 수식 `=IF($Z2="Y", $B2, WORKDAY($B2, -2))` (Y면 거래일자 그대로, 빈칸이면 −2영업일)
- 체결내역/거래내역 모두 raw_domestic 에 paste, 분리된 시트 없음
- 키움 2줄 raw 는 사용자의 기존 평탄화 수식으로 1줄로 만들어서 paste

**자동 dedup (`_apply_chey_dedup` in `data_transformer.py`):**
- 보통매매 매수/매도 행만 대상
- 같은 `(계좌·체결일·종목·매수매도)` 에 체결(Y) + 거래내역(빈칸) 둘 다 있으면 → 체결 행 자동 무시(거래내역 우선)
- 분할체결만 있는 경우엔 모두 유지 (qty 합산)
- 비매매 행(입출금/배당/환전/재투자/입고/분할)은 손대지 않음

**날짜 처리:**
- 보통매매 행의 `거래일자` 를 코드가 `체결일` 값으로 덮어쓰기 (이후 처리 모두 체결일 기준)
- WORKDAY 는 주말만 처리. 한국 휴장일 끼면 1일 어긋날 수 있음 → 사용자가 해당 셀 직접 수정
- 비매매 행은 `거래일자` 그대로 사용

**Dashboard 작전일지:**
- `raw_domestic` 하나만 읽음 (체결일 기준 dedup 동일 적용)
- `_prev_business_day` — raw_domestic 의 보통매매 체결일 중 `max(< sel_date)`
- `_load_chey_all` 은 더 이상 안 쓰임 (dead code)

**폐기된 것:**
- `raw_체결`, `raw_체결_키움` 시트 — 안 씀 (사용자 판단으로 삭제 가능)
- 함수: `transform_chey`, `flatten_kiwoom_chey`, `audit_chey_vs_domestic`, `absorb_kiwoom_chey`, `get_raw_values` — dead code
- 일회성 스크립트: `migrate_chey.py`, `verify_chey.py`, `fix_chey_codes.py` — 보관 또는 삭제

> ⚠️ **종목코드 leading 0 주의:** Google Sheets paste 시 6자리 숫자 종목코드의 앞자리 0이 떨어지는 경우 있음 (예: 086280 → 86280). 발견하면 해당 셀 수기 수정 (어퍼스트로피 prefix `'086280` 으로 텍스트화).

---

## 🔐 환경 변수 / Secrets

### 로컬 (gitignored)
- `.env` — `ECOS_API_KEY=...`
- `service_account.json` — Google Sheets 인증 키

### GitHub Secrets (Actions 용)
- `GCP_SA_JSON` — service_account.json 전체 내용
- `ECOS_API_KEY` — ECOS API 키

### Streamlit Cloud Secrets
- `gcp_service_account` (또는 `GCP_SA_JSON`) — Google Sheets 접근
- `GITHUB_PAT` — Dashboard 에서 `▶️ main.py 실행` 버튼 클릭 시 GitHub Actions 트리거용

---

## 🛠️ 자주 하는 작업

### A. 매일 아침 시장 체크 + 일지
1. cron 이 07:00 KST 자동 갱신 (대기, 안 건드림)
2. Dashboard → `📓 작전 일지` → 오늘 날짜
3. 섹션 A (자동) / B C 수동 입력 / D 위 `📥 raw 시트에서 매매 다시 불러오기` 클릭
4. D 표의 이유 컬럼에 입력 (셀 클릭 → 타이핑 → **Enter** commit)
5. E F 입력 → 💾 저장

### B. 새 거래 입력 후 portfolio 갱신
1. 증권사 거래 입력 — **모두 `raw_domestic`** 한 시트로:
   - **국내 체결내역** (당일 가시성) → 평탄화 수식으로 1줄/건 만들어서 raw_domestic 끝에 paste, **Z열에 Y 마킹**
   - **국내 거래내역** (T+2 도착) → 그대로 paste, Z열 빈칸 (AA 체결일 수식이 자동으로 −2영업일 계산)
   - **해외 거래** → `raw_international` (변경 없음)
   - 같은 거래의 체결+거래내역이 나중에 둘 다 있으면 → 코드가 자동 dedup (거래내역 우선)
2. Dashboard 사이드바 → `▶️ main.py 실행` (GitHub Actions 트리거)
   - 또는 GitHub 앱 → Actions → "Run main.py (manual)" → Run workflow
3. 1-2분 대기 (✅ 알림 옴)
4. Dashboard → `🔄 데이터 새로고침` 클릭

### C. Target Ratio 조정 (리밸런싱 목표 변경)
1. Google Sheets → `rebalancing_master` 시트
2. H열 (target_ratio) 직접 편집 — 이때 **AD열 (🎯 타겟 조정 여력)** 보면서: 🟢 면 그 숫자만큼 올려도 OK, 🔴 면 멈춤 (Y/AB/AD 는 LIVE 수식이라 H 만 고쳐도 즉시 갱신, main.py 안 돌려도 됨)
3. main.py 실행 (위 B 참조) — W/Z 등 실제 보유 기준 값 갱신
4. Dashboard → HS 포폴 → 종목별 리밸런싱 표
   - AD열이 전부 🟢 면 정상
   - Z열 (퇴직연금 위험% 현재) 가 70% 이내여야 정상 (이미 넘었으면 매도 필요)

### D. 코드 수정 → 배포
1. VSCode 에서 코드 수정
2. Source Control 패널 (`Ctrl+Shift+G`) → 메시지 입력 → Commit & Sync
3. Streamlit Cloud 자동 재배포 (~1분)
4. (필요시) Dashboard 의 `🔄 데이터 새로고침`

> ⚠️ **모든 코드 수정 후 commit & sync 안 하면 Streamlit 에 반영 안 됨**. 자주 까먹는 함정.

---

## 🐛 알려진 Quirks / 함정

### 1. yfinance MultiIndex (Dashboard.py 에서 처리됨)
최신 yfinance 는 single ticker 도 MultiIndex 컬럼 반환. `get_fx_to_krw` / `get_yf_batch` 에서 평탄화 처리:
```python
if isinstance(rate_data.columns, pd.MultiIndex):
    rate_data.columns = [c[0] for c in rate_data.columns]
```

### 2. Google Sheets 날짜 locale 변환
`value_input_option='USER_ENTERED'` 사용 시 "2026-05-08" 을 자동으로 날짜 객체로 파싱 → "2026. 5. 8." 같은 locale 표시.
- **해결**: journal_log 저장은 `value_input_option='RAW'` 사용
- 로드 시 `_normalize_date_str()` 으로 다양한 포맷 → YYYY-MM-DD 통일

### 3. Streamlit `st.form` + `st.data_editor` 입력 손실
form 안의 data_editor 는 user 의 cell 입력이 form 제출 시 누락될 수 있음.
- **해결**: `st.form` 대신 `st.container` 사용

### 4. Streamlit data_editor + dynamic data → edits reset
매 rerun 마다 새 DataFrame 객체 생성 시 data_editor 가 user edits 를 reset.
- **해결**: trades_init 을 `st.session_state` 에 캐시 + 저장 성공 시 무효화

### 5. pykrx 1.0.51 깨짐
- `get_index_ohlcv_by_date` → KeyError('지수명')
- `get_customer_deposit_trend` → 라이브러리에서 제거
- **해결**: 모두 ECOS API 로 이전 (KR 10Y, 거래대금)
- 고객예탁금/신용잔고 데이터는 포기 (ECOS/BOK 미제공, KOFIA 스크래핑은 별도 작업)

### 6. main.py 의 STEP 9.5 (시장 데이터 backfill) 는 비활성
이전엔 main.py 가 매번 market_data 시트를 통째로 덮어썼는데, 이제 GitHub cron 이 매일 append 함. 중복/충돌 방지로 STEP 9.5 주석 처리됨.

### 7. Streamlit Cloud cache (load_sheet 10분 TTL)
시트 직접 수정 후 dashboard 반영 안 보이면 사이드바 `🔄 데이터 새로고침` 클릭.

### 8. OneDrive 폴더 경로 한글/공백
워크스페이스: `C:\Users\kmhos\OneDrive - 현대번역 (1)\MyPortfolio`
- 셸 명령 시 항상 따옴표 `"..."` 사용
- VSCode 터미널이 안전

### 9. yfinance 한국 인덱스 EOD 누락 (Yahoo Finance 의 알려진 이슈)
yfinance 가 KOSPI(`^KS11`)/KOSDAQ(`^KQ11`) 의 특정 거래일을 NaN 으로 누락하는 경우 잦음.
- **해결**: `get_naver_index_previous_close()` 함수가 자동 fallback (price + chg_pct 둘 다)
- `fetch_daily_market_data()` 안에서 KOSPI/KOSDAQ NaN 감지 시 Naver API 호출 → 보충
- chg_pct override 도 함께 (yfinance dropna 후 1개만 남으면 chg=0 되는 버그 회피)

### 10. Naver chg_pct override **제거** (5/18, 5/19 두 번 사고)
Naver 모바일 API 가 가끔 direction code 잘못 줘서 chg 부호 반대로 들어가는 사고 두 번 발생:
- 5/18 KOSPI: 시장 −6.12% 인데 시트엔 +6.97%
- 5/19 KOSDAQ: 시장 −1.68% 인데 시트엔 +1.71%

**fix (2026-05-19)**:
1. **Naver chg_pct override 자체 제거** — Naver 는 **price 만 보충**. chg 는 `yfinance series.dropna()` 의 `iloc[-1]/iloc[-2]` 자동 계산.
2. **휴장일 판별 엄격화** (이중 안전망): Naver traded_date 가 yesterday_kst 와 **정확히 같을 때만** fallback. 어제가 휴장이거나 Naver 가 비정상 timestamp 주면 스킵.

**Trade-off**: yfinance 가 며칠 연속 NaN 줄 때 (예: 5/13 KOSPI 같은 사례) chg 가 며칠 전 종가 vs 어제 비교가 되어 부정확할 수 있음. 하지만 **가격은 항상 정확** + **chg 부호 안 뒤집힘** — 잘못된 방향보다 안전.

`naver_chg_override` 변수와 final_row_data 의 override 코드는 그대로 남아있음 (dict 비어있어서 작동 안 함, 미래 다른 용도 가능).

### 11. dashboard_data 의 'position' vs rebalancing_master 의 'postion' (오타)
두 시트의 같은 의미 컬럼이 명명 다름. Dashboard 코드는 **3중 fallback**: postion / position / military.
- master_data L열 `military` 가 가장 정확 (방위군/공군/육군/해군/특수군)
- 방위군 종목 (인버스/VIX) 의 pension_class 가 빈칸이어도 military 컬럼으로 자동 감지됨

### 12. master_data 의 종목별 pension_class 빠짐 함정 (5/15 사고)
새 ETF 추가 시 pension_class 빈칸 두면 → "위험 100%" + "Long weight 1.0" 으로 잡혀 도넛/규제 다 왜곡.
- **반드시 채울 것**: 새 종목 추가 시 master_data 의 F열 (pension_class) 정확히 입력
- 특히 헷지 (인버스/VIX) 는 `헷지` 명시 — 빈칸이면 도넛에 한국/미국 그룹으로 잘못 끼어듦
- Dashboard 코드는 `military='방위군'` fallback 으로 안전망 있음 (덕분에 530130 같은 사례 잡힘)

### 13. `fill_single_day_market_data.py` 의 pct_change + ffill 함정 (5/19 사고)
fill 스크립트의 pct_change 가 휴장일 NaN 행 만나면 그 다음 거래일 chg 도 NaN → ffill 로 이전 chg 가 복사됨.
- 예: 5/19 row 의 KOSPI chg 가 5/18 의 chg 가 아니라 5/15 의 chg (-6.12%) 가 들어감
- **fix (2026-05-19)**: pct_change 전에 가격 ffill 적용. `_ffilled = df_final[price_col].ffill(); chg = _ffilled.pct_change()`
- cron 의 `finance_core.get_val()` 은 `dropna().iloc[-1]/[-2]` 방식이라 NaN 무관하게 정확 (fill 스크립트와 다른 로직).

### 14. GitHub Actions schedule cron 의 5분~1시간 지연
GitHub free tier 의 known limitation. KST 07:00 정밀 트리거 위해 외부 cron 서비스 사용 중.
- **cron-job.org** → GitHub workflow_dispatch API 호출 (~1분 정확도)
- daily-market.yml 의 `schedule:` 블록 제거됨 (외부 cron 만 사용)
- PAT: fine-grained, Actions:write + Contents:read, repo=HSO2_portfolio 만 액세스
- Node.js 20 → 24: workflow yml 에 `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` env 추가됨

### 15. rebalancing_master 열 순서 변경(드래그) 금지
main.py 가 시트 열을 **번호로 하드코딩**해서 씀 (W=23 … AD=30). 열을 드래그로 옮기면 다음 main.py 실행 때 엉뚱한 열에 값이 써져 **조용히 다 깨짐**. 수식 안 `$W` `$AB` `$AC` 참조도 어긋남.
- 화면 정리는 **열 숨기기**로 (위치 안 바뀜 → 안전). 헬퍼 열 AA(Long_weight)·AC(risk_weight) 숨기면 Z·AB·AD 가 붙어 보임.
- 진짜 열 순서를 바꾸려면 main.py 를 열 번호 대신 **헤더 이름 기반**으로 리팩토링해야 함 (미완 — 백로그).

---

## 🚀 Git / 배포 정보

- **GitHub repo**: `https://github.com/hosoo832/HSO2_portfolio` (public)
- **Streamlit URL**: `hsportfolio.streamlit.app` (또는 유사)
- **git config (1회 완료)**: user.name "Hosub Kim" / user.email "hosubkim832@gmail.com"

### gitignore 에서 제외하는 것
- `service_account.json`, `.env`, `*.key`, `*.pem` (보안)
- `__pycache__/`, `*.pyc`, `.venv/`, `venv/` (Python)
- `.vscode/`, `.idea/`, `*.swp` (에디터)
- `portfolio_backfill_data.csv` (10MB, GitHub 부담)
- `theme_backup_*.csv`, `ECOS API.txt` (개인/민감)
- `debug_*.py`, `check_*.py`, `test_naver.py`, `ecos_explore*.py`, `pykrx_*.py`, `theme_remap.py` (1회성 스크립트)

---

## 💬 Claude 와의 작업 스타일 (호섭 선호)

### 톤
- **한국어 중심**, 영어 기술 용어 섞기 OK
- 캐주얼한 친구 톤. "~해", "~야", "~줄게"
- 너무 격식 차린 존댓말 X

### 응답 형식
- **표** 적극 활용 (비교/매핑할 때)
- 코드 변경 시 **변경 전/후 명확히**
- 긴 응답보단 **요점 → 디테일** 구조
- 헤딩 / 불릿 / 굵게 활용
- 이모지는 헤딩이나 강조에 가끔 (남발 X)

### 사고 패턴
- **결정적이고 솔직한 의견** 제공 (단순 "yes" 보단 "내 추천은 X, 이유는 Y")
- 위험 있으면 사전에 경고
- 코드 변경 시 **부작용 / 영향 범위** 짚어주기
- 사용자가 모르는 가정/함정 발견 시 즉시 알림

### 워크플로우
- 사용자는 VSCode + git 사용. 코드 수정 후 **항상 commit & sync 안내**
- 사용자가 모바일에서도 작업 가능. 모바일 친화적 안내 추가
- 디버그 시 진단 정보 (캡처/로그) 요청 → 보고 정확히 fix
- 큰 변경은 **단계별** 적용 (한 번에 다 X)

---

## 🎯 현재 시점의 todo / 백로그 (2026-05 기준)

### 거의 완료된 것
- ✅ Streamlit dashboard 5뷰 (전체/멘토/HS/장중실시간/작전일지)
- ✅ Daily market cron (ECOS + yfinance + Naver fallback)
- ✅ main.py 자동화 (GitHub Actions manual trigger)
- ✅ 작전 일지 시스템 (journal_log, raw 자동 import)
- ✅ 퇴직연금 가드 (pension_class 기반)
- ✅ 리밸런싱 표 + capacity 분석 (W/X/Y/Z 자동)
- ✅ KST 타임존 통일
- ✅ git 셋업 + VSCode workflow
- ✅ **시장 데이터 안정화 (2026-05)**: KST 라벨링 + intraday 필터 + Naver fallback (price+chg) + 휴장일 판별
- ✅ **cron-job.org 정밀 트리거 (2026-05)**: GitHub 5분~1시간 지연 우회, fine-grained PAT, KST 07:00 ±1분
- ✅ **Long_weight (AA열) + 시트 M/N (% of Long) 시스템 (2026-05)**: SUMPRODUCT 수식 + 채권혼합 30% 정책
- ✅ **Dashboard 비중 차트 5개 (2026-05)**: 국가별 NAV / 국가별 Long / 국가별 Net / 테마별 / 군종별
- ✅ **KPI YTD 손익 카드 (2026-05)**: TWR % 옆에 ₩ 손익 동시 표시
- ✅ **분류 체계 정리 (2026-05)**: master_data theme="채권혼합" 분리, L열 military 컬럼, rebalancing_master F열 military 명명
- ✅ **타겟 조정 여력 시스템 (2026-05-24)**: Y 라이브 수식화, AB(퇴직연금 타겟 위험%)·AC(risk_weight)·AD(🎯 통합 판정) 신설 — H 편집 시 main.py 없이 즉시 판정. 헤더 always-write + 이름 직관화
- ✅ **국가별 Net 차트 (2026-05-24)**: Long 옆에 헷지 차감 순노출 — 종목명으로 헷지→대상국 추론(코스닥/코스피→한국, 나스닥/S&P/VIX→미국), VIX ×3 / 인덱스 인버스 ×1, 세로 막대그래프(Long 총액 대비 %). 설계결정 #10

### 미해결 / 발전 가능
- ⏳ 고객예탁금/신용잔고 데이터 — ECOS/BOK 미제공, KOFIA 스크래핑 필요 (보류)
- ⏳ 작전 일지 누적 분석 — 1년치 모이면 패턴 분석 (후회 패턴, 성공 매매 공통점) AI 분석 가능
- ⏳ 알림 봇 — 매일 저녁 작전 일지 안 쓰면 알림 / 위험비중 65%+ 시 알림
- ⏳ PDF 주간 리포트 자동 생성
- ⏳ 마이너 리팩토링 — finance_core dead code 정리, 캐시 전략 점검

---

## ⚠️ Claude 가 하지 말아야 할 것

1. **사용자 허락 없이 main.py 실행하지 말 것** — 1-2분 소요, 시트 갱신됨
2. **rebalancing_master 시트의 H열 (target_ratio) 임의 수정 금지** — 사용자가 직접 결정
3. **theme 일괄 변경 금지** — 이미 16개로 통합 완료, 변경 시 사용자와 합의 필수
4. **journal_log 임의 수정 금지** — 사용자의 일지 데이터
5. **`.env` / `service_account.json` 내용을 코드에 하드코딩하거나 GitHub push 금지**
6. **GitHub 에 대량 commit 시 항상 사용자 확인** — git 히스토리는 영구적

---

## 📞 새 세션 시작 시 권장 인사

```
안녕, 호섭. 너의 포트폴리오 운영 시스템 컨텍스트 다 파악했어.
- 멘토 포폴 (60271589, 53648897) + HS 포폴 (53649012, 856045053982, 220914426167, 717190227129)
- Dashboard (Streamlit), main.py (GitHub Actions), 작전 일지 시스템 다 셋업됨
- 어떤 작업 도와줄까?
```

---

> 📝 이 문서는 살아있어. 새 결정 / 기능 추가 / quirks 발견되면 이 파일도 같이 업데이트하면 좋아.
> 마지막 갱신: **2026-05-28** (D 옵션 — 단일 raw_domestic 통합, raw_체결 시스템 폐기)
>
> ### 갱신 이력
> - 2026-05-11: 작전 일지 시스템 + Cowork 프로젝트 분리
> - 2026-05-19: market data 대대적 안정화 (KST + intraday + Naver fallback price/chg/휴장), cron-job.org 정밀 트리거, Long_weight (AA열) + 시트 M/N (% of Long) 시스템, Dashboard 비중 도넛 4개 (NAV/Long/테마/군종), KPI YTD 손익 카드, master_data L열 military + rebalancing_master F열 military 명명, Quirks #9~13 추가
> - 2026-05-19 (저녁): **Naver chg_pct override 제거** — Naver API direction code 신뢰성 부족 (5/18, 5/19 두 번 부호 사고). Naver 는 price 만 보충, chg 는 yfinance series 자동 계산. Quirks #10 업데이트.
> - 2026-05-24: rebalancing_master **Y열 → LIVE 시트 수식**(`SUM(H)−W`) 전환. **AB**(퇴직연금 타겟 위험%)·**AC**(risk_weight)·**AD**(🎯 타겟 조정 여력) 3열 신설 — H 만 수정해도 main.py 없이 즉시 판정. AB 수식은 SUMPRODUCT 쉼표형(텍스트 내성). 헤더 always-write + 이름 직관화 (Y="계좌 공간", AB="퇴직연금 타겟 위험% (≤70%)"). 설계결정 #9, Quirk #15(열 드래그 금지) 추가.
> - 2026-05-24 (저녁): Dashboard "🥧 비중" 섹션에 **국가별 Net 차트** 추가 — Long 에서 헷지를 대상국 음수 차감. 헷지→대상국 종목명 추론(`_hedge_target`), VIX류 ×3·그 외 인덱스 인버스 ×1. 세로 막대그래프(`make_net_bar`, Long 총액 대비 %)라 도넛 재정규화 착시 없고 순매도 국가도 0선 아래 빨간 막대로 표시. 차트 순서 NAV/Long/Net/테마/군종. 설계결정 #10 추가.
> - 2026-05-26: **raw_체결 시스템** — 국내 보통매매 매수/매도를 체결내역(raw_체결 시트) 기반으로 전환. 소스 분리(매매=raw_체결, 그 외=raw_domestic, dedup 불필요), `transform_chey` 신설, `transform_domestic` 에 `exclude_market_trades` 옵션, 분할체결 통합, 수수료 추정, 거래내역↔체결 감사. main.py(현금계산 포함)·Dashboard 작전일지(직전 영업일 import) 반영. `migrate_chey.py`/`verify_chey.py` 추가. 설계결정 #11. **(2026-05-28에 폐기 — #12 참조)**
> - 2026-05-28: **D 옵션 — 단일 raw_domestic 통합** (설계결정 #12). raw_체결, raw_체결_키움 시트 폐기. 사용자가 raw_domestic 에 Z(체결 Y/N), AA(체결일=WORKDAY 수식) 컬럼 추가. 체결+거래내역 모두 raw_domestic 에 paste, `_apply_chey_dedup` 가 자동 dedup(거래내역 우선, 분할체결은 합산 유지). main.py STEP 1·3·6 단순화. Dashboard 작전일지(직전 영업일 import 포함) raw_domestic 하나만 읽음. `transform_chey`/`flatten_kiwoom_chey`/`absorb_kiwoom_chey` 등 dead code화. 워크플로 B 한 시트로 통합.
