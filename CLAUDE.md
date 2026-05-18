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
| `data_transformer.py` | raw_domestic/international → 총계정원장 변환 (`classify_domestic_action`, `transform_international`) |
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
| `raw_domestic` | 계좌번호, 거래일자, 거래종류, 적요명, 종목코드, 종목명, 거래수량, 정산금액 | 증권사에서 export 한 거래내역 붙여넣기 |
| `raw_international` | 계좌번호, 거래일자, 적요명, 종목코드, 종목명, 거래수량, 정산금액(외), 통화 | 해외 거래 |
| `master_data` | ticker, name, **theme**, **pension_class**, postion (오타, 작전분류 공격/방어), maket_phase, exchange, currency, country, **military** (군분류 방위군/공군/육군/해군/특수군) | 종목 마스터. 사용자가 수동 관리 |

### main.py 가 생성/갱신하는 시트
| 시트 | 갱신 시점 | 설명 |
|---|---|---|
| `dashboard_data` | STEP 10 | 계좌별 보유 종목 + 현재 평가 (시트 통째로 덮어씀) |
| `rebalancing_data` | STEP 10 | 리밸런싱 계산 결과 (덮어씀) |
| `rebalancing_master` | STEP 8.5 | 사용자가 H열 (target_ratio) 설정. main 이 **I/J/W/X/Y/Z 자동 갱신** |
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
| Y | 계좌별 target 초과/여유 (%p) | main.py 자동 |
| Z | 퇴직연금 위험% (현재) | main.py 자동 (퇴직연금 행만) |
| **AA** | **Long_weight** | main.py 자동 — pension_class+military 기반 0.0~1.0 |

> ⚠️ F열 헤더 명명: 원래 `postion` (오타) → `military` 로 변경 완료 (2026-05-19).
> main.py 라인 449 가 `row.get('military') or row.get('postion')` fallback 으로 둘 다 매칭.

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

**(a) 한국 퇴직연금 규제용** (rebalancing_master Z열, `_risk_ratio_from_pc`):
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
}
```
→ performance_summary 에 `지정(25-05-14~)`, `지정(25-07-21~)`, `지정(25-10-29~)` 컬럼 자동 생성.

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
1. 증권사 → raw_domestic / raw_international 에 거래 붙여넣기
2. Dashboard 사이드바 → `▶️ main.py 실행` (GitHub Actions 트리거)
   - 또는 GitHub 앱 → Actions → "Run main.py (manual)" → Run workflow
3. 1-2분 대기 (✅ 알림 옴)
4. Dashboard → `🔄 데이터 새로고침` 클릭

### C. Target Ratio 조정 (리밸런싱 목표 변경)
1. Google Sheets → `rebalancing_master` 시트
2. H열 (target_ratio) 직접 편집
3. main.py 실행 (위 B 참조)
4. Dashboard → HS 포폴 → 종목별 리밸런싱 표
   - Y열 (target 초과/여유) 가 0 근처여야 정상
   - Z열 (퇴직연금 위험%) 가 65~70% 이내여야 정상

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
- ✅ **Dashboard 비중 도넛 4개 (2026-05)**: 국가별 NAV / 국가별 Long / 테마별 / 군종별
- ✅ **KPI YTD 손익 카드 (2026-05)**: TWR % 옆에 ₩ 손익 동시 표시
- ✅ **분류 체계 정리 (2026-05)**: master_data theme="채권혼합" 분리, L열 military 컬럼, rebalancing_master F열 military 명명

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
> 마지막 갱신: **2026-05-19** (market data 안정화 + Long_weight 시스템 + cron-job.org + 비중 도넛 4개 + 분류 체계 정리)
>
> ### 갱신 이력
> - 2026-05-11: 작전 일지 시스템 + Cowork 프로젝트 분리
> - 2026-05-19: market data 대대적 안정화 (KST + intraday + Naver fallback price/chg/휴장), cron-job.org 정밀 트리거, Long_weight (AA열) + 시트 M/N (% of Long) 시스템, Dashboard 비중 도넛 4개 (NAV/Long/테마/군종), KPI YTD 손익 카드, master_data L열 military + rebalancing_master F열 military 명명, Quirks #9~13 추가
> - 2026-05-19 (저녁): **Naver chg_pct override 제거** — Naver API direction code 신뢰성 부족 (5/18, 5/19 두 번 부호 사고). Naver 는 price 만 보충, chg 는 yfinance series 자동 계산. Quirks #10 업데이트.
