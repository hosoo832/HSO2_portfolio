# main.py (v125 - Actual_Ratio 및 Drift 자동 업데이트 패치 적용)

from datetime import timedelta
import pandas as pd
import numpy as np
import gspread
from datetime import datetime
import time
import warnings
import json

# --- [STEP 0] 1~5번 모듈 불러오기 ---
import config
import google_api
import data_transformer
import finance_core
import rebalancing
import account_manager

warnings.filterwarnings("ignore")

print("\n--- [MAIN] 모든 모듈 로드 완료 ---")
print("--- [MAIN] v125 (Actual Ratio & Drift 자동 업데이트 패치) 실행 시작 ---")

def main_run():
    try:
        # =================================================================
        # [스마트 날짜 설정] 평일엔 '오늘', 주말엔 '금요일' 자동 선택
        # =================================================================
        
        # 1. 비상용 수동 날짜 (평소엔 비워두세요: '')
        MANUAL_DATE = ''
        
        # 2. 날짜 자동 계산 로직
        now = datetime.now()
        
        if MANUAL_DATE:
            today_str = MANUAL_DATE
            print(f"\n🚨 [Manual] 사용자가 지정한 날짜 '{today_str}'로 강제 고정합니다.")
        else:
            if now.weekday() >= 5:
                days_to_subtract = now.weekday() - 4
                prev_friday = now - timedelta(days=days_to_subtract)
                today_str = prev_friday.strftime('%Y-%m-%d')
                print(f"\n📅 [Auto] 주말({now.strftime('%a')})이라 '지난 금요일({today_str})' 기준으로 처리합니다.")
            else:
                today_str = now.strftime('%Y-%m-%d')
                print(f"\n📅 [Auto] 오늘은 평일이므로 '오늘 날짜({today_str})'로 처리합니다.")

        # =================================================================
        # --- [STEP 1] 구글 시트에서 원본 데이터 읽기 ---
        print("\n--- [MAIN] STEP 1: 원본 데이터 4개 읽기 시작 ---")
        df_domestic, _ = google_api.get_all_records_as_text(config.SHEET_RAW_DOMESTIC)
        df_intl, _ = google_api.get_all_records_as_text(config.SHEET_RAW_INTL)
        df_master, master_sheet_instance = google_api.get_all_records_as_text(config.SHEET_MASTER_DATA)
        
        # [패치 완료] 업데이트를 위해 타겟 시트(인스턴스)도 함께 가져옵니다.
        df_target, target_sheet_instance = google_api.get_all_records_as_text(config.SHEET_REBALANCING_MASTER)
        
        if not df_master.empty and 'ticker' in df_master.columns:
            df_master['ticker'] = df_master['ticker'].astype(str).str.strip().replace('', 'N/A')
        else:
            print("  [MAIN] 'master_data'가 비어있거나 'ticker' 열이 없습니다.")

        # --- [STEP 2] KOSPI/KOSDAQ 거래소 정보 자동 채우기 ---
        if not df_master.empty:
            df_master, updates_list = finance_core.auto_fill_exchange_info(df_master)
            
            if updates_list and master_sheet_instance:
                try:
                    print("  [MAIN] 구글 'master_data' 시트 원본에 'exchange' 변경 사항을 저장하는 중...")
                    cells_to_update = []
                    for (row, col, value) in updates_list:
                        cells_to_update.append(gspread.Cell(row, col, value))
                    
                    if cells_to_update:
                        master_sheet_instance.update_cells(cells_to_update)
                        print("  [MAIN] 구글 시트 원본 저장 완료.")
                except Exception as e:
                    print(f"  [!!!] 'exchange' 정보 구글 시트 업데이트 실패! {e}")

        # --- [STEP 3] 원본 데이터를 '총계정원장'으로 '번역' ---
        print("\n--- [MAIN] STEP 3: 원본 데이터를 '총계정원장'으로 변환 중 ---")
        domestic_transactions = data_transformer.transform_domestic(df_domestic)
        intl_transactions = data_transformer.transform_international(df_intl)
        
        if intl_transactions.empty:
             intl_transactions = pd.DataFrame(columns=['account', 'date', 'action_type', 'action_detail', 'ticker', 'name', 'quantity', 'settlement_krw', 'currency', 'settlement_foreign'])

        df_transactions = pd.concat([domestic_transactions, intl_transactions], ignore_index=True)
        df_transactions['date'] = pd.to_datetime(df_transactions['date'], errors='coerce')
        df_transactions = df_transactions.dropna(subset=['date'])
        df_transactions['ticker'] = df_transactions['ticker'].astype(str).str.strip().replace('', 'N/A')
        
        # [패치 1] 구형 티커를 최신 티커로 원천 변환 (FB -> META 등)
        ticker_map = {'FB': 'META', 'Google': 'GOOGL', 'BRK.B': 'BRK-B', 'BRK/B': 'BRK-B'}
        df_transactions['ticker'] = df_transactions['ticker'].replace(ticker_map)
        
        df_transactions['account'] = df_transactions['account'].astype(str).str.strip()
        df_transactions['settlement_krw'] = pd.to_numeric(df_transactions['settlement_krw'], errors='coerce').fillna(0)
        df_transactions['quantity'] = pd.to_numeric(df_transactions['quantity'], errors='coerce').fillna(0)

        # --- [STEP 4] 보유 현황 및 '실현 손익' 계산 ---
        print("\n--- [MAIN] STEP 4: 보유 현황 및 실현 손익 계산 중 ---")
        df_holdings = finance_core.calculate_holdings_and_realized_pl(df_transactions)

        # --- [STEP 5] 현재가 조회 및 '평가 손익' 계산 ---
        print("\n--- [MAIN] STEP 5: 현재가 조회 및 평가 손익 계산 중 ---")
        df_holdings_with_master = df_holdings.copy()
        if not df_holdings.empty and not df_master.empty:
            master_cols_to_merge = ['ticker', 'name', 'asset_class', 'country', 'theme', 'postion', 'maket_phase', 'exchange', 'currency', 'price_lookup']
            if 'manual_avg_cost' in df_master.columns:
                master_cols_to_merge.append('manual_avg_cost')
            
            existing_master_cols = [col for col in master_cols_to_merge if col in df_master.columns]
            df_master_unique = df_master.drop_duplicates(subset=['ticker'])
            df_holdings_with_master = pd.merge(df_holdings, df_master_unique[existing_master_cols], on='ticker', how='left')
        
        df_holdings_with_prices, current_rates = finance_core.get_current_prices(df_holdings_with_master, intl_transactions)

        if 'name' in df_holdings_with_prices.columns:
             df_holdings_with_prices['name'] = df_holdings_with_prices['name'].fillna(df_holdings_with_prices['ticker'])

        if not df_holdings_with_prices.empty and 'quantity' in df_holdings_with_prices.columns:
            df_holdings_with_prices['market_value_krw'] = df_holdings_with_prices['quantity'] * df_holdings_with_prices['current_price_krw']
            df_holdings_with_prices['unrealized_pl_krw'] = df_holdings_with_prices['market_value_krw'] - df_holdings_with_prices['total_cost_krw']
            df_holdings_with_prices['realized_pl_krw'] = df_holdings_with_prices['realized_pl_krw'].fillna(0)
            df_holdings_with_prices['cumulative_pl_krw'] = df_holdings_with_prices['unrealized_pl_krw'] + df_holdings_with_prices['realized_pl_krw']
            df_holdings_with_prices['return_rate'] = np.where(df_holdings_with_prices['total_cost_krw'] != 0, (df_holdings_with_prices['unrealized_pl_krw'] / df_holdings_with_prices['total_cost_krw']), 0).round(4) * 100
        else:
             cols = ['account', 'ticker', 'quantity', 'avg_cost_krw', 'total_cost_krw', 'realized_pl_krw', 'market_value_krw', 'unrealized_pl_krw', 'cumulative_pl_krw', 'return_rate']
             df_holdings_with_prices = pd.DataFrame(columns=cols)

        # --- [STEP 6] 계좌별 현금 잔고 계산 ---
        print("\n--- [MAIN] STEP 6: 계좌별 현금 잔고 계산 중 ---")
        df_cash_rows, total_cash_for_dashboard = finance_core.calculate_cash_balances(
            domestic_transactions, intl_transactions, current_rates
        )

        # --- [STEP 7] 1차 대시보드 생성 ---
        print("\n--- [MAIN] STEP 7: 1차 대시보드 데이터 생성 ---")
        df_dashboard_base = pd.concat([df_holdings_with_prices, df_cash_rows], ignore_index=True)

        # --- [STEP 7.5] 계좌별 순투입원금(NIC) 계산 ---
        print("\n--- [MAIN] STEP 7.5: 계좌별 순투입원금(NIC) 계산 중 ---")
        if not df_transactions.empty:
            net_inv_df = account_manager.calculate_net_investment(df_transactions)
            if not net_inv_df.empty:
                latest_net_inv = net_inv_df.sort_values('date').groupby('account').tail(1)
                df_dashboard_base = pd.merge(
                    df_dashboard_base,
                    latest_net_inv[['account', 'net_invested_capital']],
                    on='account',
                    how='left'
                )
                df_dashboard_base['net_invested_capital'] = df_dashboard_base['net_invested_capital'].fillna(0)
            else:
                df_dashboard_base['net_invested_capital'] = 0
        else:
            df_dashboard_base['net_invested_capital'] = 0

        # --- [STEP 7.8] 매입단가 수동 보정 및 예외 처리 ---
        print("\n--- [MAIN] STEP 7.8: 매입단가 수동 보정 및 데이터 클렌징 ---")
        final_rows = []
        for _, row in df_dashboard_base.iterrows():
            try:
                qty = float(row.get('quantity', 0))
                ticker = str(row.get('ticker', ''))
                
                # [패치 2] 수기로 추가한 MMF_INT(결산 이자) 메타데이터 채우기
                if ticker == 'MMF_INT':
                    row['name'] = '누락수익 보정(배당/이자)'
                    row['asset_class'] = '현금'
                    row['theme'] = '안전 자산'
                    row['exchange'] = 'CASH'
                    row['country'] = '한국'
                
                is_cash = ticker.startswith('CASH') or ticker.startswith('KRW-CASH')
                
                # [패치 3] 전량 매도(수량 0개) 종목 완벽 필터링
                if not is_cash and abs(qty) < 0.0001 and ticker != 'MMF_INT':
                    continue
                
                calc_avg = float(row.get('avg_cost_krw', 0))
                manual_avg = float(row.get('manual_avg_cost', 0)) if 'manual_avg_cost' in row else 0
                final_avg_cost = manual_avg if manual_avg > 0 else calc_avg
                
                total_cost = final_avg_cost * qty
                curr_price = float(row.get('current_price_krw', 0))
                market_val = float(row.get('market_value_krw', qty * curr_price))
                
                if is_cash:
                    unrealized = 0
                    return_rate = 0
                else:
                    unrealized = market_val - total_cost
                    return_rate = (unrealized / total_cost * 100) if total_cost != 0 else 0
                
                row['avg_cost_krw'] = final_avg_cost
                row['total_cost_krw'] = total_cost
                row['unrealized_pl_krw'] = unrealized
                row['return_rate'] = return_rate
                row['market_value_krw'] = market_val
                
                final_rows.append(row)
            except:
                final_rows.append(row)
        
        df_dashboard_final = pd.DataFrame(final_rows)

        # --- [STEP 8] 리밸런싱 계산 ---
        print("\n--- [MAIN] STEP 8: 리밸런싱 데이터 산출 중 ---")
        df_master_unique_rebal = df_master.drop_duplicates(subset=['ticker'])
        portfolio_groups_config = {
            'Personal_Portfolio': ['53649012', '220914426167', '856045053982', '717190227129'],
            'Mentor_Portfolio': ['53648897', '60271589']
        }
        df_rebalancing = rebalancing.calculate_rebalancing_data(
            df_dashboard_final, df_target, df_master_unique_rebal,
            current_rates, portfolio_groups=portfolio_groups_config
        )

        # =================================================================
        # 🚀 [STEP 8.5] 시트 I열(Actual_Ratio), J열(Drift) 직접 업데이트
        # =================================================================
        print("\n--- [MAIN] STEP 8.5: 실제 비중(Actual Ratio) 및 괴리율(Drift) 구글 시트 업데이트 ---")
        try:
            if not df_target.empty and target_sheet_instance:
                # 1. 포트폴리오 그룹 하드코딩 (안정성 보장)
                mentor_accs = ['60271589', '53648897']
                hs_accs = ['53649012', '856045053982', '220914426167', '717190227129']

                # 2. 그룹별 데이터 분리
                df_mentor = df_dashboard_final[df_dashboard_final['account'].astype(str).isin(mentor_accs)]
                df_hs = df_dashboard_final[df_dashboard_final['account'].astype(str).isin(hs_accs)]

                # 3. 그룹별 '총자산(현금 포함)' 계산 (분모)
                mentor_total_mv = df_mentor['market_value_krw'].sum()
                hs_total_mv = df_hs['market_value_krw'].sum()

                # 4. [수정 완료] 계좌별 + 종목별 2중 조건으로 '현재 평가금액' 사전 구축 (분자용)
                # groupby에 'account'와 'ticker'를 같이 넣어서 (계좌번호, 티커) 형태의 고유 키를 만듭니다.
                mentor_acc_ticker_mv = df_mentor.groupby(['account', 'ticker'])['market_value_krw'].sum().to_dict()
                hs_acc_ticker_mv = df_hs.groupby(['account', 'ticker'])['market_value_krw'].sum().to_dict()

                # 4.5 [v126 신규] 계좌별 총자산 / 가용현금 사전 구축
                # - 계좌 AUM = 그 계좌의 모든 종목 + 현금 합 (현금격벽 인지를 위해)
                # - 계좌 cash = ticker 가 'CASH' 로 시작하는 행의 합 (KRW + 외화 환산 모두)
                mentor_acc_total = df_mentor.groupby('account')['market_value_krw'].sum().to_dict()
                hs_acc_total = df_hs.groupby('account')['market_value_krw'].sum().to_dict()

                _is_cash_m = df_mentor['ticker'].astype(str).str.startswith('CASH')
                _is_cash_h = df_hs['ticker'].astype(str).str.startswith('CASH')
                mentor_acc_cash = df_mentor[_is_cash_m].groupby('account')['market_value_krw'].sum().to_dict()
                hs_acc_cash = df_hs[_is_cash_h].groupby('account')['market_value_krw'].sum().to_dict()

                cells_to_update = []

                # 5. 각 행(종목)별 비중 순회 및 계산
                for idx, row in df_target.iterrows():
                    sheet_row = idx + 2  # 구글 시트는 1행이 헤더이므로 인덱스에 2를 더함
                    acc = str(row.get('account', '')).strip()
                    ticker = str(row.get('ticker', '')).strip()

                    if not ticker: continue # 빈칸(티커 없음)은 스킵

                    # H열: Target Ratio 파싱 (% 기호 제거 및 소수점 변환)
                    target_val_str = str(row.get('target_ratio', '0')).replace('%', '').strip()
                    try:
                        target_ratio = float(target_val_str) / 100.0
                    except:
                        target_ratio = 0.0

                    # 🚨 [수정 완료] 분자(t_mv)를 찾을 때 (계좌번호, 티커) 쌍으로 정확하게 찾습니다.
                    if acc in mentor_accs:
                        total_mv = mentor_total_mv
                        t_mv = mentor_acc_ticker_mv.get((acc, ticker), 0.0)
                        acc_aum = mentor_acc_total.get(acc, 0.0)
                        acc_cash = mentor_acc_cash.get(acc, 0.0)
                    elif acc in hs_accs:
                        total_mv = hs_total_mv
                        t_mv = hs_acc_ticker_mv.get((acc, ticker), 0.0)
                        acc_aum = hs_acc_total.get(acc, 0.0)
                        acc_cash = hs_acc_cash.get(acc, 0.0)
                    else:
                        continue # 지정되지 않은 계좌가 섞여 있으면 스킵

                    # I열/J열: 실제 비중 및 괴리율 계산
                    # (UX 패치) 괴리율 = 목표(Target) - 현재(Actual)
                    actual_ratio = float((t_mv / total_mv) if total_mv > 0 else 0.0)
                    drift = float(target_ratio - actual_ratio)

                    # [v126 신규] W/X/Y 열: 계좌 capacity 정보
                    # W (23): 계좌 AUM 비중 = 이 계좌가 그룹 AUM 의 몇 % (= 단일종목 절대 max)
                    # X (24): 계좌 가용현금 비중 = 추가 매수 capacity (% of group AUM)
                    # Y (25): 이 종목에 줄 수 있는 max target_ratio
                    #         = (현재 종목 평가액 + 계좌 가용현금) / 그룹 AUM
                    #         → target_ratio > Y 이면 매수 불가능 (예수금 부족)
                    acc_aum_ratio  = float((acc_aum / total_mv) if total_mv > 0 else 0.0)
                    acc_cash_ratio = float((acc_cash / total_mv) if total_mv > 0 else 0.0)
                    max_target_ratio = float(((t_mv + acc_cash) / total_mv) if total_mv > 0 else 0.0)

                    # I열(9), J열(10)에 순수 숫자(Float) 값 적재
                    cells_to_update.append(gspread.Cell(sheet_row, 9, actual_ratio))
                    cells_to_update.append(gspread.Cell(sheet_row, 10, drift))
                    # W(23), X(24), Y(25) 열 — 모두 decimal 형태 (시트에서 % 서식 적용 시 자동 환산)
                    cells_to_update.append(gspread.Cell(sheet_row, 23, acc_aum_ratio))
                    cells_to_update.append(gspread.Cell(sheet_row, 24, acc_cash_ratio))
                    cells_to_update.append(gspread.Cell(sheet_row, 25, max_target_ratio))

                # 5.5 [v126] W/X/Y 헤더 자동 작성 (한 번만 — 비어 있을 때)
                try:
                    header_cells = target_sheet_instance.batch_get(['W1:Y1'])
                    existing_headers = header_cells[0][0] if header_cells and header_cells[0] else []
                except Exception:
                    existing_headers = []
                if not existing_headers or any((c or '').strip() == '' for c in existing_headers + [''] * (3 - len(existing_headers))):
                    cells_to_update.extend([
                        gspread.Cell(1, 23, '계좌 AUM (%)'),
                        gspread.Cell(1, 24, '계좌 가용현금 (%)'),
                        gspread.Cell(1, 25, '매수가능 max (%)'),
                    ])

                # 6. 구글 시트로 한 번에 쏘기 (성능 최적화)
                if cells_to_update:
                    target_sheet_instance.update_cells(cells_to_update)
                    print("  [MAIN] 구글 시트 I/J/W/X/Y 열 일괄 업데이트 완료! ✅")
                    print("        (I=Actual_Ratio, J=Drift, W=계좌AUM%, X=계좌가용현금%, Y=매수가능max%)")
            else:
                print("  [!] 타겟 시트를 찾을 수 없거나 데이터가 비어있습니다.")
        except Exception as e:
            print(f"  [!!!] 실제 비중(Actual Ratio) 업데이트 중 오류 발생: {e}")

        # --- [STEP 9] 결과 요약 ---
        total_assets_krw = df_dashboard_final['market_value_krw'].sum()
        total_invested_market_value = df_dashboard_final[df_dashboard_final['asset_class'] != '현금']['market_value_krw'].sum()
        
        # --- [STEP 9.5] 시장 지표 ---
        # [v70.33] 비활성화: GitHub Actions cron (KST 매일 07:00) 이 update_market_data.py 로
        # 매일 1행씩 append 함. main.py 에서 backfill 을 또 돌리면:
        #   1) 시트 통째로 덮어써져서 cron 누적분 손실
        #   2) yfinance/ECOS API quota 낭비
        #   3) 실행 시간 +1~2분
        # 과거 데이터 다시 깔끔하게 채우고 싶을 때만 backfill_market_data.py 를 직접 실행.
        # print("\n--- [MAIN] STEP 9.5: 시장 지표 수집 ---")
        # try:
        #     import backfill_market_data
        #     backfill_market_data.run_backfill()
        # except Exception as e:
        #     print(f"  [!] 시장 지표 수집 실패: {e}")

        # --- [STEP 10] 구글 시트 업로드 ---
        print("\n--- [MAIN] STEP 10: 대시보드 업로드 ---")
        if not df_rebalancing.empty:
            google_api.upload_to_google_sheet(df_rebalancing, config.REBALANCING_SHEET_NAME)
        
        if not df_dashboard_final.empty:
            upload_cols = [
                'account', 'ticker', 'name', 'asset_class', 'country', 'theme', 'postion', 'maket_phase',
                'exchange', 'currency', 'price_lookup', 'quantity', 'current_price_krw', 'avg_cost_krw',
                'total_cost_krw', 'net_invested_capital', 'market_value_krw', 'unrealized_pl_krw',
                'realized_pl_krw', 'cumulative_pl_krw', 'return_rate', 'yf_ticker'
            ]
            
            for col in upload_cols:
                if col not in df_dashboard_final.columns:
                    df_dashboard_final[col] = ""
                    
            # [패치 4] 문자열 컬럼(테마 등)이 0으로 덮어씌워지는 오류 방지
            for col in upload_cols:
                if any(x in col for x in ['krw', 'rate', 'quantity', 'capital']):
                    df_dashboard_final[col] = pd.to_numeric(df_dashboard_final[col], errors='coerce').fillna(0)
                else:
                    df_dashboard_final[col] = df_dashboard_final[col].fillna("")
                    
            google_api.upload_to_google_sheet(df_dashboard_final[upload_cols], config.OUTPUT_SHEET_NAME)

        # --- [STEP 11] 자산 히스토리 기록 ---
        print("\n--- [MAIN] STEP 11: 자산 히스토리 기록 (향후 사용을 위한 구조 유지) ---")
        pass

        # --- [STEP 12] 상세 로그(portfolio_log) 기록 ---
        print("\n--- [MAIN] STEP 12: portfolio_log 기록 ---")
        try:
            df_log = df_dashboard_final.copy()
            df_log.insert(0, 'date', today_str)
            cols_to_save = [
                'date', 'account', 'ticker', 'name',
                'asset_class', 'country', 'theme', 'exchange',
                'quantity', 'current_price_krw', 'avg_cost_krw',
                'total_cost_krw', 'net_invested_capital',
                'market_value_krw', 'unrealized_pl_krw', 'realized_pl_krw',
                'cumulative_pl_krw', 'return_rate'
            ]
            valid_cols = [c for c in cols_to_save if c in df_log.columns]
            df_log_filtered = df_log[valid_cols].copy()
            
            # [패치 4 동일 적용] 숫자와 문자열 안전 분리 처리
            for col in valid_cols:
                if any(x in col for x in ['krw', 'rate', 'quantity', 'capital']):
                    df_log_filtered[col] = pd.to_numeric(df_log_filtered[col], errors='coerce').fillna(0)
                else:
                    df_log_filtered[col] = df_log_filtered[col].fillna("")
            
            if master_sheet_instance:
                log_sheet = master_sheet_instance.spreadsheet.worksheet("portfolio_log")
                existing_data = log_sheet.get_all_records()
                df_existing = pd.DataFrame(existing_data)
                
                if not df_existing.empty and 'date' in df_existing.columns:
                    df_existing['date'] = df_existing['date'].astype(str)
                    df_existing = df_existing[df_existing['date'] != today_str]
                
                df_final_log = pd.concat([df_existing, df_log_filtered], ignore_index=True).fillna("")
                log_sheet.clear()
                log_sheet.update(range_name='A1', values=[df_log_filtered.columns.tolist()] + df_final_log.values.tolist())
                print(f"  [Log] portfolio_log 업데이트 완료.")
        except Exception as e:
            print(f"  [!!!] portfolio_log 기록 실패: {e}")

        # --- [STEP 12.5] 자금 흐름 보고서 (Cash Flow Report) 통합 ---
        print("\n--- [MAIN] STEP 12.5: 자금 흐름(Cash Flow) 보고서 작성 ---")
        try:
            mask_transfer = df_transactions['action_type'].isin(['Transfer'])
            df_cash = df_transactions[mask_transfer].copy()
            
            if not df_cash.empty:
                df_cash = df_cash.sort_values(by=['account', 'date'], ascending=[True, True])
                df_cash['running_balance'] = df_cash.groupby('account')['settlement_krw'].cumsum()

                df_report = df_cash[['date', 'account', 'action_detail', 'settlement_krw', 'running_balance', 'name']].copy()
                df_report['date'] = df_report['date'].dt.strftime('%Y-%m-%d')
                df_report['action_detail'] = df_report['action_detail'].replace({'Deposit': '입금 (+)', 'Withdraw': '출금 (-)'})
                df_report.columns = ['날짜', '계좌번호', '구분', '금액(KRW)', '누적 순투입원금(NIC)', '비고']
                df_report = df_report.sort_values(by=['날짜', '계좌번호'], ascending=[False, True])

                df_cash['Month'] = df_cash['date'].dt.strftime('%Y-%m')
                monthly_pivot = df_cash.groupby(['Month', 'account'])['settlement_krw'].sum().unstack(fill_value=0)
                monthly_pivot['Total Net Flow'] = monthly_pivot.sum(axis=1)
                monthly_pivot = monthly_pivot.sort_index(ascending=False).reset_index()

                if master_sheet_instance:
                    wb = master_sheet_instance.spreadsheet
                    try: cf_sheet = wb.worksheet("cash_flow_log")
                    except: cf_sheet = wb.add_worksheet("cash_flow_log", 2000, 20)
                    
                    cf_sheet.clear()
                    cf_sheet.update(range_name='A1', values=[["[1] 상세 자금 흐름"]])
                    cf_sheet.update(range_name='A2', values=[df_report.columns.tolist()] + df_report.values.tolist())
                    cf_sheet.update(range_name='H1', values=[["[2] 월별 순유입 현황"]])
                    pivot_vals = json.loads(json.dumps([monthly_pivot.columns.tolist()] + monthly_pivot.values.tolist(), default=str))
                    cf_sheet.update(range_name='H2', values=pivot_vals)
                    print(f"  [CashFlow] 'cash_flow_log' 시트 업데이트 완료.")
            else:
                print("  [CashFlow] 입출금 내역이 없습니다.")
                
        except Exception as e:
            print(f"  [!!!] 자금 흐름 보고서 작성 중 오류: {e}")

        # --- [STEP 13] 성과 분석 (Performance) ---
        print("\n--- [MAIN] STEP 13: 성과 분석 (Performance) ---")
        try:
            import performance
            performance.run_performance_analysis()
            print(">>> [Success] Performance 분석 및 리포트 갱신 완료!")
        except Exception as e:
            print(f"  [!!!] Performance 실행 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()

        print("\n--- [MAIN] 모든 작업이 성공적으로 완료되었습니다! ---")

    except Exception as e:
        print(f"\n[!!!] 메인 실행 중 심각한 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main_run()