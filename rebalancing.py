# --- 5. 리밸런싱 계산 전담 (rebalancing.py) ---

import pandas as pd
import numpy as np
import yfinance as yf

print("\n[Rebalancing] 리밸런싱 계산 모듈(v82: 다중 통합 그룹 + 빈 계좌 포함 + 가격조회 완전판)을 불러왔습니다.")

def calculate_rebalancing_data(df_dashboard, df_target, df_master, current_rates, portfolio_groups=None):
    """
    [v82] 최종 통합 버전
    1. 다중 통합 계좌 그룹 지원 (Personal / Mentor)
    2. 빈 계좌(자산 0원)도 리밸런싱 대상에 포함 (Target이 있으면 계산)
    3. 신규 매수 종목 가격 조회 로직(yfinance/manual) 완전 포함
    """
    if portfolio_groups is None:
        portfolio_groups = {}

    print(f"\n[Core 5] --- 리밸런싱 계산 시작 (설정된 통합 그룹: {list(portfolio_groups.keys())}) ---")
    
    # 1. 데이터 검증
    if df_target.empty:
        return pd.DataFrame()
    
    # [v49] 현금 행 제외 및 데이터 전처리
    if 'name' in df_target.columns:
        df_target = df_target[~df_target['name'].str.contains('현금', na=False)].copy()
    df_target = df_target[df_target['ticker'].notna() & (df_target['ticker'] != 'nan')].copy()
    
    # 목표 비중 숫자 변환
    df_target['target_ratio'] = df_target['target_ratio'].astype(str).str.replace('%', '', regex=False)
    df_target['target_ratio'] = pd.to_numeric(df_target['target_ratio'], errors='coerce').fillna(0) / 100.0
    
    # -------------------------------------------------------
    # [KEY] 그룹별 총 자산(Group AUM) 미리 계산
    # -------------------------------------------------------
    df_dashboard = df_dashboard.copy()
    df_dashboard['account'] = df_dashboard['account'].astype(str).str.strip()
    
    group_aums = {} 
    account_to_group_map = {} 

    for group_name, account_list in portfolio_groups.items():
        # 해당 그룹에 속한 계좌들의 평가액 합계 계산
        mask_group = df_dashboard['account'].isin(account_list)
        total_aum = df_dashboard.loc[mask_group, 'market_value_krw'].sum()
        group_aums[group_name] = total_aum
        
        for acc in account_list:
            account_to_group_map[acc] = group_name
            
        print(f"  [Info] 그룹 '{group_name}' ({len(account_list)}개 계좌) 총 자산: {total_aum:,.0f} 원")

    all_rebal_results = []
    all_cash_rows_to_keep = [] 

    # 계좌별 루프
    df_target = df_target.dropna(subset=['account'])
    df_target['account'] = df_target['account'].astype(str).str.strip()

    for account_id, df_target_account in df_target.groupby('account'):
        
        # 2. 현재 계좌 데이터 준비
        df_dashboard_account = df_dashboard[df_dashboard['account'] == account_id].copy()
        
        # [v81 수정] 자산이 없어도(Empty) 0원으로 계산하고 진행 (skip하지 않음)
        if df_dashboard_account.empty:
            account_total_value = 0
        else:
            account_total_value = df_dashboard_account['market_value_krw'].sum()

        # ---------------------------------------------------
        # [KEY] 기준 AUM 결정 (그룹 vs 독립)
        # ---------------------------------------------------
        group_name = account_to_group_map.get(account_id)
        if group_name:
            base_aum = group_aums[group_name]
            log_prefix = f"[{group_name}]"
        else:
            base_aum = account_total_value
            log_prefix = "[독립]"
            
        print(f"  >> 계좌 {account_id} {log_prefix} 처리 중... (기준 AUM: {base_aum:,.0f}원)")

        # 3. 현금 처리 (보유 현금이 있을 때만)
        if not df_dashboard_account.empty and 'asset_class' in df_dashboard_account.columns:
            cash_rows = df_dashboard_account[df_dashboard_account['asset_class'] == '현금'].copy()
            if not cash_rows.empty:
                # 분모가 0일 경우 방어 로직
                divisor = account_total_value if account_total_value > 0 else 1
                cash_rows['current_ratio'] = cash_rows['market_value_krw'] / divisor
                cash_rows[['target_ratio', 'target_value_krw', 'rebalancing_value_krw', 'rebalancing_quantity']] = 0
                all_cash_rows_to_keep.append(cash_rows)

        # 4. 주식 자산 처리 (보유 종목이 없으면 빈 DF 생성)
        if not df_dashboard_account.empty:
            df_current_account = df_dashboard_account[df_dashboard_account['asset_class'] != '현금'].copy()
            if 'current_price_krw' not in df_current_account.columns:
                 df_current_account['current_price_krw'] = df_current_account['market_value_krw'] / df_current_account['quantity'].replace(0, np.nan)
            
            df_current_account['current_price_krw'] = df_current_account['current_price_krw'].fillna(0)
            df_current = df_current_account[['ticker', 'name', 'market_value_krw', 'current_price_krw', 'quantity']]
        else:
            # [v81 Fix] 자산이 없는 계좌를 위한 빈 데이터프레임 생성
            df_current = pd.DataFrame(columns=['ticker', 'name', 'market_value_krw', 'current_price_krw', 'quantity'])

        # 5. 목표와 현재 병합 (Outer Join)
        df_rebal = pd.merge(df_current, df_target_account[['ticker', 'target_ratio']], on='ticker', how='outer')

        # 5. 이름 매핑 (Master Data)
        if not df_master.empty:
            df_master_names = df_master[['ticker', 'name']].drop_duplicates().set_index('ticker')
            df_rebal = df_rebal.set_index('ticker')
            df_rebal['name'] = df_rebal['name'].fillna(df_master_names['name'])
            df_rebal = df_rebal.reset_index()

        # 6. 결측치 채우기
        df_rebal['market_value_krw'] = df_rebal['market_value_krw'].fillna(0)
        df_rebal['current_price_krw'] = df_rebal['current_price_krw'].fillna(0)
        df_rebal['quantity'] = df_rebal['quantity'].fillna(0)
        df_rebal['target_ratio'] = df_rebal['target_ratio'].fillna(0)
        df_rebal['name'] = df_rebal['name'].fillna(df_rebal['ticker'])

        # 7. [핵심] 리밸런싱 수치 계산 (기준: base_aum)
        # base_aum이 0일 경우(그룹 전체가 0원) 0으로 나누기 방지
        calc_base = base_aum if base_aum > 0 else 1
        
        df_rebal['current_ratio'] = df_rebal['market_value_krw'] / calc_base
        df_rebal['target_value_krw'] = base_aum * df_rebal['target_ratio']
        df_rebal['rebalancing_value_krw'] = df_rebal['target_value_krw'] - df_rebal['market_value_krw']
        df_rebal['rebalancing_quantity'] = 0.0

        # ------------------------------------------------------------------
        # 8. [복원 완료] 신규 매수 종목 가격 조회 (yfinance + price_lookup)
        # ------------------------------------------------------------------
        mask_new_buy = (df_rebal['current_price_krw'] == 0) & (df_rebal['target_ratio'] > 0)
        if mask_new_buy.any():
            print(f"    [Core 5] {mask_new_buy.sum()}개의 신규 매수 종목 '현재가' 조회 시도...")
            
            new_buy_tickers = df_rebal.loc[mask_new_buy, ['ticker']]
            if not df_master.empty:
                master_cols = ['ticker', 'name', 'exchange', 'price_lookup', 'currency']
                existing_master_cols = [col for col in master_cols if col in df_master.columns]
                new_buy_info = pd.merge(new_buy_tickers, df_master[existing_master_cols], on='ticker', how='left')
            else:
                new_buy_info = new_buy_tickers
                new_buy_info['exchange'] = ''
                new_buy_info['price_lookup'] = ''
                new_buy_info['currency'] = 'KRW'
            
            new_buy_info = new_buy_info.set_index('ticker')
            new_prices = {}

            for ticker, row in new_buy_info.iterrows():
                if pd.isna(ticker) or ticker == 'nan': continue
                try:
                    price_krw = 0.0; yf_ticker = ""
                    exchange = str(row.get('exchange', '')).upper()
                    currency = str(row.get('currency', 'KRW')).upper()
                    rate = current_rates.get(currency, 1.0)
                    
                    # 1순위: price_lookup
                    manual_price_str = str(row.get('price_lookup', '')).strip()
                    manual_price = pd.to_numeric(manual_price_str.replace(',', ''), errors='coerce')

                    if pd.notna(manual_price) and manual_price > 0:
                        price_krw = manual_price
                        print(f"      -> [P] 수동 가격: '{ticker}' -> {price_krw:,.0f} KRW")
                    
                    # 2순위: yfinance
                    else:
                        if exchange in ['KOSPI', 'ETF', 'ETN']: yf_ticker = f"{ticker}.KS"
                        elif exchange == 'KOSDAQ': yf_ticker = f"{ticker}.KQ"
                        elif exchange == 'HKG': yf_ticker = f"{ticker}.HK"
                        elif exchange == 'SSE': yf_ticker = f"{ticker}.SS"
                        elif exchange == 'SZSE': yf_ticker = f"{ticker}.SZ"
                        elif exchange == 'TSE': yf_ticker = f"{ticker}.T"
                        elif exchange in ['NASDAQ', 'NYSE', 'AMEX']: yf_ticker = ticker
                        else:
                            if str(ticker).isnumeric() and len(str(ticker)) == 6: yf_ticker = f"{ticker}.KS"
                            else: yf_ticker = ticker
                        
                        if yf_ticker:
                            data = yf.Ticker(yf_ticker).history(period='1d')
                            if not data.empty and 'Close' in data:
                                last_price_native = float(data['Close'].iloc[-1])
                                price_krw = last_price_native * rate
                                print(f"      -> [YF] 자동 가격: '{ticker}' ({yf_ticker}) -> {price_krw:,.0f} KRW")
                            else:
                                print(f"      -> [!] 데이터 없음: '{ticker}'")
                        else:
                            print(f"      -> [!] 티커 생성 불가: '{ticker}'")
                    
                    new_prices[ticker] = price_krw
                except Exception as e:
                    print(f"      -> [!] 오류 발생: '{ticker}' - {e}")
                    new_prices[ticker] = 0.0
            
            # 조회된 가격 업데이트
            df_rebal = df_rebal.set_index('ticker')
            df_rebal['current_price_krw'] = df_rebal['current_price_krw'].replace(0, np.nan)
            df_rebal['current_price_krw'].fillna(pd.Series(new_prices), inplace=True)
            df_rebal = df_rebal.reset_index()
            df_rebal['current_price_krw'] = df_rebal['current_price_krw'].fillna(0)

        # ------------------------------------------------------------------

        # 9. 수량 계산 (2차)
        mask_price_exists = df_rebal['current_price_krw'] > 0
        df_rebal.loc[mask_price_exists, 'rebalancing_quantity'] = \
            df_rebal.loc[mask_price_exists, 'rebalancing_value_krw'] / df_rebal.loc[mask_price_exists, 'current_price_krw']

        # 10. 컬럼 정리 및 필터링
        final_cols = ['ticker', 'name', 'market_value_krw', 'current_ratio', 'target_value_krw', 'target_ratio', 'rebalancing_value_krw', 'rebalancing_quantity', 'current_price_krw', 'quantity']
        existing_final_cols = [col for col in final_cols if col in df_rebal.columns]
        df_rebal_final = df_rebal[existing_final_cols].copy()
        
        # 의미 없는 행 제거 (보유도 안했고 목표도 0인 것)
        df_rebal_final = df_rebal_final[
            (df_rebal_final['market_value_krw'].abs() > 0.01) |
            (df_rebal_final['target_ratio'].abs() > 0.0001) 
        ]
        
        # 11. 반올림
        cols_round = ['market_value_krw', 'target_value_krw', 'rebalancing_value_krw', 'rebalancing_quantity', 'current_price_krw', 'quantity']
        for c in cols_round:
            if c in df_rebal_final.columns: df_rebal_final[c] = df_rebal_final[c].round(0)

        df_rebal_final['account'] = account_id
        all_rebal_results.append(df_rebal_final)

    print("\n[Core 5] --- 모든 계좌 리밸런싱 계산 완료 ---")
    
    # 결과 병합
    if not all_rebal_results and not all_cash_rows_to_keep:
        print("  [Core 5] 계산된 리밸런싱 결과가 없습니다.")
        return pd.DataFrame()
        
    df_all_rebal = pd.DataFrame()
    if all_rebal_results:
        df_all_rebal = pd.concat(all_rebal_results, ignore_index=True)
    
    if all_cash_rows_to_keep:
        df_all_cash = pd.concat(all_cash_rows_to_keep, ignore_index=True)
        df_all_rebal = pd.concat([df_all_rebal, df_all_cash], ignore_index=True)
    
    # 컬럼 순서 정렬
    cols_order = ['account', 'ticker', 'name', 'market_value_krw', 'current_ratio', 
                  'target_value_krw', 'target_ratio', 'rebalancing_value_krw', 
                  'rebalancing_quantity', 'current_price_krw', 'quantity']
    existing_cols_order = [col for col in cols_order if col in df_all_rebal.columns]
    
    return df_all_rebal[existing_cols_order].sort_values(by=['account', 'rebalancing_value_krw'], ascending=[True, False])