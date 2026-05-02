# --- 4. 재무 계산 엔진: 보유 현황, 손익, 현재가, 현금 계산 ---
# v75: 사용자 원본(v70.25) 유지 + 샤오펑/미국주식 패치 적용

import pandas as pd
import numpy as np
from pykrx import stock # [엔진 1]에서 사용
import yfinance as yf    # [엔진 3, 5]에서 사용
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

print("\n[Finance Core] 재무 계산 엔진(v75 - User Original Patched)을 불러왔습니다.")

def get_current_price_naver(ticker):
    """[초강력 우회 스크래퍼] 네이버 모바일 API를 통해 가격을 100% 정확하게 긁어옵니다."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            price_str = str(res.json().get('closePrice', '0')).replace(',', '')
            if float(price_str) > 0:
                return float(price_str)
    except Exception as e:
        print(f"  [API 경고] 네이버 모바일 API 실패 ({ticker}): {e}")
    return None

# --- [엔진 1] 거래소 자동 탐지 (v35) ---
def auto_fill_exchange_info(df_master):
    """
    pykrx를 사용해 'master_data'의 한국 주식 거래소(KOSPI/KOSDAQ)를 자동 탐지합니다.
    """
    print("\n[Core 1] 'master_data' 거래소 정보 자동 탐지 시작...")
    if 'exchange' not in df_master.columns:
        print("  [Core 1] 'master_data'에 'exchange' 열이 없습니다. 건너뜁니다.")
        return df_master, []

    df_master['exchange'] = df_master['exchange'].fillna('')
    target_stocks = df_master[(df_master['country'] == '한국') & (df_master['exchange'] == '')]
    
    if target_stocks.empty:
        print("  [Core 1] '한국' 종목 중 'exchange'가 비어있는 항목 없음. (통과)");
        return df_master, []

    print(f"  [Core 1] {len(target_stocks)}개 한국 종목의 거래소 정보 조회 중 (pykrx)...")
    try:
        kospi_tickers = set(stock.get_market_ticker_list(market='KOSPI'))
        kosdaq_tickers = set(stock.get_market_ticker_list(market='KOSDAQ'))
        print(f"  [Core 1] KOSPI {len(kospi_tickers)}개, KOSDAQ {len(kosdaq_tickers)}개 목록 불러오기 완료.")
    except Exception as e:
        print(f"  [!!!] 'pykrx'로 종목 목록을 불러오는 데 실패했습니다. {e}");
        return df_master, []

    updates_for_sheet = []
    
    for index, row in target_stocks.iterrows():
        ticker = row['ticker']; name = row['name']; market_name = None
        
        if ticker in kospi_tickers: market_name = "KOSPI"
        elif ticker in kosdaq_tickers: market_name = "KOSDAQ"
        else:
            print(f"     [!] '{ticker}' ({name}) 조회 실패 (ETF/ETN/KONEX?). 수동 입력 필요.");
            continue
            
        df_master.loc[index, 'exchange'] = market_name
        
        try:
            row_index_for_sheet = df_master.index.get_loc(index)
            cell_row = row_index_for_sheet + 2
            cell_col = df_master.columns.get_loc('exchange') + 1
            updates_for_sheet.append((cell_row, cell_col, market_name))
            
            print(f"     [+] '{ticker}' ({name}) -> {market_name} 업데이트");
        except Exception as e_idx:
             print(f"     [!] '{ticker}' ({name})의 인덱스 조회 실패: {e_idx}. 시트 업데이트 건너뜀.")
        
    if updates_for_sheet:
        print(f"  [Core 1] 총 {len(updates_for_sheet)}개 종목 'exchange' 정보 업데이트 완료.")
    else:
        print("  [Core 1] 거래소 정보 업데이트 없음.");
        
    return df_master, updates_for_sheet


# --- [엔진 2] 보유 현황 및 실현 손익 계산 (v76-Dividend Patch) ---
# --- [엔진 2] 보유 현황 및 실현 손익 계산 ---
def calculate_holdings_and_realized_pl(df_transactions):
    """
    [v109] 액면분할(Split) 로직 개선 + 디버깅 로그 추가
    - Split_Out: 수량은 줄이지만, Total Cost(내 돈)는 유지함 (P/L 실현 안 함)
    - Split_In: 수량만 늘림
    """
    # print(f"\n[Core 2] 보유 현황 계산 시작...")
    
    trades_only = df_transactions[
        (df_transactions['ticker'].notna()) &
        (df_transactions['ticker'] != 'N/A')
    ].copy()
    
    if trades_only.empty: return pd.DataFrame()
        
    trades_only['quantity'] = pd.to_numeric(trades_only['quantity'], errors='coerce').fillna(0)
    trades_only['settlement_krw'] = pd.to_numeric(trades_only['settlement_krw'], errors='coerce').fillna(0)

    # =========================================================================
    
    # 정렬 순서: Split_In을 먼저 처리하는 게 안전할 수 있으나, 보통 날짜가 같으면 순서대로
    sort_priority_map = {
        'Buy': 1, 'Stock_In': 1, 'Split_In': 1, 
        'Sell': 2, 'Stock_Out': 2, 'Split_Out': 2, 'Liquidation': 2,
        'Dividend': 3, 'Interest': 3
    }
    trades_only['sort_priority'] = trades_only['action_detail'].map(sort_priority_map).fillna(9)
    trades_only = trades_only.sort_values(by=['date', 'account', 'ticker', 'sort_priority'], kind='stable')

    holdings_state = {}
    
    # [디버깅] GOOGL 추적용
    debug_ticker = 'OFF'
    
    for index, row in trades_only.iterrows():
        key = (row['account'], row['ticker'])
        
        if key not in holdings_state:
            holdings_state[key] = {'qty': 0, 'total_cost': 0, 'avg_cost': 0, 'realized_pl': 0}
            
        state = holdings_state[key]
        qty = row['quantity']
        cost = abs(row['settlement_krw'])
        action = row['action_detail']
        
        # --- 디버깅 출력 (GOOGL만) ---
        if row['ticker'] == debug_ticker:
            print(f" >> [DEBUG {debug_ticker}] {row['date'].date()} | {action} | Qty: {qty} | Pre-Bal: {state['qty']} | ", end="")

        # 1. 매수 (Buy)
        if action == 'Buy':
            new_qty = state['qty'] + qty
            new_total_cost = state['total_cost'] + cost
            new_avg_cost = new_total_cost / new_qty if new_qty > 0 else 0
            state.update({'qty': new_qty, 'total_cost': new_total_cost, 'avg_cost': new_avg_cost})
            
        # 2. 매도 (Sell)
        elif action == 'Sell':
            qty_to_sell = qty
            # 방어 로직: 보유량보다 많이 팔면 보유량만큼만 팜
            if state['qty'] < qty: qty_to_sell = state['qty']
                
            cost_basis_sold = state['avg_cost'] * qty_to_sell
            pl = cost - cost_basis_sold
            state['realized_pl'] += pl
            
            new_qty = state['qty'] - qty_to_sell
            new_total_cost = state['avg_cost'] * new_qty if new_qty > 0 else 0
            state.update({'qty': new_qty, 'total_cost': new_total_cost})

        # 3. [New] 분할 출고 (Split_Out) - 핵심 수정!
        elif action == 'Split_Out':
            # 수량은 줄어들지만, 내 원금(Total Cost)은 그대로 유지해야 함!
            # 그래야 남은 주식(혹은 0주가 되더라도)에 원금이 붙어있다가 Split_In 때 희석됨.
            new_qty = state['qty'] - qty
            # new_total_cost는 state['total_cost'] 그대로 유지 (줄이지 않음!)
            state.update({'qty': new_qty}) 
            # (참고: 수량이 0이 되면 avg_cost는 무한대가 될 수 있으나, 곧바로 Split_In이 들어오므로 괜찮음)

        # 4. [New] 분할 입고 (Split_In)
        elif action == 'Split_In':
            new_qty = state['qty'] + qty
            # 분할로 들어온 주식은 돈 주고 산 게 아니므로 total_cost 증가는 없음 (cost=0 가정)
            # 단, Split_Out에서 유지한 total_cost가 여기에 묻어가면서 평단가가 자연스럽게 낮아짐.
            if new_qty > 0:
                state['avg_cost'] = state['total_cost'] / new_qty
            state.update({'qty': new_qty})

        # 5. 기타 입고 (Stock_In)
        elif action == 'Stock_In':
            new_qty = state['qty'] + qty
            if cost > 0: state['total_cost'] += cost
            if new_qty > 0: state['avg_cost'] = state['total_cost'] / new_qty
            state.update({'qty': new_qty})

        # 6. 기타 출고 (Stock_Out / Liquidation)
        elif action in ['Stock_Out', 'Liquidation']:
            qty_to_remove = qty if qty > 0 else state['qty']
            if state['qty'] < qty_to_remove: qty_to_remove = state['qty']
            
            if qty_to_remove > 0:
                # 일반 출고는 비율대로 원금을 차감함 (손익 실현으로 간주하거나 원금 회수)
                cost_basis = state['avg_cost'] * qty_to_remove
                pl = cost - cost_basis
                state['realized_pl'] += pl
                
                new_qty = state['qty'] - qty_to_remove
                new_total_cost = state['avg_cost'] * new_qty if new_qty > 0 else 0
                state.update({'qty': new_qty, 'total_cost': new_total_cost})

        # 7. 배당 (Dividend)
        elif action in ['Dividend', 'Interest']:
            state['realized_pl'] += cost

        # --- 디버깅 결과 출력 ---
        if row['ticker'] == debug_ticker:
            print(f"Post-Bal: {state['qty']}")

    final_holdings_list = []
    for (account, ticker), state in holdings_state.items():
        if state['qty'] > 0.0001 or abs(state['realized_pl']) > 0.01:
            final_holdings_list.append({
                'account': account, 'ticker': ticker, 'quantity': state['qty'],
                'avg_cost_krw': state['avg_cost'], 'total_cost_krw': state['total_cost'],
                'realized_pl_krw': state['realized_pl']
            })
            
    return pd.DataFrame(final_holdings_list)
    print(f"  [Core 2] 총 {len(df_holdings)}개 항목 (배당 포함) 계산 완료.");
    return df_holdings


# --- [엔진 3] 현재가 조회 (yfinance) (v77 - 결측치 철벽 방어 및 네이버 API 적용) ---
def get_current_prices(df_holdings_with_master, intl_transactions):
    print("\n[Core 3] 보유 종목 현재가 및 환율 조회 시작 (yfinance & Naver)...")
    df = df_holdings_with_master.copy()
    if 'ticker' in df.columns: df['ticker'] = df['ticker'].astype(str)
    
    if 'quantity' in df.columns: df_to_fetch_prices = df[df['quantity'] > 0.0001].copy()
    else: df_to_fetch_prices = pd.DataFrame()
        
    if df_to_fetch_prices.empty: print("  [Core 3] 현재가 조회할 보유 종목 없음.")
        
    price_dict = {}; yf_tickers_map = {}; yf_tickers_to_fetch = []
    print("  [Core 3] 1. 'price_lookup' (수동) 및 조회 대상 생성 중...")
    
    for index, row in df_to_fetch_prices.iterrows():
        ticker = str(row['ticker']).strip()
        name = row.get('name', 'N/A')
        
        # [핵심 수술 부위 1] 결측치(NaN) 완벽 방어
        exchange_raw = row.get('exchange', '')
        if pd.isna(exchange_raw): exchange_raw = ''
        exchange = str(exchange_raw).strip().upper()
        if exchange == 'NAN': exchange = ''

        manual_price_str = str(row.get('price_lookup', '')).strip()
        manual_price = pd.to_numeric(manual_price_str, errors='coerce')
        yf_ticker = ""
        
        if pd.notna(manual_price) and manual_price > 0:
            yf_ticker = f"MANUAL_{ticker}" 
            price_dict[yf_ticker] = manual_price
            print(f"     [P] 수동 가격 사용: '{ticker}' ({name}) -> {manual_price:.1f}")
        else:
            # [핵심 수술 부위 2] 영문 혼합 ETF 무조건 네이버 직행 로직
            is_korean_mixed = any(c.isalpha() for c in ticker) and exchange in ['KOSPI', 'KOSDAQ', 'ETF', 'ETN', '']
            
            if is_korean_mixed:
                print(f"     [N] 영문 혼합 코드 감지: '{ticker}' ({name}). 네이버 API 호출...")
                naver_price = get_current_price_naver(ticker)
                
                if naver_price is not None and naver_price > 0:
                    yf_ticker = f"NAVER_{ticker}"
                    price_dict[yf_ticker] = naver_price
                    print(f"         -> 네이버 조회 성공: {naver_price:,.0f}원")
                else:
                    print("         -> 네이버 실패. 야후 파이낸스로 재시도합니다.")
                    yf_ticker = f"{ticker}.KS"
            else:
                if exchange in ['KOSPI', 'ETF', 'ETN']: yf_ticker = f"{ticker}.KS"
                elif exchange == 'KOSDAQ': yf_ticker = f"{ticker}.KQ"
                elif exchange == 'HKG': yf_ticker = f"{ticker.lstrip('0')}.HK"
                elif exchange == 'SSE': yf_ticker = f"{ticker}.SS"
                elif exchange == 'SZSE': yf_ticker = f"{ticker}.SZ"
                elif exchange == 'TSE': yf_ticker = f"{ticker}.T"
                elif exchange in ['NASDAQ', 'NYSE', 'AMEX']: yf_ticker = ticker
                elif exchange == '' or exchange == 'OTC' or exchange == '비상장':
                    print(f"     [!] '{ticker}' ({name}) 거래소 정보 없음. 건너뜀.")
                    yf_ticker = f"SKIP_{ticker}"
                else:
                    if ticker.isnumeric() and len(ticker) == 6: yf_ticker = f"{ticker}.KS"
                    else: yf_ticker = ticker
        
        yf_tickers_map[index] = yf_ticker
        if not yf_ticker.startswith('SKIP_') and not yf_ticker.startswith('MANUAL_') and not yf_ticker.startswith('NAVER_'):
            yf_tickers_to_fetch.append(yf_ticker)

    unique_yf_tickers = sorted(list(set(yf_tickers_to_fetch)))
    print(f"  [Core 3] 2. yfinance 개별 조회 시작 ({len(unique_yf_tickers)}개)...")
    
    if unique_yf_tickers:
        try:
            yf_data = yf.download(tickers=unique_yf_tickers, period="5d", interval="1d", progress=False)
            for yf_ticker in unique_yf_tickers:
                try:
                    last_price = 0.0; series = pd.Series()
                    if len(unique_yf_tickers) == 1 and not yf_data.empty: series = yf_data['Close']
                    elif ('Close', yf_ticker) in yf_data.columns: series = yf_data[('Close', yf_ticker)]
                    
                    valid_values = series.dropna()
                    if not valid_values.empty: last_price = float(valid_values.iloc[-1])
                    else:
                        if yf_ticker.endswith(".KS"):
                            retry_ticker = yf_ticker.replace(".KS", ".KQ")
                            retry_data = yf.download(tickers=[retry_ticker], period="5d", interval="1d", progress=False)
                            if not retry_data.empty and 'Close' in retry_data.columns:
                                last_price = float(retry_data['Close'].dropna().iloc[-1])
                                yf_tickers_map = {k: (retry_ticker if v == yf_ticker else v) for k, v in yf_tickers_map.items()}
                    price_dict[yf_ticker] = last_price
                except: price_dict[yf_ticker] = 0.0
        except:
            for yf_ticker in unique_yf_tickers: price_dict[yf_ticker] = 0.0
            
    print("  [Core 3] yfinance 개별 조회 완료.\n  [Core 3] 3. 환율 정보 조회 중...")
    
    currencies_to_fetch = set()
    if 'currency' in df_to_fetch_prices.columns:
        currencies_to_fetch.update(df_to_fetch_prices[~df_to_fetch_prices['currency'].isin(['KRW', ''])]['currency'].unique())
    if intl_transactions is not None and not intl_transactions.empty and 'currency' in intl_transactions.columns:
        currencies_to_fetch.update(intl_transactions[~intl_transactions['currency'].isin(['KRW', '', None])]['currency'].unique())
    
    currencies_to_fetch = [curr for curr in currencies_to_fetch if pd.notna(curr) and str(curr).strip() != '']
    exchange_rates = {'KRW': 1.0}
    
    for curr in currencies_to_fetch:
        curr_upper = str(curr).strip().upper()
        if curr_upper not in exchange_rates:
            try:
                rate_ticker = "CNHKRW=X" if curr_upper == 'CNY' else f"{curr_upper}KRW=X"
                rate_data = yf.Ticker(rate_ticker).history(period='5d')
                exchange_rates[curr_upper] = float(rate_data['Close'].iloc[-1]) if not rate_data.empty else 1.0
            except: exchange_rates[curr_upper] = 1.0
            print(f"     - {curr_upper}/KRW: {exchange_rates[curr_upper]:.2f}")
    
    print("  [Core 3] 4. 현재가 및 원화 환산 적용 중...")
    df['current_price'] = 0.0; df['current_price_krw'] = 0.0; df['yf_ticker'] = ''
    
    for i, row in df.iterrows():
        if row.get('quantity', 0) <= 0.0001: continue
        yf_ticker = yf_tickers_map.get(i, '')
        original_currency = 'KRW' if yf_ticker.startswith('MANUAL_') or yf_ticker.startswith('NAVER_') else str(row.get('currency', 'KRW')).upper()
        
        rate = exchange_rates.get(original_currency, 1.0)
        current_price = price_dict.get(yf_ticker, 0.0)
        
        df.loc[i, 'current_price'] = current_price
        df.loc[i, 'current_price_krw'] = current_price * rate 
        df.loc[i, 'yf_ticker'] = yf_ticker
        
    print("  [Core 3] 현재가 조회 및 원화 환산 완료.")
    return df, exchange_rates


# --- [엔진 4] 계좌별 현금 잔고 계산 (v51, v59) ---
def calculate_cash_balances(domestic_transactions, intl_transactions, current_rates):
    """
    '전체 기간'의 거래 내역을 바탕으로 계좌별/통화별 현금 잔고를 계산합니다.
    (v121 업데이트: backfill과 동일한 원화/외화 격벽 분리 로직 적용)
    """
    print("\n[Core 4] 현재 현금 잔고 계산 중 (v121: 격벽 분리 적용)...")
    
    # [핵심 수술 부위] 국내/해외 엑셀 구분 없이 모든 거래를 합친 후 통화(Currency) 기준으로 철저히 분리
    all_txns = pd.concat([domestic_transactions, intl_transactions], ignore_index=True)
    
    # 1. 원화(KRW) 예수금: 오직 'KRW'로 기록된 거래의 원화 정산금액만 합산
    krw_txns = all_txns[all_txns['currency'] == 'KRW']
    krw_balances_account = krw_txns.groupby('account')['settlement_krw'].sum()
    
    print("  [Core 4] '전체 기간' 거래 기준 원화(KRW) 예수금 (계좌별):")
    if krw_balances_account.empty:
        print("      - 원화 예수금 없음.")
    for account_id, balance in krw_balances_account.items():
        print(f"      - 계좌 [{account_id}]: {balance:,.0f} 원")

    # 2. 외화 예수금: 'KRW'가 아닌 거래의 외화 정산금액만 합산
    fx_txns = all_txns[all_txns['currency'] != 'KRW']
    foreign_balances_account = pd.Series(dtype='float64')
    if not fx_txns.empty and 'settlement_foreign' in fx_txns.columns:
        foreign_balances_account = fx_txns.groupby(['account', 'currency'])['settlement_foreign'].sum()
        
    print("  [Core 4] '전체 기간' 거래 기준 외화 예수금 (계좌별, 통화별):")
    
    fx_balances_krw_account = {}
    if foreign_balances_account.empty:
        print("      - 외화 예수금 없음.")
    else:
        for (account_id, curr), balance in foreign_balances_account.items():
            if curr == 'KRW' or pd.isna(curr) or curr == '': continue
            rate = current_rates.get(str(curr).upper(), 1.0)
            balance_krw = balance * rate
            
            if account_id not in fx_balances_krw_account:
                fx_balances_krw_account[account_id] = 0.0
            fx_balances_krw_account[account_id] += balance_krw
            
            print(f"      - 계좌 [{account_id}] {curr}: {balance:,.2f} (원화 환산: {balance_krw:,.0f} 원, 환율: {rate:.2f})")

    print("  [Core 4] 최종 데이터에 '계좌별 현금' 행 추가 중...")
    
    all_cash_rows = []
    all_accounts = set(krw_balances_account.index) | set(fx_balances_krw_account.keys())
    
    total_cash_for_dashboard = 0.0

    for acc in all_accounts:
        krw_cash = krw_balances_account.get(acc, 0.0)
        fx_cash_krw = fx_balances_krw_account.get(acc, 0.0)
        
        total_cash_for_dashboard += (krw_cash + fx_cash_krw)
        
        # (원본 유지) 대시보드 포맷에 맞춘 공통 속성
        common_cash_attrs = {
            'quantity': 0, 'avg_cost_krw': 1.0, 'current_price_krw': 1.0,
            'unrealized_pl_krw': 0.0, 'return_rate': 0.0,
            'realized_pl_krw': 0.0, 'cumulative_pl_krw': 0.0,
            'asset_class': '현금', 'country': '통합', 'theme': '안전 자산',
            'postion': '방어', 'maket_phase': '', 'exchange': 'CASH', 'currency': 'KRW'
        }
        
        # 원화 현금 데이터 추가 (잔고가 있을 때만)
        if abs(krw_cash) > 10:
            cash_row_krw = {
                'account': acc, 'ticker': f'CASH_KRW_{acc}', 'name': '원화 예수금',
                'total_cost_krw': krw_cash, 'market_value_krw': krw_cash,
                'yf_ticker': 'CASH_KRW', **common_cash_attrs
            }
            all_cash_rows.append(cash_row_krw)
        
        # 외화 현금 데이터 추가 (잔고가 있을 때만)
        if abs(fx_cash_krw) > 10:
            cash_row_fx = {
                'account': acc, 'ticker': f'CASH_FX_{acc}', 'name': '외화 예수금 (원화환산)',
                'total_cost_krw': fx_cash_krw, 'market_value_krw': fx_cash_krw,
                'yf_ticker': 'CASH_FX', **common_cash_attrs
            }
            all_cash_rows.append(cash_row_fx)
        
    if all_cash_rows:
        df_cash_rows = pd.DataFrame(all_cash_rows)
        return df_cash_rows, total_cash_for_dashboard
    else:
        return pd.DataFrame(), 0.0


# --- [엔진 5] 일일 시장 지표 수집 (v70.30 - pykrx 하이브리드) ---
def fetch_daily_market_data():
    """
    [v70.30] 하이브리드 자동화
    - yfinance: 가격/지수 데이터 (미국장 마감 직후)
    - pykrx: 국내 거래량, 예탁금, 신용잔고 (어제 마감 기준)
    - KR 국채 금리: 데이터 미제공으로 공란 유지
    """
    print("\n[Core 5] 일일 시장 지표 수집 시작 (v70.30 - pykrx 추가)...")
    
    # 1. 37개 열에 맞는 빈 리스트 초기화
    final_row_data = [""] * 37
    today = datetime.now()
    final_row_data[0] = today.strftime('%Y-%m-%d') # Date

    # --- [A] yfinance 데이터 수집 (기존 로직 유지) ---
    yf_tickers = [
        '^KS11', '^KQ11', '^GSPC', '^IXIC', '000001.SS', '^GDAXI',
        'USDKRW=X', 'DX-Y.NYB', '^TNX', '^TYX', 'CL=F', 'GC=F', 'BTC-USD', '^VIX'
    ]
    base_ticker = yf_tickers[0]
    
    print("  [Core 5-A] yfinance 데이터 수집 중...")
    try:
        # [패치 2 적용 확인] 사용자 코드에 이미 period="5d"가 적용되어 있어 그대로 유지
        data = yf.download(tickers=yf_tickers, period="5d", interval="1d", progress=False)
        
        if data.empty or len(data) < 2:
            print("  [!!!] yfinance 데이터 부족. 빈 리스트 반환.")
            return []

        latest_row = data.iloc[-1]
        
        valid_data = data.dropna(subset=[('Close', base_ticker)])
        if valid_data.empty: return []
            
        target_row = valid_data.iloc[-1]
        prev_row = valid_data.iloc[-2] if len(valid_data) >= 2 else target_row

        def get_val(ticker, type='price'):
            try:
                price = 0.0; chg = 0.0
                if ('Close', ticker) in target_row.index:
                    price = target_row[('Close', ticker)]
                    if pd.isna(price): price = 0.0
                
                if ('Close', ticker) in prev_row.index:
                    p_prev = prev_row[('Close', ticker)]
                    if price != 0 and p_prev != 0:
                        chg = (price / p_prev) - 1
                        if ticker in ['^TNX', '^TYX']: chg = (price - p_prev) * 100
                
                if type == 'price': return f"{price:.2f}"
                if type == 'chg': 
                    if ticker in ['^TNX', '^TYX']: return f"{chg:.2f}"
                    else: return f"{chg*100:.2f}%"
            except: return "0.00"
            return "0.00"

        # yfinance 데이터 매핑
        final_row_data[1] = get_val('^KS11', 'price')
        final_row_data[2] = get_val('^KS11', 'chg')
        # [3] KOSPI Vol -> 아래 pykrx에서 채움
        
        final_row_data[4] = get_val('^KQ11', 'price')
        final_row_data[5] = get_val('^KQ11', 'chg')
        # [6] KOSDAQ Vol -> 아래 pykrx에서 채움
        
        final_row_data[7] = get_val('^GSPC', 'price')
        final_row_data[8] = get_val('^GSPC', 'chg')
        final_row_data[9] = get_val('^IXIC', 'price')
        final_row_data[10] = get_val('^IXIC', 'chg')
        final_row_data[11] = get_val('000001.SS', 'price')
        final_row_data[12] = get_val('000001.SS', 'chg')
        final_row_data[13] = get_val('^GDAXI', 'price')
        final_row_data[14] = get_val('^GDAXI', 'chg')
        final_row_data[15] = get_val('USDKRW=X', 'price')
        final_row_data[16] = get_val('USDKRW=X', 'chg')
        final_row_data[17] = get_val('DX-Y.NYB', 'price')
        final_row_data[18] = get_val('DX-Y.NYB', 'chg')
        final_row_data[19] = get_val('^TNX', 'price')
        final_row_data[20] = get_val('^TNX', 'chg')
        final_row_data[21] = get_val('^TYX', 'price')
        final_row_data[22] = get_val('^TYX', 'chg')
        final_row_data[23] = get_val('CL=F', 'price')
        final_row_data[24] = get_val('CL=F', 'chg')
        final_row_data[25] = get_val('GC=F', 'price')
        final_row_data[26] = get_val('GC=F', 'chg')
        final_row_data[27] = get_val('BTC-USD', 'price')
        final_row_data[28] = get_val('BTC-USD', 'chg')
        final_row_data[29] = get_val('^VIX', 'price')
        final_row_data[30] = get_val('^VIX', 'chg')

    except Exception as e:
        print(f"  [!!!] yfinance 수집 중 오류: {e}")
        # 오류 나도 pykrx 시도 위해 return하지 않고 진행

    # --- [B] pykrx 데이터 수집 (거래대금, 예탁금, 신용잔고) ---
    print("  [Core 5-B] pykrx 데이터 수집 중 (거래대금/예탁금/신용잔고)...")
    try:
        # 날짜 설정: 오늘 새벽 실행 -> 어제 마감 데이터 필요
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=8)
        
        str_yesterday = yesterday.strftime("%Y%m%d")
        str_week_ago = week_ago.strftime("%Y%m%d")

        # 1. KOSPI / KOSDAQ 거래대금 (Value)
        # 지수 티커: KOSPI='1001', KOSDAQ='2001'
        try:
            # 기간 조회 후 마지막 값 사용
            df_kospi_vol = stock.get_index_ohlcv_by_date(str_week_ago, str_yesterday, "1001")
            df_kosdaq_vol = stock.get_index_ohlcv_by_date(str_week_ago, str_yesterday, "2001")

            if not df_kospi_vol.empty:
                # [수정] '거래량' -> '거래대금' 변경 및 단위 환산 (원 -> 억원)
                last_val_kospi = df_kospi_vol['거래대금'].iloc[-1]
                val_kospi_100m = last_val_kospi / 100000000 # 1억으로 나누기
                final_row_data[3] = f"{val_kospi_100m:.0f}" # 소수점 없이 정수만 (HTS 스타일)
            
            if not df_kosdaq_vol.empty:
                last_val_kosdaq = df_kosdaq_vol['거래대금'].iloc[-1]
                val_kosdaq_100m = last_val_kosdaq / 100000000 # 1억으로 나누기
                final_row_data[6] = f"{val_kosdaq_100m:.0f}" # 소수점 없이 정수만
                
            print(f"     [+] 거래대금(억원) 업데이트 완료: KOSPI {final_row_data[3]}억, KOSDAQ {final_row_data[6]}억")
            
        except Exception as e:
            print(f"     [!] 거래대금 조회 실패: {e}")

        # 2. 예탁금 & 신용잔고 (억원 단위로 변환)
        try:
            df_deposit = stock.get_customer_deposit_trend(str_week_ago, str_yesterday)
            
            if not df_deposit.empty and len(df_deposit) >= 1:
                # 원본(백만원) -> 억원 변환 (나누기 100)
                curr_deposit = df_deposit['고객예탁금'].iloc[-1] / 100
                curr_credit = df_deposit['신용잔고'].iloc[-1] / 100
                
                prev_deposit = df_deposit['고객예탁금'].iloc[-2] / 100 if len(df_deposit) >= 2 else curr_deposit
                prev_credit = df_deposit['신용잔고'].iloc[-2] / 100 if len(df_deposit) >= 2 else curr_credit

                # 데이터 채우기 (소수점 없이 정수 반올림 추천)
                final_row_data[33] = f"{curr_deposit:.0f}" # 억원
                
                dep_chg = 0.0
                if prev_deposit != 0: dep_chg = (curr_deposit / prev_deposit) - 1
                final_row_data[34] = f"{dep_chg:.6f}"

                final_row_data[35] = f"{curr_credit:.0f}" # 억원
                
                cred_chg = 0.0
                if prev_credit != 0: cred_chg = (curr_credit / prev_credit) - 1
                final_row_data[36] = f"{cred_chg:.6f}"
                
                print(f"     [+] 예탁금/신용잔고(억원) 업데이트 완료: 예탁금 {final_row_data[33]}억")
            else:
                print("     [!] 예탁금 데이터 기간 내 없음.")

        except Exception as e:
             print(f"     [!] 예탁금/신용잔고 조회 실패: {e}")

    except Exception as e:
        print(f"  [!!!] pykrx 수집 중 치명적 오류: {e}")

    # [31~32] KR Bond Rate (공란 유지 - 신뢰할 수 있는 무료 소스 없음)
    
    print("  [Core 5] 데이터 수집 완료 (pykrx 포함).")
    return final_row_data
