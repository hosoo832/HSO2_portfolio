import pandas as pd
import numpy as np
import yfinance as yf
from pykrx import stock 
from datetime import datetime, timedelta
import time
import warnings
import json

warnings.filterwarnings("ignore")

import config
import google_api
import data_transformer
import finance_core

def safe_int(val):
    try:
        if pd.isna(val) or val == "" or str(val).strip() == "": return 0
        val = float(val)
        if np.isinf(val): return 0
        return int(val)
    except:
        return 0

def safe_float(val):
    try:
        if pd.isna(val) or val == "" or str(val).strip() == "": return 0.0
        val = float(val)
        if np.isinf(val): return 0.0
        return val
    except:
        return 0.0

TICKER_MANUAL_MAP = {
    'FB': 'META', 'Google': 'GOOGL', 'BRK.B': 'BRK-B',
    '252670': '252670.KS', '114800': '114800.KS', '122630': '122630.KS', 
    '233740': '233740.KS', '251340': '251340.KS', 
}

TARGET_COLUMNS = [
    'date', 'account', 'ticker', 'name', 
    'asset_class', 'country', 'theme', 'exchange', 
    'quantity', 'current_price_krw', 'avg_cost_krw', 
    'total_cost_krw', 'net_invested_capital', 
    'market_value_krw', 'unrealized_pl_krw', 
    'realized_pl_krw', 'cumulative_pl_krw', 'return_rate'
]

def get_smart_ticker(ticker, exchange_map):
    t = str(ticker).strip()
    if t in TICKER_MANUAL_MAP: return TICKER_MANUAL_MAP[t]
    exchange = exchange_map.get(t, '').upper()
    if exchange == 'HKG': return f"{t.lstrip('0')}.HK"
    if exchange == 'SSE': return f"{t}.SS"
    if exchange == 'SZSE': return f"{t}.SZ"
    if exchange in ['NASDAQ', 'NYSE', 'AMEX']: return t
    if t.isdigit(): return f"{t}.KS"
    return t

def run_backfill():
    print("--- [Backfill v126 Final] 수동가격(price_lookup) 연동 및 pykrx 시계열 보완 ---")
    
    try:
        _, master_sheet_inst = google_api.get_all_records_as_text(config.SHEET_MASTER_DATA)
        doc_container = master_sheet_inst.spreadsheet
        try:
            log_sheet = doc_container.worksheet("portfolio_log")
            log_sheet.clear() 
        except:
            log_sheet = doc_container.add_worksheet(title="portfolio_log", rows=1000, cols=20)
        log_sheet.append_row(TARGET_COLUMNS)
    except Exception as e:
        print(f"  [!] 연결 실패: {e}")
        return

    df_domestic, _ = google_api.get_all_records_as_text(config.SHEET_RAW_DOMESTIC)
    df_intl, _ = google_api.get_all_records_as_text(config.SHEET_RAW_INTL)
    df_master, _ = google_api.get_all_records_as_text(config.SHEET_MASTER_DATA)

    exchange_map = {}
    if not df_master.empty:
        df_master['ticker'] = df_master['ticker'].astype(str).str.strip()
        df_master = df_master.drop_duplicates(subset=['ticker'])
        for _, row in df_master.iterrows():
            if pd.notna(row['ticker']):
                exchange_map[str(row['ticker'])] = str(row.get('exchange', '')).upper()

    domestic_txn = data_transformer.transform_domestic(df_domestic)
    intl_txn = data_transformer.transform_international(df_intl)
    
    if intl_txn.empty:
         intl_txn = pd.DataFrame(columns=['account', 'date', 'action_type', 'ticker', 'name', 'quantity', 'settlement_krw', 'currency'])

    df_all_txn = pd.concat([domestic_txn, intl_txn], ignore_index=True)
    df_all_txn['date'] = pd.to_datetime(df_all_txn['date'], errors='coerce')
    df_all_txn = df_all_txn.dropna(subset=['date']) 
    df_all_txn['account'] = df_all_txn['account'].astype(str).str.strip()
    df_all_txn['settlement_krw'] = pd.to_numeric(df_all_txn['settlement_krw'], errors='coerce').fillna(0)
    if 'settlement_foreign' not in df_all_txn.columns: df_all_txn['settlement_foreign'] = 0.0
    df_all_txn['settlement_foreign'] = pd.to_numeric(df_all_txn['settlement_foreign'], errors='coerce').fillna(0)

    start_date = "2020-01-01" 
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    target_dates = pd.date_range(start=start_date, end=yesterday, freq='D') 
    
    all_tickers = df_all_txn['ticker'].unique().tolist()
    valid_tickers = [t for t in all_tickers if t and str(t).upper() not in ['KRW', 'USD', 'CASH', 'NAN', '']]
    yf_tickers = [get_smart_ticker(t, exchange_map) for t in valid_tickers]
    unique_yf = list(set(yf_tickers))
    rate_tickers = ['KRW=X', 'USDKRW=X', 'HKDKRW=X', 'CNYKRW=X', 'JPYKRW=X']
    
    try:
        price_history = yf.download(unique_yf, start=start_date, end=datetime.now(), progress=False, auto_adjust=True)['Close']
        rate_history = yf.download(rate_tickers, start=start_date, end=datetime.now(), progress=False, auto_adjust=True)['Close']
        if not price_history.empty and price_history.index.tz is not None: price_history.index = price_history.index.tz_localize(None)
        if not rate_history.empty and rate_history.index.tz is not None: rate_history.index = rate_history.index.tz_localize(None)
    except:
        price_history = pd.DataFrame()
        rate_history = pd.DataFrame()

    failed_kr_tickers = []
    for t in valid_tickers:
        exch = exchange_map.get(str(t), '').upper()
        if exch in ['KOSPI', 'KOSDAQ', 'ETF', 'ETN', '']:
            yf_t = get_smart_ticker(t, exchange_map)
            is_mixed = any(c.isalpha() for c in str(t))
            if is_mixed or (not price_history.empty and yf_t not in price_history.columns) or price_history.empty:
                if not str(t).startswith("FUND_"):
                    failed_kr_tickers.append(str(t))

    failed_kr_tickers = list(set(failed_kr_tickers))
    if failed_kr_tickers:
        print(f"  [Pykrx 보완] 야후 조회 실패/알파벳 혼합 ETF {len(failed_kr_tickers)}개 발견. 시계열 데이터 수집 중...")
        start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
        end_str = datetime.now().strftime("%Y%m%d")
        
        for t in failed_kr_tickers:
            try:
                df_krx = stock.get_market_ohlcv(start_str, end_str, t)
                if not df_krx.empty:
                    yf_t = get_smart_ticker(t, exchange_map)
                    df_krx.index = df_krx.index.tz_localize(None)
                    
                    if price_history.empty: price_history = pd.DataFrame(index=df_krx.index)
                    if yf_t not in price_history.columns: price_history[yf_t] = np.nan
                        
                    price_history.loc[df_krx.index, yf_t] = df_krx['종가']
                    print(f"     -> {t} pykrx 과거 데이터 장착 완료!")
            except Exception as e:
                print(f"     -> {t} pykrx 보완 실패: {e}")

    history_log_list = []
    BATCH_SIZE = 100
    processed_count = 0

    for cut_off_date in target_dates:
        date_str = cut_off_date.strftime('%Y-%m-%d')
        mask_date = df_all_txn['date'] <= cut_off_date
        past_txns = df_all_txn.loc[mask_date].copy()
        if past_txns.empty: continue
        
        df_nic_calc = past_txns[past_txns['action_type'] == 'Transfer']
        nic_by_account = df_nic_calc.groupby('account')['settlement_krw'].sum().to_dict()
        
        krw_txns = past_txns[past_txns['currency'] == 'KRW']
        cash_krw_by_acc = krw_txns.groupby('account')['settlement_krw'].sum().to_dict()

        fx_txns = past_txns[past_txns['currency'] != 'KRW']
        cash_fx_by_acc_curr = fx_txns.groupby(['account', 'currency'])['settlement_foreign'].sum().to_dict()
        
        fx_krw_by_acc = {}
        for (acc, curr), bal_foreign in cash_fx_by_acc_curr.items():
            if abs(bal_foreign) < 0.01: continue
            rate = 1.0
            r_ticker = f"{curr}KRW=X"
            
            if r_ticker in rate_history.columns:
                r_s = rate_history[r_ticker].dropna()
                idx = r_s.index.asof(cut_off_date)
                if pd.notna(idx): rate = safe_float(r_s.loc[idx])
                
            if rate == 1.0 and curr == 'USD':
                if 'KRW=X' in rate_history.columns:
                    r_s = rate_history['KRW=X'].dropna()
                    idx = r_s.index.asof(cut_off_date)
                    if pd.notna(idx): rate = safe_float(r_s.loc[idx])
                    
            if rate == 1.0: 
                rate = data_transformer.get_historical_rate_cached_yf(pd.to_datetime(cut_off_date), curr, 'KRW')

            bal_krw = bal_foreign * rate
            fx_krw_by_acc[acc] = fx_krw_by_acc.get(acc, 0.0) + bal_krw

        df_holdings = pd.DataFrame() 
        try: 
            result_df = finance_core.calculate_holdings_and_realized_pl(past_txns)
            if not result_df.empty: df_holdings = result_df
        except Exception as e: pass 

        cash_rows = []
        for acc, bal in cash_krw_by_acc.items():
            bal_safe = safe_float(bal)
            if abs(bal_safe) > 10: 
                cash_rows.append({
                    'account': acc, 'ticker': f'CASH_KRW_{acc}', 'name': '원화 예수금',
                    'quantity': 0, 'avg_cost_krw': 1, 'total_cost_krw': bal_safe, 'market_value_krw': bal_safe, 
                    'asset_class': '현금', 'country': '통합', 'theme': '안전 자산', 'exchange': 'CASH', 'currency': 'KRW', 'realized_pl_krw': 0
                })
        
        for acc, bal_krw in fx_krw_by_acc.items():
            if abs(bal_krw) > 10:
                cash_rows.append({
                    'account': acc, 'ticker': f'CASH_FX_{acc}', 'name': '외화 예수금 (원화환산)',
                    'quantity': 0, 'avg_cost_krw': 1, 'total_cost_krw': bal_krw, 'market_value_krw': bal_krw,
                    'asset_class': '현금', 'country': '통합', 'theme': '안전 자산', 'exchange': 'CASH', 'currency': 'KRW', 'realized_pl_krw': 0
                })

        if cash_rows:
            df_cash_raw = pd.DataFrame(cash_rows)
            df_cash_grouped = df_cash_raw.groupby(['account', 'ticker', 'name', 'asset_class', 'country', 'theme', 'exchange', 'currency', 'avg_cost_krw'])[['total_cost_krw', 'market_value_krw']].sum().reset_index()
            df_cash_grouped['quantity'] = 0; df_cash_grouped['realized_pl_krw'] = 0
            
            if df_holdings.empty: df_holdings = df_cash_grouped
            else: df_holdings = pd.concat([df_holdings, df_cash_grouped], ignore_index=True)

        if df_holdings.empty: continue

        cols_to_use = ['ticker', 'name', 'asset_class', 'country', 'theme', 'exchange', 'currency']
        if 'price_lookup' in df_master.columns: cols_to_use.append('price_lookup') 
        if 'manual_avg_cost' in df_master.columns: cols_to_use.append('manual_avg_cost')
            
        df_merged = pd.merge(df_holdings, df_master[cols_to_use], on='ticker', how='left', suffixes=('', '_m'))
        
        if 'name_m' in df_merged.columns: df_merged['name'] = df_merged['name_m'].fillna(df_merged['name'])
        if 'asset_class_m' in df_merged.columns: df_merged['asset_class'] = df_merged['asset_class_m'].fillna(df_merged['asset_class'])
        if 'exchange_m' in df_merged.columns: df_merged['exchange'] = df_merged['exchange_m'].fillna(df_merged['exchange'])
        df_merged['exchange'] = df_merged.apply(lambda x: exchange_map.get(str(x['ticker']), x['exchange']), axis=1)

        numeric_cols = ['quantity', 'avg_cost_krw', 'total_cost_krw', 'realized_pl_krw']
        for col in numeric_cols:
            if col in df_merged.columns: df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').fillna(0)

        rows_for_date = []
        for _, row in df_merged.iterrows():
            try:
                ticker = row['ticker']
                is_cash = str(ticker).startswith('CASH_') 
                qty = safe_float(row['quantity'])
                if not is_cash and abs(qty) < 0.0001: continue 
                
                avg_cost = safe_float(row['avg_cost_krw'])
                manual_cost = safe_float(row.get('manual_avg_cost', 0))
                if manual_cost > 0: avg_cost = manual_cost
                
                manual_lookup_price = 0.0
                if 'price_lookup' in row and pd.notna(row['price_lookup']):
                    try: manual_lookup_price = float(str(row['price_lookup']).replace(',', ''))
                    except: pass
                
                current_price = avg_cost 
                exchange = str(row.get('exchange', '')).upper()
                currency = str(row.get('currency', '')).upper()
                
                if exchange == 'HKG': currency = 'HKD'
                elif exchange in ['NASDAQ', 'NYSE', 'AMEX']: currency = 'USD'
                elif exchange in ['SSE', 'SZSE']: currency = 'CNY'
                elif exchange in ['KOSPI', 'KOSDAQ']: currency = 'KRW'
                elif currency == '' or currency == 'NAN': 
                    currency = 'USD' if str(ticker).upper() in ['FB', 'META', 'GOOGL', 'AAPL', 'AMZN', 'TSLA', 'NVDA'] else 'KRW'

                if is_cash:
                    current_price = 1.0; market_val = row['market_value_krw']; total_cost = row['total_cost_krw']
                else:
                    yf_t = get_smart_ticker(ticker, exchange_map)
                    price_fetched_success = False; raw_price = 0.0
                    
                    if manual_lookup_price > 0:
                        raw_price = manual_lookup_price
                        price_fetched_success = True
                    else:
                        try:
                            if yf_t in price_history.columns:
                                series = price_history[yf_t].dropna()
                                idx = series.index.asof(cut_off_date)
                                if pd.notna(idx):
                                    raw_price = safe_float(series.loc[idx])
                                    if raw_price > 0: price_fetched_success = True
                        except: pass

                    if price_fetched_success:
                        rate = 1.0
                        if currency != 'KRW':
                            rate_ticker = f"{currency}KRW=X"
                            if rate_ticker in rate_history.columns:
                                r_series = rate_history[rate_ticker].dropna()
                                r_idx = r_series.index.asof(cut_off_date)
                                if pd.notna(r_idx): rate = safe_float(r_series.loc[r_idx])
                            if rate == 1.0 and currency == 'USD':
                                if 'KRW=X' in rate_history.columns:
                                    r_series = rate_history['KRW=X'].dropna()
                                    r_idx = r_series.index.asof(cut_off_date)
                                    if pd.notna(r_idx): rate = safe_float(r_series.loc[r_idx])
                            
                            if rate == 1.0: rate = data_transformer.get_historical_rate_cached_yf(pd.to_datetime(cut_off_date), currency, 'KRW')
                        
                        current_price = raw_price * rate
                    else:
                        current_price = avg_cost
                    
                    market_val = qty * current_price; total_cost = avg_cost * qty

                acc_id_str = str(row['account']).strip()
                nic_val = safe_int(nic_by_account.get(acc_id_str, 0))
                unrealized = market_val - total_cost
                if is_cash: unrealized = 0
                realized_pl = safe_int(row.get('realized_pl_krw', 0))
                cumulative_pl = safe_int(unrealized + realized_pl)
                
                return_rate = round((unrealized / total_cost) * 100, 2) if abs(total_cost) > 0 else 0.0

                log_entry = {
                    'date': date_str, 'account': acc_id_str, 'ticker': ticker, 'name': row.get('name', ticker),
                    'asset_class': row.get('asset_class', ''), 'country': row.get('country', ''), 'theme': row.get('theme', ''),
                    'exchange': exchange, 'quantity': qty, 'current_price_krw': round(current_price, 2),
                    'avg_cost_krw': round(avg_cost, 2), 'total_cost_krw': safe_int(total_cost), 'net_invested_capital': nic_val,
                    'market_value_krw': safe_int(market_val), 'unrealized_pl_krw': safe_int(unrealized),
                    'realized_pl_krw': realized_pl, 'cumulative_pl_krw': cumulative_pl, 'return_rate': return_rate
                }
                rows_for_date.append(log_entry)
            except Exception: continue

        history_log_list.extend(rows_for_date)
        processed_count += 1
        print(f"  > {date_str} 완료 ({len(rows_for_date)}건)")

        if processed_count >= BATCH_SIZE:
             print("  [Upload] 구글 시트로 중간 전송 중...")
             df_batch = pd.DataFrame(history_log_list)
             for col in TARGET_COLUMNS:
                 if col not in df_batch.columns: df_batch[col] = ""
             df_batch = df_batch[TARGET_COLUMNS] 
             
             json_str = df_batch.fillna("").to_json(orient='values', date_format='iso')
             payload = json.loads(json_str)
             log_sheet.append_rows(payload)
             history_log_list = []; processed_count = 0; time.sleep(1)

    if history_log_list:
        print("  [Upload] 최종 데이터 전송 중...")
        df_final = pd.DataFrame(history_log_list)
        for col in TARGET_COLUMNS:
             if col not in df_final.columns: df_final[col] = ""
        df_final = df_final[TARGET_COLUMNS]
        json_str = df_final.fillna("").to_json(orient='values', date_format='iso')
        payload = json.loads(json_str)
        log_sheet.append_rows(payload)
        
    print("--- [완료] Backfill 완벽 종료! ---")

if __name__ == "__main__":
    run_backfill()