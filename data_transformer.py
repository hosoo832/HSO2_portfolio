# data_transformer.py

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

print("\n[Transformer] 데이터 변환 모듈(v109 - Split Logic Added)을 불러왔습니다.")

# --- [헬퍼 함수 1] 국내 거래내역 '번역' (v128 - 거래종류+적요명 쌍끌이 검사) ---
def classify_domestic_action(row):
    """[v128] 적요명까지 포함하여 퇴직연금 수수료, 재투자, 부담금 완벽 추적"""
    trade_type = str(row.get('거래종류', '')).strip()
    memo = str(row.get('적요명', '')).strip()       # [추가] 적요명 가져오기
    ticker_val = str(row.get('종목코드', '')).strip()
    name_val = str(row.get('종목명', '')).strip()

    # [핵심] 거래종류와 적요명을 하나로 합쳐서 검색 (어디에 적혀있든 무조건 잡아냄)
    search_text = trade_type + " " + memo

    # 1. 매매 (Trade)
    if '보통매매' in search_text or '재투자' in search_text: 
        action_detail = 'Sell' if '매도' in search_text else 'Buy'
        return 'Trade', action_detail, ticker_val, name_val
        
    # 2. 수익 (Income)
    elif '배당' in search_text or '분배' in search_text or 'ISA이벤트' in search_text:
        return 'Income', 'Dividend', None, '배당/분배금'
    elif '이용료' in search_text or '이자' in search_text:
        return 'Income', 'Interest', None, '이자'
        
    # 3. 환전 (FX)
    elif '외화매수' in search_text or '야간외화' in search_text:
        return 'FX', 'FX_Out', None, '외화매수 (원화출금)'
    elif '환전정산' in search_text or '외화매도' in search_text:
        return 'FX', 'FX_In', None, '외화매도 (원화입금)'
        
    # 4. 입출금 및 수수료 (Transfer)
    # [주의] '입출금'이라는 단어 때문에 꼬이는 것을 방지하기 위해 정교한 필터링
    cleaned_text = search_text.replace('입출금', '') 
    
    if '수수료' in cleaned_text or '출금' in cleaned_text:
        return 'Transfer', 'Withdraw', None, '출금/수수료'
    # 퇴직연금의 꽃 '부담금' 키워드 추가
    elif '입금' in cleaned_text or '부담금' in cleaned_text or '회사지원금' in cleaned_text: 
        return 'Transfer', 'Deposit', None, '입금/부담금'
        
    # 5. 주식 권리 (Stock Action)
    # [NEW] 액면분할 - 일반 입고/출고보다 먼저 체크!
    elif '액면분할병합출고' in search_text:
        return 'Trade', 'Split_Out', ticker_val, name_val
    elif '액면분할병합입고' in search_text:
        return 'Trade', 'Split_In', ticker_val, name_val
    elif '입고' in search_text or '해외이벤트입금' in search_text:
        return 'Trade', 'Stock_In', ticker_val, name_val
    elif '출고' in search_text: 
        if '청산' in search_text:
            return 'Trade', 'Liquidation', ticker_val, name_val
        return 'Trade', 'Stock_Out', ticker_val, name_val
        
    # 6. 기타 (Other)
    print(f"  [!!! v128 경고] 미분류 항목: 거래종류 '{trade_type}', 적요명 '{memo}' -> 0원 처리됨")
    return 'Other', search_text, None, search_text

# --- [함수 정의 1] 국내 거래내역 변환 (인덱스 에러 완벽 패치) ---
def transform_domestic(df):
    """'정산금액'/'거래수량' 필터링 시 인덱스 어긋남 방지 적용"""
    print("  [Transform] 국내 거래내역 변환 (총계정원장 방식 v66)...")
    
    if df.empty:
        print("  [Transform] 국내 거래내역 원본이 비어있습니다."); return pd.DataFrame()

    df = df.replace('', np.nan) 
    df = df.dropna(subset=['계좌번호', '거래종류', '거래일자'], how='any')
        
    df = df.dropna(subset=['정산금액', '거래수량'], how='all').copy()
    
    df_transformed = pd.DataFrame(index=df.index)
    df_transformed['account'] = df['계좌번호'].astype(str)
    df_transformed['date'] = pd.to_datetime(df['거래일자'], errors='coerce') 
    classified_cols = ['action_type', 'action_detail', 'ticker', 'name']
    df_transformed[classified_cols] = df.apply(classify_domestic_action, axis=1, result_type='expand')
    
    # [핵심 수술 부위] 숫자로 변환한 값을 변수가 아닌 df_transformed 내부에 직접 저장 (길이 어긋남 원천 차단)
    df_transformed['_settlement'] = pd.to_numeric(df['정산금액'].astype(str).str.replace(r'[^\d.-]', '', regex=True).replace('', '0'), errors='coerce')
    df_transformed['_quantity'] = pd.to_numeric(df['거래수량'].astype(str).str.replace(r'[^\d.-]', '', regex=True).replace('', '0'), errors='coerce')
    
    # (A) '정산금액'이 필요한 거래 유형
    needs_settlement = ['Dividend', 'Interest', 'Deposit', 'Withdraw', 'FX_In', 'FX_Out']
    mask_invalid_settlement = (
        df_transformed['action_detail'].isin(needs_settlement) & 
        (df_transformed['_settlement'].isna() | (df_transformed['_settlement'] == 0))
    )
    if mask_invalid_settlement.any():
        print(f"    [v66] '정산금액'이 비어있거나 0인 거래 {mask_invalid_settlement.sum()}건 무시.")
        df_transformed = df_transformed[~mask_invalid_settlement]

    # (B) '거래수량'이 필요한 거래 유형
    needs_quantity = ['Buy', 'Sell', 'Stock_In', 'Split_In', 'Split_Out']
    mask_invalid_quantity = (
        df_transformed['action_detail'].isin(needs_quantity) & 
        (df_transformed['_quantity'].isna() | (df_transformed['_quantity'] == 0))
    )
    if mask_invalid_quantity.any():
        print(f"    [v66] '거래수량'이 비어있거나 0인 거래 {mask_invalid_quantity.sum()}건 무시.")
        df_transformed = df_transformed[~mask_invalid_quantity]

    # NaN을 0으로 채움
    settlement_amount = df_transformed['_settlement'].fillna(0)
    df_transformed['quantity'] = df_transformed['_quantity'].fillna(0)

    # 부호 처리
    outflow_mask = df_transformed['action_detail'].isin(['Buy', 'FX_Out', 'Withdraw'])
    inflow_mask = df_transformed['action_detail'].isin(['Sell', 'FX_In', 'Dividend', 'Interest', 'Deposit'])
    
    settlement_amount_final = pd.Series(0.0, index=df_transformed.index)
    settlement_amount_final[outflow_mask] = settlement_amount[outflow_mask].abs() * -1
    settlement_amount_final[inflow_mask] = settlement_amount[inflow_mask].abs()
    
    df_transformed['settlement_krw'] = settlement_amount_final
    df_transformed['currency'] = 'KRW'
    
    print(f"  [Transform] 국내 거래 {len(df_transformed)}건 처리 완료."); 
    return df_transformed[['account', 'date', 'action_type', 'action_detail', 'ticker', 'name', 'quantity', 'settlement_krw', 'currency']]

# --- [헬퍼 함수 2] 과거 환율 조회 (캐시, yfinance) ---
# (원본 v60 로직)
historical_rates_cache = {}

def fetch_yf_rate(ticker, date_obj):
    """지정된 날짜(또는 그 이전 7일)의 yfinance 종가를 가져옵니다."""
    # print(f"    [Rate Fetch] {date_obj.strftime('%Y-%m-%d')} {ticker} 조회 시도...")
    current_date = date_obj
    for i in range(7): # 7일간 역추적
        try:
            start_date = current_date; end_date = current_date + timedelta(days=1)
            rate_data = yf.Ticker(ticker).history(start=start_date, end=end_date)
            if not rate_data.empty and 'Close' in rate_data:
                rate = float(rate_data['Close'].iloc[0])
                # print(f"      -> {ticker} 성공: {rate:.2f} (조회 날짜: {current_date.strftime('%Y-%m-%d')})"); 
                return rate
            else:
                if i < 6: 
                    # print(f"      [!] {ticker}: {current_date.strftime('%Y-%m-%d')} 데이터 없음. 하루 전 재시도..."); 
                    current_date -= timedelta(days=1)
                else: 
                    print(f"      [!!!] 7일간 {ticker} 데이터 없음. 1.0 사용."); 
                    return 1.0
        except Exception as e: 
            print(f"      [!!!] {ticker} yfinance 환율 조회 중 오류 발생: {e}. 1.0 사용."); 
            return 1.0
    return 1.0

def get_historical_rate_cached_yf(date_obj, from_curr, to_curr='KRW'):
    """환율 조회를 캐시하여 처리합니다 (CNY 교차 환율 지원)."""
    date_str = date_obj.strftime('%Y-%m-%d'); 
    from_curr_upper = str(from_curr).strip().upper(); 
    cache_key = (date_str, from_curr_upper)
    
    if cache_key in historical_rates_cache: 
        return historical_rates_cache[cache_key]
    
    if from_curr_upper == to_curr: 
        historical_rates_cache[cache_key] = 1.0; 
        return 1.0

    # print(f"    [Rate Cache] {date_str} {from_curr_upper} -> {to_curr} 환율 조회 (캐시 없음)...")
    
    if from_curr_upper == 'CNY':
        # CNY는 USD/KRW와 USD/CNY로 교차 계산
        rate_usd_krw = get_historical_rate_cached_yf(date_obj, 'USD', 'KRW')
        if rate_usd_krw == 1.0: 
            print(f"      [!!!] CNY 계산 실패: USD/KRW 환율 조회 실패. 1.0 사용."); 
            historical_rates_cache[cache_key] = 1.0; 
            return 1.0
            
        usd_cny_cache_key = (date_str, 'USD/CNY');
        if usd_cny_cache_key in historical_rates_cache: 
            rate_usd_cny = historical_rates_cache[usd_cny_cache_key]; 
        else: 
            rate_usd_cny = fetch_yf_rate("CNY=X", date_obj); 
            historical_rates_cache[usd_cny_cache_key] = rate_usd_cny
            
        if rate_usd_cny == 1.0 or rate_usd_cny == 0: 
            print(f"      [!!!] CNY 계산 실패: CNY=X 환율 조회 실패. 1.0 사용."); 
            historical_rates_cache[cache_key] = 1.0; 
            return 1.0
            
        final_rate = rate_usd_krw / rate_usd_cny; 
        historical_rates_cache[cache_key] = final_rate
        # print(f"      -> CNY/KRW 교차 계산 성공: {final_rate:.2f}"); 
        return final_rate

    # CNY 외의 다른 통화
    rate_ticker = f"{from_curr_upper}{to_curr}=X"; 
    rate = fetch_yf_rate(rate_ticker, date_obj); 
    historical_rates_cache[cache_key] = rate; 
    return rate

# --- [함수 정의 2] 해외 거래내역 변환 (v110 - 이벤트입금/외화매도 버그 완벽 수정) ---
def transform_international(df):
    """[v110 Update] 해외이벤트입금(Deposit) 및 외화매도(FX_Out) 부호/분류 버그 수정"""
    print("  [Transform] 해외 거래내역 변환 (Split Logic v110)...")
    
    if df.empty:
        print("  [Transform] 해외 거래내역 원본이 비어있습니다."); return pd.DataFrame()

    df = df.replace('', np.nan)
    df = df.dropna(subset=['계좌번호', '적요명', '거래일자'], how='any')
        
    df = df.dropna(subset=['정산금액(외)', '거래수량'], how='all').copy()

    df_transformed = pd.DataFrame(index=df.index); 
    df_transformed['account'] = df['계좌번호'].astype(str)
    df_transformed['date'] = pd.to_datetime(df['거래일자'], errors='coerce') 
    df_transformed['currency'] = df['통화'].astype(str).str.strip()
    
    original_rows = len(df_transformed); 
    df_transformed = df_transformed.dropna(subset=['date'])
    if len(df_transformed) < original_rows: 
        print(f"    [경고!] 날짜 변환 실패로 {original_rows - len(df_transformed)}개 행이 제외되었습니다.")
    
    settlement_foreign_raw_str = df.loc[df_transformed.index, '정산금액(외)'].astype(str).str.replace(',', '', regex=False)
    settlement_foreign_numeric = pd.to_numeric(settlement_foreign_raw_str, errors='coerce')
    
    quantity_raw_str_intl = df.loc[df_transformed.index, '거래수량'].astype(str).str.replace(',', '', regex=False)
    quantity_numeric_intl = pd.to_numeric(quantity_raw_str_intl, errors='coerce')

    memo_stripped = df.loc[df_transformed.index, '적요명'].astype(str).str.strip(); 
    ticker = df.loc[df_transformed.index, '종목코드'].astype(str).str.strip(); 
    name = df.loc[df_transformed.index, '종목명'].astype(str).str.strip()
    
    # [v110 수정] 해외 거래 분류 로직
    conditions = [
        # 1. Income (Specific)
        memo_stripped == '해외배당',
        memo_stripped == '배당금(외화)입금',
        memo_stripped.str.contains('이자', na=False),
        # 2. Trade (Buy/Sell)
        memo_stripped == '매수',
        memo_stripped == '매도',
        # 3. Split 
        memo_stripped == '액면분할병합출고',  
        memo_stripped == '액면분할병합입고',  
        # 4. Stock In/Out (General)
        memo_stripped == '이벤트입고',
        memo_stripped == '회사분할입고',
        memo_stripped == '청산출고',
        # 5. FX (외화매수/매도 명확히 분리)
        memo_stripped.str.contains('환전|외화매수', na=False), # USD 증가 (+)
        memo_stripped == '외화매도',                         # USD 감소 (-) [수정됨]
        # 6. Transfer (현금)
        memo_stripped == '해외이벤트입금',                   # 현금 입금 (+) [수정됨]
        memo_stripped.str.contains('입금', na=False), 
        memo_stripped.str.contains('출금', na=False)
    ]
    choices_type =    ['Income',   'Income',   'Income',   'Trade', 'Trade', 'Trade',     'Trade',    'Trade',    'Trade',    'Trade',    'FX',    'FX',     'Transfer', 'Transfer', 'Transfer']
    choices_detail =  ['Dividend', 'Dividend', 'Interest', 'Buy',   'Sell',  'Split_Out', 'Split_In', 'Stock_In', 'Stock_In', 'Stock_Out',  'FX_In', 'FX_Out', 'Deposit',  'Deposit',  'Withdraw']
    choices_ticker =  [ticker,     ticker,     None,       ticker,  ticker,  ticker,      ticker,     ticker,     ticker,     ticker,     None,    None,     None,       None,       None]
    choices_name =    [name+" 배당", name+" 배당", '이자',     name,    name,    name,        name,       name,       name,       name,       memo_stripped, memo_stripped, memo_stripped, memo_stripped, memo_stripped]
    
    df_transformed['action_type'] = np.select(conditions, choices_type, default='Other')
    df_transformed['action_detail'] = np.select(conditions, choices_detail, default=memo_stripped)
    df_transformed['ticker'] = np.select(conditions, choices_ticker, default=None)
    df_transformed['name'] = np.select(conditions, choices_name, default=memo_stripped)

    other_mask = (df_transformed['action_type'] == 'Other')
    if other_mask.any():
        print(f"    [!!! v62 경고] 해외 적요명 '{memo_stripped[other_mask].iloc[0]}'을(를) 'Other'로 분류 (현금 0 처리)")

    needs_settlement = ['Dividend', 'Interest', 'Deposit', 'Withdraw', 'FX_In', 'FX_Out']
    mask_invalid_settlement_intl = (
        df_transformed['action_detail'].isin(needs_settlement) & 
        (settlement_foreign_numeric.isna() | (settlement_foreign_numeric == 0))
    )
    if mask_invalid_settlement_intl.any():
        print(f"    [v65] '정산금액(외)'가 비어있거나 0인 '거래/수익/환전/입출금' {mask_invalid_settlement_intl.sum()}건을 무시합니다.")
        df_transformed = df_transformed[~mask_invalid_settlement_intl]
        settlement_foreign_numeric = settlement_foreign_numeric[~mask_invalid_settlement_intl]
        quantity_numeric_intl = quantity_numeric_intl[~mask_invalid_settlement_intl]

    needs_quantity = ['Buy', 'Sell', 'Stock_In', 'Split_In', 'Split_Out']
    mask_invalid_quantity_intl = (
        df_transformed['action_detail'].isin(needs_quantity) & 
        (quantity_numeric_intl.isna() | (quantity_numeric_intl == 0))
    )
    if mask_invalid_quantity_intl.any():
        print(f"    [v65] '거래수량'이 비어있거나 0인 'Buy/Sell/Stock_In/Split' {mask_invalid_quantity_intl.sum()}건을 무시합니다.")
        df_transformed = df_transformed[~mask_invalid_quantity_intl]
        settlement_foreign_numeric = settlement_foreign_numeric[~mask_invalid_quantity_intl]
        quantity_numeric_intl = quantity_numeric_intl[~mask_invalid_quantity_intl]
        
    settlement_foreign_raw = settlement_foreign_numeric.fillna(0)
    df_transformed['quantity'] = quantity_numeric_intl.fillna(0)
    
    # [v110 수정] 부호 처리 (FX_Out이 마이너스로 빠지도록 추가)
    outflow_mask_intl = df_transformed['action_detail'].isin(['Buy', 'Withdraw', 'Split_Out', 'FX_Out'])
    inflow_mask_intl = df_transformed['action_detail'].isin(['Sell', 'FX_In', 'Dividend', 'Interest', 'Deposit', 'Split_In'])
    
    settlement_foreign_final = pd.Series(0.0, index=df_transformed.index)
    settlement_foreign_final[outflow_mask_intl] = settlement_foreign_raw[outflow_mask_intl].abs() * -1
    settlement_foreign_final[inflow_mask_intl] = settlement_foreign_raw[inflow_mask_intl].abs()
    
    df_transformed['settlement_foreign'] = settlement_foreign_final
    
    # --- 8. 과거 환율 조회 및 원화 환산 ---
    unique_date_currency_pairs = df_transformed[~df_transformed['currency'].isin(['KRW', ''])]\
                                    [['date', 'currency']].drop_duplicates().sort_values(by='currency')
    
    print(f"  [Transform] 총 {len(unique_date_currency_pairs)}개의 고유 (날짜,통화) 조합에 대한 과거 환율 조회 시작 (yfinance)...")
    
    usd_pairs = unique_date_currency_pairs[unique_date_currency_pairs['currency'].str.upper() == 'USD']
    other_pairs = unique_date_currency_pairs[unique_date_currency_pairs['currency'].str.upper() != 'USD']
    
    for _, row in usd_pairs.iterrows(): 
        get_historical_rate_cached_yf(row['date'], row['currency'], 'KRW')
    for _, row in other_pairs.iterrows(): 
        get_historical_rate_cached_yf(row['date'], row['currency'], 'KRW')
        
    print("  [Transform] 과거 환율 조회/캐싱 완료. 원화 정산금액 계산 중...")
    
    settlement_krw_list = []
    for index, row in df_transformed.iterrows():
        rate = get_historical_rate_cached_yf(row['date'], row['currency'], 'KRW')
        settlement_krw = df_transformed.loc[index, 'settlement_foreign'] * rate 
        settlement_krw_list.append(settlement_krw)
        
    df_transformed['settlement_krw'] = settlement_krw_list; 
    print("  [Transform] 원화 정산금액 계산 완료.")
    
    print(f"  [Transform] 해외 거래 {len(df_transformed)}건 처리 완료."); 
    return df_transformed[['account', 'date', 'action_type', 'action_detail', 'ticker', 'name', 'quantity', 'settlement_krw', 'currency', 'settlement_foreign']]