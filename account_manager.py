# account_manager.py

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

def calculate_net_investment(df_transformed):
    """
    [v116 Final] 완성된 데이터(Transformed Data) 기반 원금 계산
    - 기존의 복잡한 Raw Data 처리 로직(환율, 정규식 등)을 모두 제거했습니다.
    - data_transformer가 이미 완벽하게 계산해둔 'settlement_krw'를 신뢰합니다.
    - 오직 'Transfer'(이체/입출금) 항목만 골라서 더합니다.
    """
    print("  [Account Manager] 순투입원금(NIC) 계산 (Transformed Data 기반 v116)...")
    
    # 1. 데이터 유효성 검사 (이미 가공된 데이터인지 확인)
    required_cols = ['date', 'account', 'action_type', 'settlement_krw']
    if not all(col in df_transformed.columns for col in required_cols):
        print(f"  [!] 필수 컬럼 누락. Transformed Data가 아닙니다. 보유 컬럼: {df_transformed.columns.tolist()}")
        return pd.DataFrame(columns=['date', 'account', 'net_invested_capital'])

    # 2. 'Transfer' (입출금) 필터링
    # 이미 입금(+), 출금(-) 부호가 적용되어 있으므로 그냥 가져옵니다.
    mask_transfer = df_transformed['action_type'] == 'Transfer'
    df_nic = df_transformed[mask_transfer].copy()
    
    if df_nic.empty:
        print("  [Account Manager] 'Transfer' 내역이 없습니다. (원금 0원)")
        # 데이터가 없어도 빈 그래프를 그리기 위해 아래 로직 진행
    
    # 3. 계좌별/일별 집계 (Sum)
    daily_flow = df_nic.groupby(['date', 'account'])['settlement_krw'].sum().reset_index()

    # 4. 시계열 채우기 (Backfill) - 그래프 끊김 방지
    # 전체 거래 내역에 존재하는 모든 계좌를 대상으로 함
    accounts = df_transformed['account'].unique()
    full_range_data = []
    today = pd.Timestamp.today().normalize()
    
    # 전체 데이터의 시작일 (계좌별 시작일보다 전체 시작일이 더 안전할 수 있음)
    global_start_date = df_transformed['date'].min() if not df_transformed.empty else today

    for acc in accounts:
        # 해당 계좌의 입출금 내역
        acc_data = daily_flow[daily_flow['account'] == acc].sort_values('date')
        
        # 입출금 내역이 하나도 없는 계좌라도, 거래 내역이 있다면 0원으로라도 표시해야 함
        if acc_data.empty:
            start_date = global_start_date
        else:
            start_date = acc_data['date'].min()
            
        # 날짜 뼈대 만들기
        date_range = pd.date_range(start=start_date, end=today)
        temp_df = pd.DataFrame({'date': date_range, 'account': acc})
        
        # 데이터 병합 (입출금 없는 날은 0)
        temp_df = temp_df.merge(acc_data, on=['date', 'account'], how='left').fillna(0)
        
        # [최종] 누적 합계 (CumSum) -> 이것이 바로 원금!
        temp_df['net_invested_capital'] = temp_df['settlement_krw'].cumsum()
        
        full_range_data.append(temp_df)

    if not full_range_data:
        return pd.DataFrame(columns=['date', 'account', 'net_invested_capital'])

    final_df = pd.concat(full_range_data)
    
    # [검증] 터미널 출력
    print("  [Account Manager] 계산 완료. 계좌별 최신 원금:")
    latest_check = final_df.groupby('account').tail(1)
    for _, row in latest_check.iterrows():
        print(f"    - 계좌 {row['account']}: {row['net_invested_capital']:,.0f} 원")

    return final_df[['date', 'account', 'net_invested_capital']]