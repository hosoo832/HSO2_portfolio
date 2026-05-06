# report_cash_flow.py

import pandas as pd
import config
import google_api
import data_transformer
import warnings

warnings.filterwarnings("ignore")

def run_cash_flow_report():
    print("\n=======================================================")
    print("   [Cash Flow Report v2] 자금 흐름 및 누적 원금 추적기")
    print("=======================================================\n")

    # 1. 데이터 로딩 및 변환
    print(" [1] 데이터 로딩 및 변환 중...")
    df_domestic, _ = google_api.get_all_records_as_text(config.SHEET_RAW_DOMESTIC)
    df_intl, _ = google_api.get_all_records_as_text(config.SHEET_RAW_INTL)
    
    dom_trans = data_transformer.transform_domestic(df_domestic)
    intl_trans = data_transformer.transform_international(df_intl)
    
    df_all = pd.concat([dom_trans, intl_trans], ignore_index=True)
    df_all['date'] = pd.to_datetime(df_all['date'])
    
    # 2. 'Transfer' 필터링
    mask_transfer = df_all['action_type'] == 'Transfer'
    df_cash = df_all[mask_transfer].copy()
    
    if df_cash.empty:
        print(" [!] 입출금 내역이 없습니다.")
        return

    # 3. 누적 잔고(Running Balance) 계산을 위한 정렬
    # (계좌별로 묶고, 날짜를 과거->미래 순으로 정렬해야 누적합 계산 가능)
    df_cash = df_cash.sort_values(by=['account', 'date'], ascending=[True, True])

    # [핵심] 계좌별 누적 합계 계산 (이게 바로 그 시점의 순투입원금!)
    df_cash['running_balance'] = df_cash.groupby('account')['settlement_krw'].cumsum()

    # 4. 보고서용 재정렬 (최신 날짜가 위로 오게)
    df_cash = df_cash.sort_values(by=['date', 'account'], ascending=[False, True])

    # 5. 데이터프레임 가공 (상세 내역)
    report_df = df_cash[[
        'date', 'account', 'action_detail', 'settlement_krw', 'running_balance', 'name'
    ]].copy()
    
    report_df.columns = ['날짜', '계좌번호', '구분', '금액(KRW)', '누적 순투입원금(NIC)', '비고']
    
    report_df['날짜'] = report_df['날짜'].dt.strftime('%Y-%m-%d')
    report_df['구분'] = report_df['구분'].replace({'Deposit': '입금 (+)', 'Withdraw': '출금 (-)'})

    # 6. [New] 월별 자금 동향 요약표 만들기 (Pivot)
    print(" [2] 월별 자금 동향 분석 중...")
    df_cash['Month'] = df_cash['date'].dt.strftime('%Y-%m')
    
    # 월별/계좌별 입출금 합계
    monthly_pivot = df_cash.groupby(['Month', 'account'])['settlement_krw'].sum().unstack(fill_value=0)
    
    # 전체 합계 컬럼 추가
    monthly_pivot['Total Net Flow'] = monthly_pivot.sum(axis=1)
    
    # 최신 월이 위로 오게 정렬
    monthly_pivot = monthly_pivot.sort_index(ascending=False)
    monthly_pivot.reset_index(inplace=True)

    # 7. 구글 시트 업로드
    print("\n [3] 구글 시트 'cash_flow_log' 탭에 저장 중...")
    
    try:
        _, sheet_inst = google_api.get_all_records_as_text(config.SHEET_MASTER_DATA)
        workbook = sheet_inst.spreadsheet
        
        try:
            log_sheet = workbook.worksheet("cash_flow_log")
            log_sheet.clear()
        except:
            log_sheet = workbook.add_worksheet(title="cash_flow_log", rows=2000, cols=20)
        
        # (A) 상세 내역 업로드 (A1 셀부터)
        log_sheet.update(range_name='A1', values=[["[1] 상세 자금 흐름 (최신순)"]])
        log_sheet.update(range_name='A2', values=[report_df.columns.values.tolist()] + report_df.values.tolist())
        
        # (B) 월별 요약표 업로드 (H1 셀부터 - 옆에 배치)
        start_col_char = "H" # H열부터 시작
        log_sheet.update(range_name=f'{start_col_char}1', values=[["[2] 월별 순유입 현황 (단위: 원)"]])
        
        # 헤더와 데이터 준비
        pivot_headers = monthly_pivot.columns.values.tolist()
        pivot_values = monthly_pivot.values.tolist()
        
        # JSON 직렬화 문제 방지 (int64 -> int)
        import json
        pivot_values_clean = json.loads(json.dumps(pivot_values, default=str))
        
        log_sheet.update(range_name=f'{start_col_char}2', values=[pivot_headers] + pivot_values_clean)
        
        print(" [Success] 저장 완료!")
        print(f"  - A열~F열: 상세 입출금 내역 (누적 원금 포함)")
        print(f"  - H열~  : 월별/계좌별 자금 흐름 요약")
        
    except Exception as e:
        print(f" [!] 업로드 실패: {e}")

if __name__ == "__main__":
    run_cash_flow_report()