# --- 2. 구글 API 전담: 인증, 읽기, 쓰기 ---

import gspread
import pandas as pd
import numpy as np
import time

# [핵심] 1번 파일에서 설정값 가져오기
import config 

# --- 1. 구글 시트 인증 및 파일 열기 ---
# 이 코드는 파일이 'import'될 때 한 번만 실행됩니다.
try:
    print("\n[Google API] gspread 인증 시도...")
    gc = gspread.service_account(
        filename=config.CREDS_FILE_PATH,
        scopes=config.SCOPES
    )
    print("[Google API] 인증 성공!")
    
    # (중요) '거래내역' 파일 열기
    sheet_file = gc.open(config.GOOGLE_SHEET_NAME)
    print(f"[Google API] '{config.GOOGLE_SHEET_NAME}' 파일을 열었습니다.")

except Exception as e:
    print(f"[!!!] 구글 인증 또는 파일 열기 실패: {e}")
    print("service_account.json 파일 경로/공유/API 활성화를 다시 확인해주세요.")
    gc = None
    sheet_file = None
    exit() # [v70.16] 강제 종료 (exit)는 매우 위험하므로 주석 처리

# --- 2. [함수] 시트 데이터 '텍스트'로 읽어오기 ---
# (원본 v47/v51 로직과 동일)
def get_all_records_as_text(sheet_name):
    """
    지정된 이름의 시트를 열고, 헤더를 자동으로 찾아 텍스트(str)로 읽어옵니다.
    """
    try:
        worksheet = sheet_file.worksheet(sheet_name)
        print(f"  [Reader] '{sheet_name}' 시트를 '텍스트' 형식으로 강제 읽기 중...")
    except gspread.exceptions.WorksheetNotFound:
        print(f"  [!!!] '{sheet_name}' 시트를 찾을 수 없습니다. 빈 DataFrame을 반환합니다.")
        return pd.DataFrame(), None # v67: worksheet 객체도 반환
        
    all_values = worksheet.get_all_values()
    if not all_values: 
        print(f"  [Reader] '{sheet_name}' 시트가 비어있습니다.")
        return pd.DataFrame(), worksheet # v67: 비어있어도 worksheet 객체 반환

    # [v47/v51] 헤더 행 자동 감지 로직
    header_row_index = -1
    for i, row in enumerate(all_values):
        if ('ticker' in row and 'name' in row) or \
           ('ticker' in row and 'target_ratio' in row and 'account' in row) or \
           ('거래일자' in row and '종목명' in row):
            header_row_index = i
            break
            
    if header_row_index == -1:
        if all_values:
            header = all_values[0]; data = all_values[1:]
            print(f"  [!] '{sheet_name}'에서 표준 헤더를 못찾음. 1행을 헤더로 강제 사용.")
        else:
            return pd.DataFrame(), worksheet # 빈 시트
    else:
        # 헤더를 찾은 경우
        header = all_values[header_row_index]
        data = all_values[header_row_index + 1:]
        print(f"  [Reader] '{sheet_name}'의 {header_row_index + 1}행에서 헤더 발견.")

    df = pd.DataFrame(data, columns=header, dtype=str)
    
    # [v47] 헤더 행 이전에 제목 행 등이 있을 수 있으므로, 실제 데이터가 시작되는 빈 행을 제거
    df = df.replace('', np.nan).dropna(how='all')
    
    # 'get_all_records_as_text'는 원본 시트 인스턴스도 함께 반환 (v35 auto_fill_exchange_info 용도)
    return df, worksheet

# --- 3. [함수] 구글 시트에 업로드 (서식 지정) ---
# (원본 v47 로직과 동일)
def upload_to_google_sheet(df_to_upload, sheet_name):
    """
    DataFrame을 지정된 시트에 업로드하고, 열 이름에 따라 서식을 자동 적용합니다.
    """
    print(f"\n[Uploader] 최종 결과를 '{sheet_name}' 시트에 업로드 시작...")
    try:
        workbook = sheet_file # 이미 열려있는 파일 사용
        worksheet = None
        try:
            worksheet = workbook.worksheet(sheet_name)
            print(f"  [Uploader] 기존 '{sheet_name}' 시트를 찾았습니다. 내용을 지웁니다."); 
            worksheet.clear(); 
            time.sleep(1) # API 제한 방지를 위한 대기
        except gspread.exceptions.WorksheetNotFound:
            print(f"  [Uploader] '{sheet_name}' 시트 없음. 새로 생성합니다."); 
            worksheet = workbook.add_worksheet(title=sheet_name, rows="1", cols="1"); 
            time.sleep(1)
        
        df_export = df_to_upload.copy()
        
        # 날짜/시간 포맷팅
        for col in df_export.select_dtypes(include=['datetime64[ns]']).columns: 
            df_export[col] = df_export[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df_export = df_export.astype(object)
        
        # [v47] 숫자형(int, float)에 대해서만 inf, nan -> 0 처리
        for col in df_export.columns:
            if pd.api.types.is_numeric_dtype(df_to_upload[col]):
                df_export[col] = df_export[col].replace([np.inf, -np.inf], np.nan)
                df_export[col] = df_export[col].fillna(0)
            else:
                # 문자열 열의 NaN은 빈 문자열 ''로
                df_export[col] = df_export[col].fillna('')
                
        data_to_upload = [df_export.columns.values.tolist()] + df_export.values.tolist()
        print(f"  [Uploader] '{sheet_name}' 시트에 데이터 쓰는 중 ({len(data_to_upload)} 행)...")
        
        worksheet.update(values=data_to_upload, range_name='A1', value_input_option='USER_ENTERED')
        print(f"  [Uploader] 데이터 업로드 완료."); 
        time.sleep(1)
        
        # [v47 신규] 자동 서식 지정 로직
        print(f"  [Uploader] '{sheet_name}' 시트 서식 지정 중...")
        formats_to_apply = []
        
        def to_a1(row, col): # 1-based index
            return gspread.utils.rowcol_to_a1(row, col)

        num_rows = len(data_to_upload)
        if num_rows <= 1:
            print("  [Uploader] 데이터가 1행 이하이므로 서식 지정을 건너뜁니다.")
            return True # 업로드 자체는 성공

        col_names = df_export.columns.values.tolist()
        
        for i, col_name in enumerate(col_names):
            col_idx = i + 1 # 1-based index
            col_range = f"{to_a1(2, col_idx)}:{to_a1(num_rows, col_idx)}"
            
            if col_name == 'ticker' or col_name == 'account':
                # 텍스트
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "TEXT"}}
                })
            elif col_name.endswith('_price'):
                # 지수/가격: 천단위 콤마 + 소수점 2자리 (yfinance가 6598.87 식으로 줌)
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}
                })
            elif col_name.endswith('_krw') or col_name.endswith('_value') or col_name.endswith('_volume'):
                # 원화 / 거래대금 / 예탁금 등: 정수 (천단위 콤마)
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}
                })
            elif col_name.endswith('_pct'):
                # 등락률: raw decimal (0.0046) → "0.46%" 표시
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}
                })
            elif col_name.endswith('_rate'):
                # 채권 금리: 이미 % 단위 숫자 (4.29) → "4.29%" 표시 (×100 안 함)
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "NUMBER", "pattern": "0.00\"%\""}}
                })
            elif col_name.endswith('_bps'):
                # BPS (소수점 2자리)
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}
                })
            elif 'quantity' in col_name or col_name == 'current_price' or col_name == 'avg_cost_krw':
                 # 정수 (쉼표) - 기존 로직 유지
                formats_to_apply.append({
                    "range": col_range,
                    "format": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}
                })

        if formats_to_apply:
            print(f"  [Uploader] 총 {len(formats_to_apply)}개의 서식 규칙을 적용합니다.")
            worksheet.batch_format(formats_to_apply)
            print(f"  [Uploader] 서식 지정 완료.")
        else:
            print("  [Uploader] 적용할 서식 규칙이 없습니다.")
            
        print(f"[Uploader] '{sheet_name}' 시트 업로드 완료!"); 
        return True
        
    except Exception as e: 
        print(f"  [!!!] 구글 시트 업로드 중 오류 발생: {e}"); 
        return False

# --- 4. [신규 함수 v69] 구글 시트에 '한 줄' 추가하기 ---
def append_row_to_sheet(row_data_list, sheet_name):
    """
    DataFrame이 아닌 list 데이터를 시트 맨 아래에 '한 줄 추가(append)'합니다.
    (market_data, journal_log 등 로그 기록용)
    """
    print(f"\n[Appender] '{sheet_name}' 시트에 1개 행 추가 시도...")
    try:
        workbook = sheet_file # 이미 열려있는 파일 사용
        worksheet = None
        try:
            worksheet = workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  [!!!] '{sheet_name}' 시트 없음. 새로 생성합니다."); 
            worksheet = workbook.add_worksheet(title=sheet_name, rows="1", cols="1"); 
            time.sleep(1)
            
            # [v69] 새로 만들 경우, 헤더(Header)를 먼저 입력
            # [v70] 41개 전체 헤더로 확장
            header = [
                "date",
                "KOSPI_price", "KOSPI_chg_pct", "KOSPI_volume",
                "KOSDAQ_price", "KOSDAQ_chg_pct", "KOSDAQ_volume",
                "SP500_price", "SP500_chg_pct", "SP500_volume",
                "NASDAQ_price", "NASDAQ_chg_pct", "NASDAQ_volume",
                "SHANGHAI_price", "SHANGHAI_chg_pct", "SHANGHAI_volume",
                "DAX_price", "DAX_chg_pct", "DAX_volume",
                "USDKRW_price", "USDKRW_chg_pct",
                "USD_IDX_price", "USD_IDX_chg_pct",
                "US_10Y_Bond_rate", "US_10Y_Bond_chg_bps",
                "US_30Y_Bond_rate", "US_30Y_Bond_chg_bps",
                "WTI_price", "WTI_chg_pct",
                "GOLD_price", "GOLD_chg_pct",
                "BTC_price", "BTC_chg_pct",
                "VIX_price", "VIX_chg_pct",
                "KR_10Y_Bond_rate", "KR_10Y_Bond_chg_bps",
                "Customer_Deposit_value", "Customer_Deposit_chg_pct",
                "Credit_Balance_value", "Credit_Balance_chg_pct"
            ]
            worksheet.append_row(header, value_input_option='USER_ENTERED')
            print(f"  [Appender] 새 시트 생성 및 헤더 입력 완료.")
            time.sleep(1)
        
        # [v69] 데이터 한 줄 추가 (list 형식)
        worksheet.append_row(row_data_list, value_input_option='USER_ENTERED')
        print(f"  [Appender] '{sheet_name}' 시트에 1개 행({len(row_data_list)}개 열) 추가 완료.")
        return True

    except Exception as e:
        print(f"  [!!!] 구글 시트 행 추가 중 오류 발생: {e}")
        return False