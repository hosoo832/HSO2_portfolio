# --- 1. 설정: 인증 및 연결 ---

import os

# [핵심 경로 설정]
# 이 프로젝트 폴더의 기본 경로를 설정합니다.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 서비스 계정 JSON 파일의 전체 경로
CREDS_FILE_PATH = os.path.join(BASE_DIR, 'service_account.json')

# [구글 API 설정]
# 구글 API에 연결할 때 필요한 권한 범위
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# [구글 시트 이름 설정]
# 연결할 구글 스프레드시트 '파일'의 정확한 이름
GOOGLE_SHEET_NAME = "거래내역"

# [시트 이름 설정 (Input)]
# 1. 원본 데이터를 읽어올 시트 4개의 이름
SHEET_RAW_DOMESTIC = 'raw_domestic'
SHEET_RAW_INTL = 'raw_international'
SHEET_MASTER_DATA = 'master_data'
SHEET_REBALANCING_MASTER = 'rebalancing_master'

# [시트 이름 설정 (Output)]
# 2. 스크립트가 계산 결과를 '업로드'할 시트 3개의 이름
OUTPUT_SHEET_NAME = "dashboard_data"
REBALANCING_SHEET_NAME = "rebalancing_data"
SHEET_MARKET_DATA = "market_data" # [v70] 매매 일지용 시장 데이터 시트 (이것이 추가됨)

print("[Config] 설정 파일을 불러왔습니다.")