# migrate_chey.py
# [1회성 마이그레이션] raw_domestic 의 '보통매매' 매수/매도 → 새 'raw_체결' 시트로 이관
#
#   사용법:
#     python migrate_chey.py            ← 드라이런 (읽기만, 무엇을 쓸지 미리보기. 아무것도 안 씀)
#     python migrate_chey.py --commit   ← 실제로 raw_체결 시트 생성 + 기록
#
#   안전 보장:
#     - raw_domestic 은 읽기만 한다. 한 글자도 수정/삭제 안 함.
#     - raw_체결 이 이미 있으면 내용을 비우고 새로 쓴다 (1회성이므로).
#     - 종목코드의 앞자리 0 보존을 위해 value_input_option='RAW' 사용.

import sys
import time
import pandas as pd
import gspread

import config
import google_api

CHEY_SHEET_NAME = 'raw_체결'

# raw_체결 시트 스키마 (1줄 = 1체결)
#   정산금액: 과거분은 거래내역의 정확한 값을 그대로 보존(검증 대조용).
#            앞으로 키움/미래에셋에서 새로 넣는 행은 비워두면 main.py 가 계산함.
OUT_COLS = ['계좌번호', '체결일', '종목코드', '종목명', '매매구분', '체결수량', '체결평균단가', '정산금액']


def is_market_trade(거래종류):
    """진짜 시장매매(보통매매 매수/매도)만 True. OTC(장외)는 제외 — 거래내역에 그대로 남김.
    재투자/무상주입고/액면분할 등은 '보통매매'가 아니므로 자동 제외됨."""
    t = str(거래종류)
    return ('보통매매' in t) and ('OTC' not in t)


def trade_side(거래종류):
    t = str(거래종류)
    if '매도' in t:
        return '매도'
    if '매수' in t:
        return '매수'
    return None


def main():
    commit = '--commit' in sys.argv
    mode = 'COMMIT (실제 기록)' if commit else 'DRY-RUN (미리보기 — 아무것도 안 씀)'
    print(f"\n{'='*64}")
    print(f"  raw_체결 마이그레이션   |   모드: {mode}")
    print(f"{'='*64}")

    # --- 1. raw_domestic 읽기 (읽기 전용) ---
    df, _ = google_api.get_all_records_as_text(config.SHEET_RAW_DOMESTIC)
    if df.empty:
        print("[!!!] raw_domestic 이 비어있음. 중단."); return
    print(f"\n[읽기] raw_domestic 총 {len(df)} 행")
    print(f"[읽기] 감지된 헤더: {list(df.columns)}")

    needed = ['계좌번호', '거래일자', '거래종류', '종목코드', '종목명', '거래수량']
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"\n[!!!] raw_domestic 에 필요한 헤더가 없음: {missing}")
        print("      위 '감지된 헤더'와 비교해서 헤더명을 확인해줘. 중단.")
        return

    # 단가/정산금액 컬럼 이름 유연 탐색 (예: '거래단가', '거래단가/환율')
    price_col = next((c for c in df.columns if '거래단가' in str(c)), None)
    settle_col = next((c for c in df.columns
                       if '정산금액' in str(c) and '외' not in str(c)), None)
    print(f"[읽기] 단가 컬럼: {price_col or '(없음)'}  /  정산금액 컬럼: {settle_col or '(없음)'}")

    # --- 2. '보통매매' 매수/매도(장외 제외)만 추출 ---
    mask = df['거래종류'].apply(is_market_trade)
    trades = df[mask].copy()

    otc = df[df['거래종류'].astype(str).str.contains('보통매매', na=False)
             & df['거래종류'].astype(str).str.contains('OTC', na=False)]

    print(f"\n[추출] '보통매매' 매수/매도(OTC 제외): {len(trades)} 행")
    if len(otc):
        print(f"[참고] OTC 매매 {len(otc)}건은 제외 — 거래내역에 그대로 남김")
    if trades.empty:
        print("[!!!] 추출된 매매가 0건. 헤더/데이터 확인 필요. 중단."); return
    print(f"[추출] 잡힌 거래종류 종류: {sorted(trades['거래종류'].astype(str).unique())}")

    # --- 3. raw_체결 스키마로 변환 ---
    out = pd.DataFrame()
    out['계좌번호'] = trades['계좌번호'].astype(str).str.strip()
    out['체결일'] = trades['거래일자'].astype(str).str.strip()
    out['종목코드'] = trades['종목코드'].astype(str).str.strip()
    out['종목명'] = trades['종목명'].astype(str).str.strip()
    out['매매구분'] = trades['거래종류'].apply(trade_side)
    out['체결수량'] = trades['거래수량'].astype(str).str.replace(',', '', regex=False).str.strip()
    out['체결평균단가'] = (trades[price_col].astype(str).str.replace(',', '', regex=False).str.strip()
                       if price_col else '')
    out['정산금액'] = (trades[settle_col].astype(str).str.replace(',', '', regex=False).str.strip()
                   if settle_col else '')
    out = out[OUT_COLS]

    # --- 4. 데이터 품질 점검 ---
    bad_side = out['매매구분'].isna().sum()
    empty_price = (out['체결평균단가'].isin(['', 'nan', 'None'])).sum()
    empty_settle = (out['정산금액'].isin(['', 'nan', 'None'])).sum()
    if bad_side:
        idx = out[out['매매구분'].isna()].index
        print(f"\n[!주의] 매수/매도 판별 실패 {bad_side}건 — 거래종류: "
              f"{sorted(trades.loc[idx, '거래종류'].astype(str).unique())}")

    # --- 5. 요약 ---
    dts = pd.to_datetime(out['체결일'], errors='coerce')
    dmin, dmax = dts.min(), dts.max()
    dmin_s = dmin.strftime('%Y-%m-%d') if pd.notna(dmin) else '?'
    dmax_s = dmax.strftime('%Y-%m-%d') if pd.notna(dmax) else '?'
    print(f"\n{'-'*64}\n  추출 요약\n{'-'*64}")
    print(f"  총 매매 행       : {len(out)}")
    print(f"  매수 / 매도      : {(out['매매구분']=='매수').sum()} / {(out['매매구분']=='매도').sum()}")
    print(f"  날짜 범위        : {dmin_s}  ~  {dmax_s}"
          f"   (날짜변환 실패 {dts.isna().sum()}건)")
    print(f"  체결평균단가 빈칸 : {empty_price}건")
    print(f"  정산금액 빈칸    : {empty_settle}건")
    print(f"  계좌별 건수      :")
    for acc, n in out['계좌번호'].value_counts().items():
        print(f"      {acc:>15} : {n}")
    print(f"\n  [샘플 — 가장 오래된 3건]")
    print('   ' + out.assign(_d=dts).sort_values('_d').drop(columns='_d')
          .head(3).to_string(index=False).replace('\n', '\n   '))
    print(f"\n  [샘플 — 가장 최근 3건]")
    print('   ' + out.assign(_d=dts).sort_values('_d').drop(columns='_d')
          .tail(3).to_string(index=False).replace('\n', '\n   '))

    # --- 6. 커밋 ---
    if not commit:
        print(f"\n{'='*64}")
        print("  DRY-RUN 종료 — 아무것도 안 썼음. raw_domestic 도 그대로.")
        print("  위 요약이 정상으로 보이면, 실제 생성:")
        print("      python migrate_chey.py --commit")
        print(f"{'='*64}\n")
        return

    print(f"\n[커밋] '{CHEY_SHEET_NAME}' 시트 생성/덮어쓰기 ...")
    sf = google_api.sheet_file
    try:
        ws = sf.worksheet(CHEY_SHEET_NAME)
        print(f"  기존 '{CHEY_SHEET_NAME}' 시트 발견 → 내용 초기화")
        ws.clear(); time.sleep(1)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  '{CHEY_SHEET_NAME}' 시트 신규 생성")
        ws = sf.add_worksheet(title=CHEY_SHEET_NAME,
                              rows=str(len(out) + 200), cols=str(len(OUT_COLS) + 2))
        time.sleep(1)

    values = [OUT_COLS] + out.fillna('').astype(str).values.tolist()
    ws.update(values=values, range_name='A1', value_input_option='RAW')
    print(f"  OK — '{CHEY_SHEET_NAME}' 에 헤더 + {len(out)}행 기록 완료 (총 {len(values)}행)")
    print(f"\n  raw_domestic 은 읽기만 했음 — 한 글자도 안 건드림.")
    print(f"{'='*64}\n")


if __name__ == '__main__':
    main()
