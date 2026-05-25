# migrate_chey.py
# [1회성 마이그레이션] raw_domestic 의 '보통매매' 매수/매도 → 새 'raw_체결' 시트로 이관
#
#   사용법:
#     python migrate_chey.py            ← 드라이런 (읽기만, 미리보기. 아무것도 안 씀)
#     python migrate_chey.py --commit   ← 실제로 raw_체결 시트 생성 + 기록
#
#   핵심 — 분할체결 통합:
#     같은 날 같은 종목을 여러 번 매매하면, 증권사 거래내역은 정산금액을
#     마지막 행에만 몰아주거나(중간 행은 빈칸) 행마다 따로 준다.
#     → (계좌·날짜·종목·매수매도) 단위로 합산해 1행으로 통합한다.
#     → 수량 합 / 정산금액 합 둘 다 정확히 보존됨 (어느 패턴이든 동일).
#
#   안전 보장:
#     - raw_domestic 은 읽기만 한다. 한 글자도 수정/삭제 안 함.
#     - 종목코드 앞자리 0 보존을 위해 value_input_option='RAW' 사용.

import sys
import time
import numpy as np
import pandas as pd
import gspread

import config
import google_api

CHEY_SHEET_NAME = 'raw_체결'

# raw_체결 시트 스키마 (1줄 = 같은날·같은종목·같은방향 매매 통합 1건)
#   정산금액: 과거분은 거래내역의 정확한 값(통합 합계)을 그대로 보존.
#            앞으로 키움/미래에셋에서 새로 넣는 행은 비워두면 main.py 가 계산함.
OUT_COLS = ['계좌번호', '체결일', '종목코드', '종목명', '매매구분', '체결수량', '체결평균단가', '정산금액']


def is_market_trade(거래종류):
    """진짜 시장매매(보통매매 매수/매도)만 True. OTC(장외)는 제외 — 거래내역에 남김.
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


def to_num(series):
    """콤마 제거 후 숫자로. 빈칸/'nan'/실패 → NaN"""
    cleaned = (series.astype(str).str.replace(',', '', regex=False).str.strip()
               .replace({'': None, 'nan': None, 'NaN': None, 'None': None}))
    return pd.to_numeric(cleaned, errors='coerce')


def num_str(v, blank_if_zero=False):
    """숫자 → 시트 기록용 문자열. NaN→''. 정수면 정수로, 아니면 소수."""
    if pd.isna(v):
        return ''
    if blank_if_zero and v == 0:
        return ''
    f = float(v)
    return str(int(f)) if f.is_integer() else str(round(f, 2))


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
        print(f"\n[!!!] raw_domestic 에 필요한 헤더가 없음: {missing}  → 중단."); return

    price_col = next((c for c in df.columns if '거래단가' in str(c)), None)
    settle_col = next((c for c in df.columns
                       if '정산금액' in str(c) and '외' not in str(c)), None)
    print(f"[읽기] 단가 컬럼: {price_col or '(없음)'}  /  정산금액 컬럼: {settle_col or '(없음)'}")
    if not price_col or not settle_col:
        print("[!!!] 단가 또는 정산금액 컬럼을 못 찾음. 중단."); return

    # --- 2. '보통매매' 매수/매도(장외 제외)만 추출 ---
    trades = df[df['거래종류'].apply(is_market_trade)].copy()
    otc = df[df['거래종류'].astype(str).str.contains('보통매매', na=False)
             & df['거래종류'].astype(str).str.contains('OTC', na=False)]
    print(f"\n[추출] '보통매매' 매수/매도(OTC 제외): {len(trades)} 행 (원본 행 단위)")
    if len(otc):
        print(f"[참고] OTC 매매 {len(otc)}건 제외 — 거래내역에 그대로 남김")
    if trades.empty:
        print("[!!!] 추출된 매매가 0건. 중단."); return
    print(f"[추출] 잡힌 거래종류: {sorted(trades['거래종류'].astype(str).unique())}")

    # --- 3. 행 단위 정리 ---
    rec = pd.DataFrame()
    rec['계좌번호'] = trades['계좌번호'].astype(str).str.strip()
    rec['체결일'] = trades['거래일자'].astype(str).str.strip()
    rec['종목코드'] = trades['종목코드'].astype(str).str.strip()
    rec['종목명'] = trades['종목명'].astype(str).str.strip()
    rec['매매구분'] = trades['거래종류'].apply(trade_side)
    rec['수량'] = to_num(trades['거래수량'])
    rec['단가'] = to_num(trades[price_col])
    rec['정산금액'] = to_num(trades[settle_col])
    rec['거래금액'] = rec['수량'] * rec['단가']      # 가중평균단가 계산용

    bad_side = rec['매매구분'].isna().sum()
    if bad_side:
        print(f"[!주의] 매수/매도 판별 실패 {bad_side}건 — 거래종류 확인 필요")

    # --- 3b. 분할체결 통합 (계좌·날짜·종목·매수매도 단위 합산) ---
    grp_keys = ['계좌번호', '체결일', '종목코드', '매매구분']
    g = rec.groupby(grp_keys, dropna=False, sort=False)
    con = g.agg(
        종목명=('종목명', 'first'),
        체결수량=('수량', 'sum'),         # 수량 합
        거래금액합=('거래금액', 'sum'),
        정산금액=('정산금액', 'sum'),       # 빈칸(NaN)은 0 취급 → 합산하면 총액 보존
        원본행수=('수량', 'size'),
    ).reset_index()

    # 가중평균 체결단가 ( = Σ(수량×단가) / Σ수량 )
    con['체결평균단가'] = (con['거래금액합'] / con['체결수량']).replace([np.inf, -np.inf], np.nan).round(2)

    multi = int((con['원본행수'] > 1).sum())
    print(f"\n[통합] 원본 {len(rec)}행 → 통합 {len(con)}행  (여러 번 체결을 합친 그룹 {multi}개)")

    # --- 4. 통합 결과 품질 점검 ---
    dts = pd.to_datetime(con['체결일'], errors='coerce')
    empty_settle = int((con['정산금액'].isna() | (con['정산금액'] == 0)).sum())
    empty_price = int((con['체결평균단가'].isna() | (con['체결평균단가'] == 0)).sum())
    zero_qty = int((con['체결수량'].isna() | (con['체결수량'] == 0)).sum())

    # --- 5. 최종 출력 테이블 ---
    out = pd.DataFrame({
        '계좌번호': con['계좌번호'].astype(str),
        '체결일': con['체결일'].astype(str),
        '종목코드': con['종목코드'].astype(str),
        '종목명': con['종목명'].astype(str),
        '매매구분': con['매매구분'].astype(str).replace({'nan': '', 'None': ''}),
        '체결수량': con['체결수량'].apply(num_str),
        '체결평균단가': con['체결평균단가'].apply(num_str),
        '정산금액': con['정산금액'].apply(lambda v: num_str(v, blank_if_zero=True)),
    })[OUT_COLS]

    dmin, dmax = dts.min(), dts.max()
    dmin_s = dmin.strftime('%Y-%m-%d') if pd.notna(dmin) else '?'
    dmax_s = dmax.strftime('%Y-%m-%d') if pd.notna(dmax) else '?'
    print(f"\n{'-'*64}\n  통합 결과 요약\n{'-'*64}")
    print(f"  통합 매매 행      : {len(out)}")
    print(f"  매수 / 매도       : {(out['매매구분']=='매수').sum()} / {(out['매매구분']=='매도').sum()}")
    print(f"  날짜 범위         : {dmin_s}  ~  {dmax_s}   (날짜변환 실패 {int(dts.isna().sum())}건)")
    print(f"  체결수량 0/빈칸    : {zero_qty}건")
    print(f"  체결평균단가 빈칸  : {empty_price}건")
    print(f"  정산금액 빈칸      : {empty_settle}건  (← 통합 후에도 비면 main.py 가 계산)")
    print(f"  계좌별 건수       :")
    for acc, n in out['계좌번호'].value_counts().items():
        print(f"      {acc:>15} : {n}")

    # 분할체결이 실제로 합쳐졌는지 — 원본행수 최다 그룹 보여주기
    top = con.sort_values('원본행수', ascending=False).head(1)
    if len(top) and int(top['원본행수'].iloc[0]) > 1:
        r = top.iloc[0]
        print(f"\n  [분할체결 통합 예시 — 가장 많이 합쳐진 그룹]")
        print(f"      {r['계좌번호']} / {r['체결일']} / {r['종목명']} / {r['매매구분']}")
        print(f"      원본 {int(r['원본행수'])}행  →  통합 1행 : "
              f"체결수량 {num_str(r['체결수량'])}, 정산금액 {num_str(r['정산금액'], True) or '(빈칸)'}")

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
