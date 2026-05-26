# fix_chey_codes.py
# [1회성 복구] raw_체결 시트에서 종목코드 leading 0 누락 자동 수정
#
#   사용법:
#     python fix_chey_codes.py            ← 드라이런 (수정할 셀 미리보기)
#     python fix_chey_codes.py --commit   ← 실제 수정
#
#   Google Sheets paste 시 키움 종목번호의 leading 0 이 떨어진 경우 (예: 086280 → 86280)
#   raw_체결의 종목코드 열에서 숫자만이고 6자리 미만인 값을 zfill(6) 으로 복구.

import sys
import time
import gspread

import config
import google_api

SHEET = 'raw_체결'
TICKER_COL = '종목코드'


def main():
    commit = '--commit' in sys.argv
    mode = 'COMMIT (실제 수정)' if commit else 'DRY-RUN (미리보기)'
    print(f"\n{'='*60}\n  raw_체결 종목코드 leading 0 복구  |  {mode}\n{'='*60}")

    try:
        ws = google_api.sheet_file.worksheet(SHEET)
    except Exception as e:
        print(f"[!!!] '{SHEET}' 읽기 실패: {e}"); return

    all_vals = ws.get_all_values()
    if not all_vals:
        print(f"[!!!] '{SHEET}' 비어있음"); return

    header = all_vals[0]
    if TICKER_COL not in header:
        print(f"[!!!] '{TICKER_COL}' 컬럼 없음. 헤더: {header}"); return
    col_idx = header.index(TICKER_COL)        # 0-based
    col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1]

    fixes = []   # (sheet_row_num, old, new, 종목명)
    name_idx = header.index('종목명') if '종목명' in header else None
    for r_i, row in enumerate(all_vals[1:], start=2):   # 시트 행번호 = r_i (1-based, header=1)
        if col_idx >= len(row):
            continue
        v = row[col_idx].strip()
        if v.isdigit() and 0 < len(v) < 6:
            new_v = v.zfill(6)
            nm = row[name_idx] if name_idx is not None and name_idx < len(row) else ''
            fixes.append((r_i, v, new_v, nm))

    if not fixes:
        print("\n복구할 행 없음. 모든 종목코드 정상.")
        return

    print(f"\n발견된 leading-0 누락 종목: {len(fixes)}건")
    # 같은 (old → new) 그룹별로 카운트
    from collections import Counter
    cnt = Counter((old, new) for _, old, new, _ in fixes)
    for (old, new), n in sorted(cnt.items()):
        sample_names = [nm for r,o,nw,nm in fixes if o == old][:3]
        print(f"  {old} → {new}  ({n}건)   예시 종목명: {', '.join(set(sample_names))}")

    print(f"\n[샘플 — 처음 10건]")
    for r_i, old, new, nm in fixes[:10]:
        print(f"  행 {r_i:4d}: '{old}' → '{new}'   ({nm})")

    if not commit:
        print(f"\n{'='*60}")
        print("  DRY-RUN 종료. 실제 수정:")
        print("      python fix_chey_codes.py --commit")
        print(f"{'='*60}\n")
        return

    print(f"\n[커밋] {len(fixes)}개 셀 일괄 업데이트 ...")
    cells = [gspread.Cell(row=r_i, col=col_idx + 1, value=new)
             for r_i, _, new, _ in fixes]
    ws.update_cells(cells, value_input_option='RAW')
    time.sleep(0.5)
    print(f"  OK — {len(fixes)}개 셀 복구 완료.")
    print(f"\n  raw_체결 의 {TICKER_COL} 열 leading 0 복원됨.")
    print(f"  이제 python main.py 돌리면 대시보드도 정상화돼.")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
