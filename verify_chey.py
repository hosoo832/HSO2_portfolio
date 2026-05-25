# verify_chey.py
# [검증 전용] raw_체결 도입 후 결과가 기존과 동일한지 대조.
# 시트를 절대 수정하지 않는다 (읽기 + 계산 + 비교만).
#
#   python verify_chey.py
#
#   구(舊) = 현재 운영 동작 : raw_domestic 만, 보통매매 포함
#   신(新) = raw_domestic(보통매매 제외) + raw_체결
#   둘이 일치하면 main.py 적용 안전.

import pandas as pd

import config
import google_api
import data_transformer
import finance_core


def agg_compare(old, new, keys, valcol, label, tol=0.5):
    o = old.groupby(keys)[valcol].sum()
    n = new.groupby(keys)[valcol].sum()
    diff = o.subtract(n, fill_value=0)
    diff = diff[diff.abs() > tol]
    if diff.empty:
        print(f"  OK   {label} — 완전 일치")
        return True
    print(f"  FAIL {label} — {len(diff)}건 불일치 (구 − 신):")
    for k, v in diff.head(15).items():
        print(f"         {k} : {v:+,.1f}")
    return False


def main():
    print("\n" + "=" * 66)
    print("  raw_체결 검증 — 신구 결과 대조 (시트 안 건드림, 읽기·계산만)")
    print("=" * 66)

    df_domestic, _ = google_api.get_all_records_as_text(config.SHEET_RAW_DOMESTIC)
    df_chey, _ = google_api.get_all_records_as_text(config.SHEET_RAW_CHEY)
    if df_domestic.empty or df_chey.empty:
        print("[!!!] raw_domestic 또는 raw_체결 이 비어있음. 중단."); return

    # 구(舊) = 현재 운영 동작
    old = data_transformer.transform_domestic(df_domestic, exclude_market_trades=False)
    # 신(新) = raw_domestic(보통매매 제외) + raw_체결
    new_rest = data_transformer.transform_domestic(df_domestic, exclude_market_trades=True)
    new_chey = data_transformer.transform_chey(df_chey)
    new = pd.concat([new_rest, new_chey], ignore_index=True)

    for d in (old, new):
        d['date'] = pd.to_datetime(d['date'], errors='coerce')
        d['ticker'] = d['ticker'].astype(str).str.strip().replace('', 'N/A')
        d['account'] = d['account'].astype(str).str.strip()
        d['settlement_krw'] = pd.to_numeric(d['settlement_krw'], errors='coerce').fillna(0)
        d['quantity'] = pd.to_numeric(d['quantity'], errors='coerce').fillna(0)
    old = old.dropna(subset=['date'])
    new = new.dropna(subset=['date'])

    print(f"\n구 거래 {len(old)}건  /  신 거래 {len(new)}건"
          f"  (신 = 거래내역잔여 {len(new_rest)} + 체결 {len(new_chey)})")

    print("\n[1] 현금흐름 — 계좌·날짜별 settlement_krw 합 (= 예수금/현금 동일성)")
    p1 = agg_compare(old, new, ['account', 'date'], 'settlement_krw', '계좌·날짜별 현금흐름')

    print("\n[2] 보유수량 — 계좌·종목·날짜별 quantity 합 (= 보유/비중 동일성)")
    p2 = agg_compare(old, new, ['account', 'ticker', 'date'], 'quantity', '계좌·종목·날짜별 수량')

    print("\n[3] 최종 보유 현황 — calculate_holdings_and_realized_pl 통과 후 대조")
    h_old = finance_core.calculate_holdings_and_realized_pl(old)
    h_new = finance_core.calculate_holdings_and_realized_pl(new)
    p3 = True
    if len(h_old) != len(h_new):
        print(f"  FAIL 보유 항목 수 다름: 구 {len(h_old)} vs 신 {len(h_new)}")
        p3 = False
    else:
        m = h_old.merge(h_new, on=['account', 'ticker'], suffixes=('_구', '_신'))
        if len(m) != len(h_old):
            print(f"  FAIL 계좌·종목 키 불일치 (구 {len(h_old)}개 중 매칭 {len(m)}개)")
            p3 = False
        for col in ['quantity', 'total_cost_krw', 'realized_pl_krw']:
            gap = (m[f'{col}_구'] - m[f'{col}_신']).abs()
            bad = m[gap > 1.0]
            if len(bad):
                p3 = False
                print(f"  FAIL {col} — {len(bad)}개 종목 불일치:")
                for _, r in bad.head(10).iterrows():
                    print(f"         {r['account']}/{r['ticker']} : "
                          f"구 {r[f'{col}_구']:,.0f} vs 신 {r[f'{col}_신']:,.0f}")
            else:
                print(f"  OK   {col} — 완전 일치")

    print("\n" + "=" * 66)
    if p1 and p2 and p3:
        print("  결과: 검증 통과 ✓  — 신·구 결과 완전 일치. main.py 적용 안전.")
    else:
        print("  결과: 검증 실패 ✗  — 위 불일치 확인 필요. 적용 보류.")
    print("=" * 66 + "\n")


if __name__ == '__main__':
    main()
