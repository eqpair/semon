import json, sys
sys.path.insert(0, '/home/eq/semon')
from config import SECTORS
from sector_signal import ohlcv_store, update_ohlcv
from crawler import fetch_all_ohlcv
import asyncio

async def main():
    all_codes = list({code for codes in SECTORS.values() for code, _ in codes})
    print("OHLCV 로딩 중...")
    ohlcv = await fetch_all_ohlcv(all_codes)
    update_ohlcv(ohlcv)

    results = []
    for sector, codes in SECTORS.items():
        code_list = [c for c, _ in codes]
        closes = {}
        for code in code_list:
            entry = ohlcv_store.get(code)
            if entry and len(entry["closes"]) >= 60:
                closes[code] = entry["closes"][-60:]  # 최근 60일

        if len(closes) < 2:
            results.append((sector, 0, len(closes), []))
            continue

        # 수익률 계산
        codes_ok = list(closes.keys())
        returns = {}
        for code in codes_ok:
            c = closes[code]
            returns[code] = [(c[i]-c[i-1])/c[i-1] for i in range(1, len(c))]

        # 평균 상관계수
        import statistics
        corrs = []
        low_corr = []
        n = len(codes_ok)
        for i in range(n):
            for j in range(i+1, n):
                a, b = returns[codes_ok[i]], returns[codes_ok[j]]
                if len(a) != len(b): continue
                mean_a = sum(a)/len(a)
                mean_b = sum(b)/len(b)
                cov = sum((a[k]-mean_a)*(b[k]-mean_b) for k in range(len(a)))/len(a)
                std_a = (sum((x-mean_a)**2 for x in a)/len(a))**0.5
                std_b = (sum((x-mean_b)**2 for x in b)/len(b))**0.5
                if std_a > 0 and std_b > 0:
                    corrs.append(cov/(std_a*std_b))

        avg_corr = sum(corrs)/len(corrs) if corrs else 0

        # 테마 평균과 상관계수 낮은 종목 찾기
        theme_avg = [sum(returns[c][i] for c in codes_ok)/len(codes_ok)
                     for i in range(len(list(returns.values())[0]))]
        
        low_codes = []
        for code in codes_ok:
            r = returns[code]
            mean_r = sum(r)/len(r)
            mean_t = sum(theme_avg)/len(theme_avg)
            cov = sum((r[i]-mean_r)*(theme_avg[i]-mean_t) for i in range(len(r)))/len(r)
            std_r = (sum((x-mean_r)**2 for x in r)/len(r))**0.5
            std_t = (sum((x-mean_t)**2 for x in theme_avg)/len(theme_avg))**0.5
            if std_r > 0 and std_t > 0:
                c_corr = cov/(std_r*std_t)
                name = dict(codes).get(code, code)
                if c_corr < 0.3:
                    low_codes.append((name, round(c_corr,2)))

        results.append((sector, round(avg_corr,3), len(closes), low_codes))

    # 상관계수 낮은 순으로 출력
    results.sort(key=lambda x: x[1])
    print(f"\n{'테마':<20} {'평균상관':>8} {'종목수':>5}  낮은종목")
    print("-"*70)
    for sector, corr, cnt, low in results:
        flag = "⚠️ " if corr < 0.3 else "  "
        low_str = ", ".join(f"{n}({c})" for n,c in low[:3]) if low else ""
        print(f"{flag}{sector:<18} {corr:>8.3f} {cnt:>5}  {low_str}")

asyncio.run(main())