#!/usr/bin/env python3
"""
reflex_ensemble.py — Reflex 국면 필터의 조합 평균 노출 (Ensemble Equity Exposure)

Reflex 단계 판정의 주요 기준값 6개(Fundamentals 하한, 확인 일수, 최소 유지 일수, Momentum 기울기 기간,
임계값, 평활 기간)와 Fundamentals 가중치 2종으로 2,430개 조합을 만들고, 매일 그중 Risk-On(1~5단계)인
조합의 비율을 주식 노출 비중으로 쓴다. 기준값을 하나 고르는 단계가 없어 과최적화 위험이 없다.

워크포워드 검증(2015~2026, 거래비용·CD 금리 포함, 배당 제외):
  시장 보유 연 8.6%, Sharpe 0.48, MDD −46%  /  조합 평균 노출 연 12.6%, Sharpe 0.99, MDD −19%
  2,430개 조합 전부 시장보다 MDD가 작았고 97%가 Sharpe가 높았음.

출력: data/reflex/ensemble.parquet (date, exposure, n_up, n)
      docs/data/regime.json (오늘 노출, 국면 라벨, 최근 250일 노출 추이)
사용법: python reflex_ensemble.py [--push]
"""
import json, itertools, argparse, subprocess, importlib.util, os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
OUT_PQ = BASE / "data" / "reflex" / "ensemble.parquet"
OUT_JSON = BASE / "docs" / "data" / "regime.json"
GRID = dict(fund_gate=[35, 40, 45, 50, 55], confirm=[3, 5, 8], dwell=[5, 15, 25],
            m_slope=[10, 20, 40], th=[5, 10, 15], smooth=[10, 20, 40])
UP = {1, 2, 3, 4, 5}
ON, OFF = 0.7, 0.3          # 라벨 기준: 70% 이상 Risk-On, 30% 이하 Risk-Off, 사이는 Neutral


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def label(x):
    return "Risk-On" if x >= ON else "Risk-Off" if x <= OFF else "Neutral"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    a = ap.parse_args()
    spec = importlib.util.spec_from_file_location("reflex_mod", BASE / "reflex.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    P0 = dict(m.P)
    idx, kr, us, br, fl, *_ = m.load()
    X, comp = m.build(idx, kr, us, br, fl)
    S, comps, A0 = m.score(X)

    def axes(smooth, fw):
        out = {}
        for axis, cdef in m.COMP.items():
            cols = [f"{axis}:{c}" for c in cdef]
            w = {k: v[1] for k, v in cdef.items()}
            if axis == "F" and fw is not None:
                w = fw
            raw = m.wmean(comps[cols].set_axis(list(cdef), axis=1), w)
            span = P0["fast_smooth"] if axis == "Mf" else smooth
            out[axis] = raw.ewm(span=span, adjust=False, ignore_na=True).mean().where(raw.notna())
        return pd.DataFrame(out, index=X.index)

    FW = {"base": None, "equal": {k: 1 / len(m.COMP["F"]) for k in m.COMP["F"]}}
    keys = list(GRID)
    n_up = pd.Series(0.0, index=X.index); n_valid = pd.Series(0.0, index=X.index)
    for fw in FW.values():
        cache = {}
        for c in itertools.product(*GRID.values()):
            p = dict(zip(keys, c))
            if p["smooth"] not in cache:
                cache[p["smooth"]] = axes(p["smooth"], fw)
            m.P = dict(P0); m.P.update(p)
            _, conf, _, _ = m.stage_series(cache[p["smooth"]])
            v = conf.notna()
            n_valid += v.astype(float)
            n_up += conf.map(lambda s: 1.0 if s in UP else 0.0).where(v, 0.0)
    m.P = P0
    ens = pd.DataFrame({"n_up": n_up, "n": n_valid})
    ens["exposure"] = (ens.n_up / ens.n).where(ens.n > 0)
    ens = ens.dropna()
    ens.index.name = "date"
    OUT_PQ.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PQ.with_suffix(".parquet.tmp"); ens.to_parquet(tmp); os.replace(tmp, OUT_PQ)

    last = ens.iloc[-1]; x = float(last.exposure)
    h = ens.exposure.iloc[-250:]
    out = dict(
        as_of=str(ens.index[-1].date()), generated=datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        exposure=round(x, 3), label=label(x), n_up=int(last.n_up), n=int(last.n),
        prev=round(float(ens.exposure.iloc[-2]), 3) if len(ens) > 1 else None,
        thresholds=dict(on=ON, off=OFF),
        history=[[str(d.date()), round(float(v), 3)] for d, v in h.items()],
    )
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"); os.replace(tmp, OUT_JSON)
    log(f"ensemble: {out['as_of']} 노출 {x:.0%} ({out['n_up']}/{out['n']} 조합 Risk-On) → {out['label']}")
    if a.push:
        def run(*cmd):
            return subprocess.run(cmd, cwd=BASE, capture_output=True, text=True)
        rel = str(OUT_JSON.relative_to(BASE))
        run("git", "add", rel)
        if run("git", "diff", "--cached", "--quiet", "--", rel).returncode != 0:
            run("git", "commit", "-m", f"regime: {out['as_of']}", "--", rel)
            for _ in range(3):
                if run("git", "pull", "--rebase", "--autostash").returncode == 0 and run("git", "push").returncode == 0:
                    log("push: 완료"); break


if __name__ == "__main__":
    main()
