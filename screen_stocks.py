"""코스피/코스닥 전 종목 중 30주 이동평균선 위에 있는 종목을 걸러 docs/data.json으로 저장."""
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import yfinance as yf

MA_WEEKS = 30
WORKERS = 8
# 한국 시장 가격제한폭(±30%)을 넘는 일간 변동은 감자/액면분할/거래정지 해제 등
# 기업 이벤트 → KRX 원주가 기반 이동평균이 왜곡되므로 수정주가로 재검증 필요
CORP_ACTION_THRESHOLD = 0.31
OUT_PATH = Path(__file__).parent / "docs" / "data.json"


def get_listings() -> pd.DataFrame:
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fdr.StockListing(market)
        df = df[["Code", "Name", "Market", "Close", "Marcap"]].copy()
        frames.append(df)
    merged = pd.concat(frames, ignore_index=True)
    # 우선주/스팩/리츠 등도 코드가 섞여 있지만 일단 전 종목 대상. 거래정지 등으로 Close가 0인 종목 제외
    merged = merged[merged["Close"] > 0]
    return merged


def evaluate(weekly: pd.Series):
    if len(weekly) < MA_WEEKS:
        return None
    ma30 = weekly.rolling(MA_WEEKS).mean().iloc[-1]
    price = float(weekly.iloc[-1])
    if pd.isna(ma30) or price <= ma30:
        return None
    return {
        "price": price,
        "ma30": round(float(ma30), 2),
        "gap_pct": round((price / float(ma30) - 1) * 100, 2),
    }


def check_adjusted(code: str, market: str):
    """감자/액면분할 등이 감지된 종목을 yfinance 수정주가로 재검증."""
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    df = yf.download(
        code + suffix, period="1y", interval="1wk",
        auto_adjust=True, progress=False,
    )
    if df is None or df.empty:
        return None
    weekly = df["Close"].iloc[:, 0].dropna()
    return evaluate(weekly)


def check_stock(code: str, market: str, start: str):
    try:
        df = fdr.DataReader(code, start)
        if df.empty:
            return None
        daily = df["Close"].dropna()
        max_move = daily.pct_change().abs().max()
        if pd.notna(max_move) and max_move > CORP_ACTION_THRESHOLD:
            # KRX 원주가는 기업 이벤트를 반영하지 않아 이동평균이 왜곡됨
            return check_adjusted(code, market)
        weekly = daily.resample("W").last().dropna()
        return evaluate(weekly)
    except Exception:
        return None


def main():
    listings = get_listings()
    total = len(listings)
    print(f"대상 종목: {total}")

    # 30주 MA 계산에 필요한 기간 + 여유
    start = (datetime.today() - timedelta(weeks=MA_WEEKS + 10)).strftime("%Y-%m-%d")

    results = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(check_stock, row.Code, row.Market, start): row
            for row in listings.itertuples(index=False)
        }
        for fut in as_completed(futures):
            row = futures[fut]
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{total} ({time.time() - t0:.0f}s)")
            res = fut.result()
            if res:
                results.append(
                    {
                        "code": row.Code,
                        "name": row.Name,
                        "market": row.Market,
                        "marcap": int(row.Marcap),
                        **res,
                    }
                )

    results.sort(key=lambda x: x["marcap"], reverse=True)
    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_scanned": total,
        "count": len(results),
        "stocks": results,
    }
    OUT_PATH.parent.mkdir(exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"완료: {len(results)}/{total} 종목이 30주선 위 → {OUT_PATH}")


if __name__ == "__main__":
    sys.exit(main())
