"""코스피/코스닥 전 종목 중 30주 이동평균선 위에 있는 종목을 걸러 docs/data.json으로 저장.

통과 종목은 웹 차트용 주봉 데이터(docs/charts/{code}.json)도 함께 생성한다.
"""
import json
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import yfinance as yf

MA_WEEKS = 30
CHART_WEEKS = 156  # 차트에 보여줄 기간(약 3년)
WORKERS = 8
# 한국 시장 가격제한폭(±30%)을 넘는 일간 변동은 감자/액면분할/거래정지 해제 등
# 기업 이벤트 → KRX 원주가 기반 이동평균이 왜곡되므로 수정주가로 재검증 필요
CORP_ACTION_THRESHOLD = 0.31
DOCS = Path(__file__).parent / "docs"
OUT_PATH = DOCS / "data.json"
CHART_DIR = DOCS / "charts"


def get_listings() -> pd.DataFrame:
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fdr.StockListing(market)
        df = df[["Code", "Name", "Market", "Close", "Marcap"]].copy()
        frames.append(df)
    merged = pd.concat(frames, ignore_index=True)
    # 코스닥 글로벌 세그먼트는 코스닥 내 우량주 분류일 뿐이므로 코스닥으로 통합
    merged["Market"] = merged["Market"].replace("KOSDAQ GLOBAL", "KOSDAQ")
    # 거래정지 등으로 Close가 0인 종목 제외
    merged = merged[merged["Close"] > 0]
    return merged


def build_payload(weekly_ohlc: pd.DataFrame):
    """주봉 OHLC DataFrame → 스크리닝 결과 + 차트 데이터. 30주선 아래면 None."""
    close = weekly_ohlc["Close"].dropna()
    if len(close) < MA_WEEKS:
        return None
    ma30 = close.rolling(MA_WEEKS).mean()
    price = float(close.iloc[-1])
    last_ma = ma30.iloc[-1]
    if pd.isna(last_ma) or price <= last_ma:
        return None

    # 30주선 상향돌파 후 연속으로 위에 머문 주 수 (MA 계산 가능 구간 내에서)
    above = (close > ma30)[ma30.notna()]
    weeks_above = 0
    for is_above in reversed(above.tolist()):
        if not is_above:
            break
        weeks_above += 1

    tail = weekly_ohlc.tail(CHART_WEEKS)
    candles = [
        {
            "time": ts.strftime("%Y-%m-%d"),
            "open": round(float(r.Open), 2),
            "high": round(float(r.High), 2),
            "low": round(float(r.Low), 2),
            "close": round(float(r.Close), 2),
            "volume": int(r.Volume) if pd.notna(r.Volume) else 0,
        }
        for ts, r in tail.iterrows()
        if not (pd.isna(r.Open) or pd.isna(r.Close))
    ]
    ma_points = [
        {"time": ts.strftime("%Y-%m-%d"), "value": round(float(v), 2)}
        for ts, v in ma30.loc[tail.index[0]:].items()
        if not pd.isna(v)
    ]
    return {
        "price": price,
        "ma30": round(float(last_ma), 2),
        "gap_pct": round((price / float(last_ma) - 1) * 100, 2),
        "weeks_above": weeks_above,
        "chart": {"candles": candles, "ma30": ma_points},
    }


def weekly_from_daily(daily: pd.DataFrame) -> pd.DataFrame:
    return daily.resample("W-FRI").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    ).dropna(how="all")


def check_adjusted(code: str, market: str):
    """감자/액면분할 등이 감지된 종목을 yfinance 수정주가로 재검증."""
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    df = yf.download(
        code + suffix, period="4y", interval="1wk",
        auto_adjust=True, progress=False,
    )
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return build_payload(df[["Open", "High", "Low", "Close", "Volume"]])


def check_stock(code: str, market: str, start: str):
    try:
        df = fdr.DataReader(code, start)
        if df.empty:
            return None
        daily = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
        max_move = daily["Close"].pct_change().abs().max()
        if pd.notna(max_move) and max_move > CORP_ACTION_THRESHOLD:
            # KRX 원주가는 기업 이벤트를 반영하지 않아 이동평균이 왜곡됨
            return check_adjusted(code, market)
        return build_payload(weekly_from_daily(daily))
    except Exception:
        return None


def main():
    listings = get_listings()
    total = len(listings)
    print(f"대상 종목: {total}")

    # 차트 표시 기간 + MA 계산 워밍업 + 여유
    start = (datetime.today() - timedelta(weeks=CHART_WEEKS + MA_WEEKS + 8)).strftime("%Y-%m-%d")

    results = []
    charts = {}
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
                charts[row.Code] = res.pop("chart")
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
    DOCS.mkdir(exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    # 탈락 종목의 이전 차트 파일이 남지 않도록 전체 재생성
    if CHART_DIR.exists():
        shutil.rmtree(CHART_DIR)
    CHART_DIR.mkdir()
    for code, chart in charts.items():
        (CHART_DIR / f"{code}.json").write_text(
            json.dumps(chart, ensure_ascii=False), encoding="utf-8"
        )
    print(f"완료: {len(results)}/{total} 종목이 30주선 위 → {OUT_PATH}")
    print(f"차트 파일 {len(charts)}개 생성 → {CHART_DIR}")


if __name__ == "__main__":
    sys.exit(main())
