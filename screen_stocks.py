"""코스피/코스닥 전 종목 중 30주 이동평균선 위에 있는 종목을 걸러 docs/data.json으로 저장.

통과 종목은 웹 차트용 주봉 데이터(docs/charts/{code}.json)도 함께 생성한다.
"""
import json
import re
import shutil
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import requests
import yfinance as yf

MA_WEEKS = 30
CHART_WEEKS = 156  # 차트에 보여줄 기간(약 3년)
WORKERS = 8
# 한국 시장 가격제한폭(±30%)을 넘는 일간 변동은 감자/액면분할/거래정지 해제 등
# 기업 이벤트 → KRX 원주가 기반 이동평균이 왜곡되므로 수정주가로 재검증 필요
CORP_ACTION_THRESHOLD = 0.31
# 30주선 추세 분석에 부적합하거나 사용자가 제외 요청한 ETF 유형:
#   - 인버스: 30주선 위 = 기초지수 하락 추세라 신호 해석이 반대
#   - 채권/머니마켓/금리형: 가격이 사실상 단조 우상향이라 상시 30주선 위
#   - 미국 등 해외주식 / 원자재 / 레버리지: 사용자 요청으로 제외 (국내 종목만)
# 주의:
#   - '금'은 금융, '은'은 은행을 오탐하므로 원자재는 정밀 키워드만 사용
#   - '글로벌'은 국내 '코스닥글로벌'을 오탐하지 않게 lookbehind로 제외
#   - '니케이/TOPIX'는 '커뮤니케이션'을 오탐하므로 '일본'으로 대체
#   - MSCI Korea(국내)는 유지되도록 'MSCI' 단독 대신 국가/지역명으로 매칭
ETF_EXCLUDE = (
    # 채권·머니마켓·금리형 (○○채는 '채권'이 아니라 '채'로 끝나 별도 매칭)
    "인버스|채권|국채|국고채|국공채|회사채|금융채|은행채|특수채|여전채|카드채|"
    "공사채|전단채|크레딧|물가채|통안|머니마켓|MMF|파킹|"
    "금리|KOFR|SOFR|양도성예금|초단기|"
    # 레버리지
    "레버리지|2X|3X|"
    # 원자재
    "원유|WTI|천연가스|천연자원|원자재|농산물|옥수수|콩선물|니켈|팔라듐|"
    "백금|귀금속|커머디티|골드|은선물|구리|KRX금|금현물|금선물|국제금|금액티브|"
    # 미국 주식
    "미국|S&P|나스닥|다우|필라델피아|러셀|"
    # 기타 해외주식 (글로벌은 코스닥글로벌 보호)
    r"(?<!코스닥)글로벌|월드|해외|중국|차이나|본토|과창판|심천|항셍|HSCEI|CSI|"
    "ChiNext|A50|일본|유럽|유로|독일|DAX|인도|베트남|VN30|신흥국|선진|이머징|"
    "MSCI EM|러시아|멕시코|필리핀|아시아|대만|라틴|브라질|홍콩|태국|사우디|싱가포르|"
    # 해외기업 테마(밸류체인/고정테크 등) 및 한중 혼합. '메타'는 '메타버스' 오탐이라 제외.
    # 한국기업 밸류체인(현대차·SK하이닉스 등)은 여기 안 걸려 유지됨.
    "엔비디아|테슬라|팔란티어|버크셔|구글|마이크로소프트|브로드컴|샤오미|BYD|"
    "일라이릴리|TSMC|애플|아마존|넷플릭스|알파벳|퀄컴|ASML|텐센트|알리바바|"
    "소니|도요타|한중"
)

# 사용자 요청 추가 제외: 시장/규모 지수 추종 · TR(토탈리턴) · 커버드콜 · 배당형.
# 지수형 = 시장/규모 대표지수어를 포함하되 섹터/테마 수식어가 없는 것
#   (예: 'KODEX 200'·'코스닥150'=제외, 'TIGER 200 IT'·'코스닥150바이오테크'=섹터라 유지)
ETF_MARKET_INDEX = re.compile(
    r"코스피|코스닥|\bKRX\d|MSCI|(?<!\d)200(?!\d)|K200|"
    r"대형주|중형주|중소형|소형주|코리아TOP10"
)
ETF_SECTOR_HINT = re.compile(
    r"IT|금융|건설|소비재|산업재|에너지|중공업|철강|소재|헬스케어|커뮤니케이션|"
    r"반도체|바이오|2차전지|게임|인터넷|조선|방산|원자력|자동차|증권|은행|미디어|"
    r"소프트|전력|로봇|수출|휴머노이드|고배당|기후"
)
ETF_TR = re.compile(r"TR(?![A-Za-z])")  # 뒤에 영문자 없는 TR만 → 'TREX'·'TRF'는 오탐 안 됨
# 자산배분/혼합형: 채권·해외자산이 섞여 한국 섹터/테마 주식 ETF가 아님
#   TDF(은퇴시점) · TRF(위험조절) · TIF(인출) · 멀티에셋 · 주식혼합
ETF_ALLOCATION = re.compile(r"TDF|TRF|TIF|멀티에셋|혼합")


def etf_extra_excluded(name: str) -> bool:
    is_index = bool(ETF_MARKET_INDEX.search(name)) and not ETF_SECTOR_HINT.search(name)
    return (
        is_index
        or bool(ETF_TR.search(name))
        or bool(ETF_ALLOCATION.search(name))
        or "커버드콜" in name
        or "배당" in name
    )


# GitHub Actions 러너는 UTC라 datetime.now()를 그대로 쓰면 화면에 UTC가 찍힌다
KST = timezone(timedelta(hours=9))

DOCS = Path(__file__).parent / "docs"
OUT_PATH = DOCS / "data.json"
CHART_DIR = DOCS / "charts"
REPORT_PATH = DOCS / "reports.json"
THEME_PATH = DOCS / "themes.json"
BELOW_PATH = DOCS / "below.json"

# 네이버 종목분석 리포트. 정적 사이트라 브라우저에서 네이버를 직접 부르면 CORS로
# 막히므로 스크리닝 때 미리 받아 JSON으로 굽는다. 목록에 목표주가·투자의견은 없고
# 제목·증권사·작성일·PDF 링크만 있다(그건 리포트별 상세 페이지를 열어야 함).
REPORT_LIST_URL = "https://finance.naver.com/research/company_list.naver?page={}"
REPORT_PAGES = 400      # 30건/페이지 → 약 12,000건, 최근 14개월치
# 종목당 개수는 제한하지 않는다(수집 기간이 상한 역할). 전 종목 합쳐 약 4,200건,
# 파일 841KB지만 gzip 전송 시 약 150KB라 지연 로딩으로 감당된다.

# 네이버 금융 테마 분류(266개). 업종(WICS)이 배타적인 산업 구분이라면 테마는
# 'CXL', '소캠' 같은 재료 단위라 한 종목이 여러 테마에 중복 소속된다(평균 2.8개).
# 인포스탁 데이터는 증권사 HTS 전용 상용 공급이라 개인이 쓸 공개 API가 없다.
NAVER_THEME_LIST_URL = "https://finance.naver.com/sise/theme.naver?page={}"
NAVER_THEME_DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={}"
THEME_PAGES = 7          # 페이지당 40개 → 266개 전부

# 네이버 금융 업종 분류(WICS 기반, 79개). KRX/통계청 표준산업분류(KSIC)는
# 지주사가 전부 '기타 금융업'으로 뭉개져 투자 관점 분류로 못 쓴다.
NAVER_GROUP_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
NAVER_DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={}"
UA = {"User-Agent": "Mozilla/5.0"}
PREF_SUFFIX = re.compile(r"(\d?우[BC]?)$")  # 삼성물산우B, SK우 등 우선주 접미사


def _naver_get(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"
    return r.text


def get_sector_map() -> dict:
    """네이버 업종 분류 → {종목코드: 업종명}. 실패 시 빈 dict(섹터 없이 진행)."""
    try:
        html = _naver_get(NAVER_GROUP_URL)
    except Exception as e:
        print(f"[경고] 업종 목록 조회 실패: {e!r}", file=sys.stderr)
        return {}
    groups = re.findall(
        r"sise_group_detail\.naver\?type=upjong&no=(\d+)\">([^<]+)</a>", html
    )
    if not groups:
        print("[경고] 업종 목록 파싱 실패 (네이버 페이지 구조 변경 가능)", file=sys.stderr)
        return {}

    def fetch(item):
        no, name = item
        try:
            return name, re.findall(
                r"/item/main\.naver\?code=(\d{6})", _naver_get(NAVER_DETAIL_URL.format(no))
            )
        except Exception:
            return name, None

    mapping, failed = {}, 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for name, codes in ex.map(fetch, groups):
            if codes is None:
                failed += 1
                continue
            for c in codes:
                mapping[c] = name
    print(f"업종 분류: {len(groups) - failed}/{len(groups)}개 업종, {len(mapping)}종목 매핑")
    return mapping


def get_themes() -> list:
    """[{name, total, codes}] — codes는 나중에 통과 종목만 남긴다. 실패 시 빈 리스트."""
    pairs = []
    for page in range(1, THEME_PAGES + 1):
        try:
            html = _naver_get(NAVER_THEME_LIST_URL.format(page))
        except Exception:
            continue
        pairs += re.findall(
            r"/sise/sise_group_detail\.naver\?type=theme&no=(\d+)\">([^<]+)</a>", html
        )
    pairs = list(dict.fromkeys(pairs))
    if not pairs:
        print("[경고] 테마 목록을 받지 못했습니다", file=sys.stderr)
        return []

    def fetch(item):
        no, name = item
        try:
            codes = re.findall(
                r"/item/main\.naver\?code=(\d{6})",
                _naver_get(NAVER_THEME_DETAIL_URL.format(no)),
            )
            return name, list(dict.fromkeys(codes))
        except Exception:
            return name, None

    out, failed = [], 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for name, codes in ex.map(fetch, pairs):
            if codes is None:
                failed += 1
                continue
            out.append({"name": name, "total": len(codes), "codes": codes})
    print(f"테마 분류: {len(out)}/{len(pairs)}개 테마 수집 (실패 {failed})")
    return out


def _parse_report_page(page: int):
    """리포트 목록 한 페이지 → [{code,title,broker,date,pdf,nid}]. 실패 시 None."""
    try:
        html = _naver_get(REPORT_LIST_URL.format(page))
    except Exception:
        return None
    out = []
    for row in re.findall(r"<tr>(.*?)</tr>", html, re.S):
        m = re.search(r"/item/main\.naver\?code=(\d{6})", row)
        if not m:
            continue
        cells = [re.sub(r"<[^>]+>", "", c).strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
        if len(cells) < 5:
            continue
        nid = re.search(r"company_read\.naver\?nid=(\d+)", row)
        pdf = re.search(r'href="(https://stock\.pstatic\.net[^"]+\.pdf)"', row)
        out.append({
            "code": m.group(1), "title": cells[1], "broker": cells[2], "date": cells[4],
            "pdf": pdf.group(1) if pdf else None, "nid": nid.group(1) if nid else None,
        })
    return out


def get_reports(codes: set) -> dict:
    """종목코드별 최신 리포트. 실패해도 스크리닝은 계속되도록 빈 dict를 돌려준다."""
    by_code = defaultdict(list)
    failed = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(_parse_report_page, range(1, REPORT_PAGES + 1)):
            if res is None:
                failed += 1
                continue
            for r in res:
                if r["code"] in codes:
                    by_code[r["code"]].append(r)
    if failed == REPORT_PAGES:
        print("[경고] 리포트 목록을 한 건도 받지 못했습니다", file=sys.stderr)
        return {}
    # 목록이 최신순이라 그대로 담으면 최근 것이 위에 온다
    out = {c: [{k: v for k, v in r.items() if k != "code"} for r in rs]
           for c, rs in by_code.items()}
    total = sum(len(v) for v in out.values())
    print(f"리포트: {REPORT_PAGES - failed}/{REPORT_PAGES}페이지, "
          f"{len(out)}종목 커버, {total}건")
    return out


def resolve_sector(code: str, name: str, market: str, sector_map: dict) -> str:
    """종목의 업종명. ETF는 별도 버킷, 우선주는 보통주 업종을 물려받는다."""
    if market == "ETF":
        return "ETF"
    if code in sector_map:
        return sector_map[code]
    # 우선주: 코드 끝자리만 다른 보통주(005930 ← 005935)를 먼저 시도
    base = code[:5] + "0"
    if base in sector_map:
        return sector_map[base]
    return "기타"


def get_listings() -> pd.DataFrame:
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fdr.StockListing(market)
        df = df[["Code", "Name", "Market", "Close", "Marcap", "ChagesRatio"]].copy()
        df = df.rename(columns={"ChagesRatio": "ChangeRate"})  # FDR 원본 오타
        frames.append(df)

    # ETF: 컬럼 체계가 달라 표준 스키마로 정규화 (Symbol→Code, Price→Close,
    # MarCap은 억원 단위라 원 단위로 환산해 주식 Marcap과 통일)
    etf = fdr.StockListing("ETF/KR")
    etf = pd.DataFrame({
        "Code": etf["Symbol"],
        "Name": etf["Name"],
        "Market": "ETF",
        "Close": etf["Price"],
        "Marcap": etf["MarCap"].fillna(0) * 10**8,
        "ChangeRate": etf["ChangeRate"],
    })
    # 인버스·채권·머니마켓·금리형·해외·원자재·레버리지 ETF 제외
    etf = etf[~etf["Name"].str.contains(ETF_EXCLUDE, regex=True, na=False)]
    # 시장/규모 지수 추종·TR·커버드콜·배당형 ETF 제외
    etf = etf[~etf["Name"].apply(etf_extra_excluded)]
    frames.append(etf)

    merged = pd.concat(frames, ignore_index=True)
    # 코스닥 글로벌 세그먼트는 코스닥 내 우량주 분류일 뿐이므로 코스닥으로 통합
    merged["Market"] = merged["Market"].replace("KOSDAQ GLOBAL", "KOSDAQ")
    # 거래정지 등으로 Close가 0인 종목 제외
    merged = merged[merged["Close"] > 0]
    # 스팩(기업인수목적회사)은 합병 대상 탐색용 페이퍼컴퍼니라 추세 분석 대상이 아님
    merged = merged[~merged["Name"].str.contains("스팩", na=False)]
    return merged


def build_payload(weekly_ohlc: pd.DataFrame):
    """주봉 OHLC DataFrame → 지표 + 차트 데이터. 30주 미만이면 None.

    30주선 아래 종목도 차트를 만든다. 테마 화면에서 미달 종목 이름을 눌러도
    차트·리포트가 뜨게 하려면 통과 여부와 무관하게 차트가 필요하다.
    통과 여부는 above 플래그로 알린다.
    """
    close = weekly_ohlc["Close"].dropna()
    if len(close) < MA_WEEKS:
        return None
    ma30 = close.rolling(MA_WEEKS).mean()
    price = float(close.iloc[-1])
    last_ma = ma30.iloc[-1]
    if pd.isna(last_ma):
        return None

    # 30주선 상향돌파 후 연속으로 위에 머문 주 수 (MA 계산 가능 구간 내에서)
    above = (close > ma30)[ma30.notna()]
    weeks_above = 0
    for is_above in reversed(above.tolist()):
        if not is_above:
            break
        weeks_above += 1

    tail = weekly_ohlc.tail(CHART_WEEKS)
    return {
        "above": bool(price > last_ma),
        "price": price,
        "ma30": round(float(last_ma), 2),
        "gap_pct": round((price / float(last_ma) - 1) * 100, 2),
        "weeks_above": weeks_above,
        "chart": compact_chart(tail, ma30),
    }


def _num(x):
    """정수로 떨어지면 정수로 — 국내 주가는 대부분 원 단위라 소수점이 낭비다."""
    i = round(float(x))
    return i if abs(float(x) - i) < 0.005 else round(float(x), 2)


def compact_chart(tail: pd.DataFrame, ma30: pd.Series) -> dict:
    """차트 JSON 압축 형식. 종목당 22.7KB → 6.5KB (3.5배).

    키 이름을 봉마다 반복하고 날짜를 문자열로 쓰는 게 용량의 대부분이었다.
    30주선 아래 종목까지 차트를 만들면 파일이 530개에서 2,450개로 늘어나
    원래 형식으로는 53MB가 되므로 형식을 줄여야 한다.
      t0: 첫 봉 날짜, d: t0로부터의 일수(주봉이라 대개 7의 배수),
      c: [시가, 고가, 저가, 종가, 거래량], m: 30주선(값 없으면 null)
    """
    rows = [(ts, r) for ts, r in tail.iterrows()
            if not (pd.isna(r.Open) or pd.isna(r.Close))]
    if not rows:
        return {"t0": "", "d": [], "c": [], "m": []}
    t0 = rows[0][0]
    return {
        "t0": t0.strftime("%Y-%m-%d"),
        "d": [(ts - t0).days for ts, _ in rows],
        "c": [[_num(r.Open), _num(r.High), _num(r.Low), _num(r.Close),
               int(r.Volume) if pd.notna(r.Volume) else 0] for _, r in rows],
        "m": [None if pd.isna(ma30.get(ts)) else _num(ma30.get(ts)) for ts, _ in rows],
    }


def get_trading_days(days: int = 40) -> list:
    """최근 개장일 목록(YYYY-MM-DD). 실패하면 빈 리스트.

    웹에서 NEW 배지를 '달력일'이 아니라 '영업일'로 세려면 실제 개장일이 필요하다.
    주말만 빼면 추석·설 같은 휴장일에 하루씩 어긋난다. 코스피 지수 일봉의
    인덱스가 곧 개장일이라 요청 한 번으로 정확한 달력을 얻는다.
    """
    start = (datetime.today() - timedelta(days=days * 2 + 30)).strftime("%Y-%m-%d")
    try:
        idx = fdr.DataReader("KS11", start).index
    except Exception as e:
        print(f"[경고] 개장일 목록을 받지 못했습니다: {e!r}", file=sys.stderr)
        return []
    return [d.strftime("%Y-%m-%d") for d in idx][-days:]


def weekly_from_daily(daily: pd.DataFrame) -> pd.DataFrame:
    return daily.resample("W-FRI").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    ).dropna(how="all")


def check_adjusted(code: str, market: str):
    """감자/액면분할 등이 감지된 종목을 yfinance 수정주가로 재검증.

    데이터를 아예 못 받으면(재시도 후에도) LookupError — 호출부에서
    '검증 불가'로 집계해 조용한 누락을 방지한다.
    """
    # 국내 ETF는 yfinance에서 대부분 .KS. 코스닥 종목만 .KQ.
    suffix = ".KQ" if market == "KOSDAQ" else ".KS"
    last_err = None
    for attempt in range(2):
        try:
            df = yf.download(
                code + suffix, period="4y", interval="1wk",
                auto_adjust=True, progress=False,
            )
        except Exception as e:
            last_err = e
            time.sleep(2)
            continue
        if df is None or df.empty:
            last_err = "empty"
            time.sleep(2)
            continue
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return build_payload(df[["Open", "High", "Low", "Close", "Volume"]])
    raise LookupError(f"yfinance 데이터 없음: {last_err}")


def check_stock(code: str, market: str, start: str):
    """반환: (결과 or None, 상태). 상태가 ok가 아니면 검증 불가로 집계."""
    try:
        df = fdr.DataReader(code, start)
        if df.empty:
            return None, "ok"  # 상장폐지 등 데이터 자체가 없는 정상 케이스
        daily = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
        max_move = daily["Close"].pct_change().abs().max()
        if pd.notna(max_move) and max_move > CORP_ACTION_THRESHOLD:
            # KRX 원주가는 기업 이벤트를 반영하지 않아 이동평균이 왜곡됨
            try:
                return check_adjusted(code, market), "ok"
            except LookupError:
                return None, "unverified"
        return build_payload(weekly_from_daily(daily)), "ok"
    except Exception as e:
        print(f"  [오류] {code}: {e!r}", file=sys.stderr)
        return None, "error"


def main():
    listings = get_listings()
    total = len(listings)
    print(f"대상 종목: {total}")
    sector_map = get_sector_map()
    # 섹터별 '전체' 종목 수 — 히트맵에서 30주선 위 비중(몇/전체)을 내는 분모.
    # 우리가 실제로 스캔한 개별 종목만 세야 비율이 맞는다(ETF 제외).
    sector_totals = Counter(
        resolve_sector(row.Code, row.Name, row.Market, sector_map)
        for row in listings.itertuples(index=False)
        if row.Market != "ETF"
    )

    # 테마는 스크리닝보다 먼저 받는다. 테마 화면이 목록보다 먼저 필요한 건 아니지만,
    # 수집이 실패하면 그 사실을 9분짜리 스크리닝 전에 알 수 있다.
    raw_themes = get_themes()

    # 차트 표시 기간 + MA 계산 워밍업 + 여유
    start = (datetime.today() - timedelta(weeks=CHART_WEEKS + MA_WEEKS + 8)).strftime("%Y-%m-%d")

    results = []
    charts = {}
    below = {}          # 30주선 아래 개별주: 코드 → [이름, 섹터, 이격률]
    unverified = []
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
            res, status = fut.result()
            if status != "ok":
                unverified.append({"code": row.Code, "name": row.Name, "reason": status})
            if not res:
                continue
            chart = res.pop("chart")
            sector = resolve_sector(row.Code, row.Name, row.Market, sector_map)
            if res.pop("above"):
                charts[row.Code] = chart
                results.append(
                    {
                        "code": row.Code,
                        "name": row.Name,
                        "market": row.Market,
                        "marcap": int(row.Marcap or 0),
                        "sector": sector,
                        "change_pct": round(float(row.ChangeRate or 0), 2),
                        **res,
                    }
                )
            elif row.Market != "ETF":
                # 미달 종목도 차트를 남긴다. 섹터·테마 화면에서 미달 종목 이름을
                # 눌러도 차트가 뜬다. ETF는 두 화면 모두에서 빼므로 제외한다.
                charts[row.Code] = chart
                below[row.Code] = [row.Name, sector, res["gap_pct"]]

    # 이전 실행 결과와 비교해 '최초 진입일(since)'을 유지 — 웹에서 최근 진입 종목에
    # NEW 배지를 띄우는 근거. 계속 30주선 위면 날짜를 보존하고, 이탈했다 재진입하면
    # 새 날짜로 갱신된다(직전 결과에 없으면 오늘이 최초 진입일).
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    prev_since = {}
    if OUT_PATH.exists():
        try:
            prev = json.loads(OUT_PATH.read_text(encoding="utf-8"))
            prev_since = {s["code"]: s.get("since") for s in prev.get("stocks", [])}
        except (json.JSONDecodeError, KeyError):
            pass
    for s in results:
        if s["code"] not in prev_since:
            s["since"] = today  # 직전 결과에 없던 종목 = 오늘 신규 진입
        else:
            # 이미 있던 종목: 기록된 날짜 유지. since 도입 전 데이터는 기간(주)으로
            # 역산해 채워, 최초 배포 때 전 종목이 NEW로 뜨는 것을 방지한다.
            weeks = max(int(s.get("weeks_above") or 1) - 1, 0)
            s["since"] = prev_since[s["code"]] or (
                now - timedelta(weeks=weeks)
            ).strftime("%Y-%m-%d")

    results.sort(key=lambda x: x["marcap"], reverse=True)
    out = {
        "updated": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        "total_scanned": total,
        "count": len(results),
        "unverified": unverified,
        "sector_totals": dict(sector_totals),
        "trading_days": get_trading_days(),
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
    passed = {r["code"] for r in results}

    # 테마: 통과 종목(codes)과 미달 종목(below)을 나눠 저장한다.
    # 한 종목이 여러 테마에 중복 소속되므로 합계는 전체 종목 수와 맞지 않는다.
    themes, used = [], set()
    for t in raw_themes:
        hit = [c for c in t["codes"] if c in passed]
        # 스크리닝하지 못한 코드는 버린다. 네이버 테마에는 우리가 제외한
        # ETF·스팩이나 상장폐지 종목이 섞여 있다.
        miss = [c for c in t["codes"] if c in below]
        # 스크리닝 대상이 하나도 없는 테마는 버린다. 스팩 테마가 여기 걸리는데,
        # 스팩은 아예 스캔하지 않으므로 '0/41 (0%)'가 사실과 다른 값이 된다.
        if not hit and not miss:
            continue
        themes.append({"name": t["name"], "total": t["total"],
                       "codes": hit, "below": miss})
        used.update(miss)
    THEME_PATH.write_text(
        json.dumps({"updated": out["updated"], "themes": themes}, ensure_ascii=False),
        encoding="utf-8",
    )

    # 30주선 아래 종목은 data.json에 없다(거긴 통과 종목만). 섹터·테마 화면이
    # 미달 종목 목록을 그리려면 이름·섹터·이격률이 필요해 따로 싣는다.
    # 두 화면이 같은 사전을 쓰므로 파일 하나로 둔다.
    BELOW_PATH.write_text(
        json.dumps({"updated": out["updated"], "stocks": below}, ensure_ascii=False),
        encoding="utf-8",
    )

    # 통과 종목 + 테마에 걸린 미달 종목의 증권사 리포트.
    # 리포트 목록은 어차피 400페이지를 통째로 받으므로 대상만 넓히면 된다.
    # (수집 실패해도 스크리닝 결과는 그대로 유지)
    reports = get_reports(passed | set(below))
    REPORT_PATH.write_text(
        json.dumps({"updated": out["updated"], "reports": reports}, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"완료: {len(results)}/{total} 종목이 30주선 위 → {OUT_PATH}")
    print(f"차트 파일 {len(charts)}개 생성 → {CHART_DIR}")
    print(f"리포트 파일 → {REPORT_PATH} ({REPORT_PATH.stat().st_size // 1024}KB)")
    print(f"테마 파일 → {THEME_PATH} "
          f"({len(themes)}개 테마, 미달 {len(used)}종목 참조, "
          f"{THEME_PATH.stat().st_size // 1024}KB)")
    print(f"미달 파일 → {BELOW_PATH} "
          f"({len(below)}종목, {BELOW_PATH.stat().st_size // 1024}KB)")
    print(f"차트 대상: 통과 {len(passed)} + 미달 {len(below)} = {len(charts)}종목")
    if unverified:
        print(f"[주의] 검증 불가 {len(unverified)}종목 (데이터 오류로 판정 제외):")
        for u in unverified:
            print(f"  {u['code']} {u['name']} ({u['reason']})")


if __name__ == "__main__":
    sys.exit(main())
