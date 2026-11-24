#!/usr/bin/env python3
"""配息管家 資料管線 v0（小型驗證）

只接已確認授權的開放資料（見「資料授權確認表」），不爬任何網頁。
流程：取得資料 -> 檢查欄位 -> 正規化 -> 品質檢查 -> 寫入 SQLite。

用法：
  python pipeline.py --offline DIR            # 讀 DIR 裡下載好的 CSV（離線測試）
  python pipeline.py                          # 連 openapi.twse.com.tw 取得 JSON
  python pipeline.py --offline DIR --demo     # 跑完後印出查詢範例
"""
import argparse
import csv
import datetime
import io
import json
import pathlib
import re
import sqlite3
import ssl
import sys
import urllib.request

OPENAPI = "https://openapi.twse.com.tw/v1"
TPEX_OPENAPI = "https://www.tpex.org.tw/openapi/v1"
ATTRIBUTION = (
    f"資料來源：臺灣證券交易所 {datetime.date.today().year} 上市公司股東會公告資料（OpenAPI t187ap41_L，v1）、"
    f"上市公司股利分派情形（OpenAPI t187ap45_L，v1）、上市股票除權除息預告表（OpenAPI TWT48U_ALL，v1）、"
    f"上市個股日成交資訊（OpenAPI STOCK_DAY_ALL，v1）；證券櫃檯買賣中心 {datetime.date.today().year} "
    f"上櫃股票每日收盤行情（OpenAPI tpex_mainboard_daily_close_quotes，v1）、"
    f"上櫃股票除權除息預告表（OpenAPI tpex_exright_prepost，v1）、"
    f"上櫃公司召開股東常 (臨時) 會日期、地點及採用電子投票情形等資料彙總表（OpenAPI t187ap41_O，v1）、"
    f"上櫃公司股利分派情形（data.gov.tw 資料集 58285，t187ap45_O，每日更新版）。"
    "此開放資料依政府資料開放授權條款 (Open Government Data License) 進行公眾釋出，"
    "使用者於遵守本條款各項規定之前提下，得利用之。政府資料開放授權條款：https://data.gov.tw/license"
)
MIN_KEEP_RATIO = 0.7  # 新資料筆數低於上次的 70%，視為異常，不覆蓋

SOURCES = {
    "meetings": {
        "path": "/opendata/t187ap41_L",
        "csv": "t187ap41_L.csv",
        "need": ["出表日期", "公司代號", "公司名稱", "股東常(臨時)會",
                 "開會日期", "是否採電子投票", "股務單位"],
    },
    "dividends": {
        "path": "/opendata/t187ap45_L",
        "csv": "t187ap45_L.csv",
        "need": ["出表日期", "公司代號", "公司名稱", "決議（擬議）進度", "股利年度",
                 "股利所屬年(季)度", "期別", "董事會（擬議）股利分派日", "股東會日期",
                 "股東配發-盈餘分配之現金股利(元/股)",
                 "股東配發-法定盈餘公積發放之現金(元/股)",
                 "股東配發-資本公積發放之現金(元/股)"],
    },
}

SOURCES["exdiv"] = {
    "path": "/exchangeReport/TWT48U_ALL",
    "json": "TWT48U_ALL.json",
    "need": ["Date", "Code", "Name", "Exdividend", "CashDividend", "StockDividendRatio"],
}  # 欄位是英文；Date 是除權息日（民國年）。預告表只含近期，所以每天存快照累積歷史

SOURCES["exdiv_otc"] = {  # 上櫃股票除權除息預告表（櫃買中心），欄位名稱與上市不同
    "base": TPEX_OPENAPI,
    "path": "/tpex_exright_prepost",
    "json": "tpex_exright_prepost.json",
    "timeout": 60,
    "need": ["ExRrightsExDividendDate", "SecuritiesCompanyCode", "CompanyName", "ExRrightsExDividend",
             "CashDividend", "StockDividendRatio"],
}
# 欄位對照：(除權息日, 代號, 名稱, 除權或除息, 現金股利, 股票股利)
EXDIV_FIELDS = {
    "exdiv": ("Date", "Code", "Name", "Exdividend", "CashDividend", "StockDividendRatio"),
    "exdiv_otc": ("ExRrightsExDividendDate", "SecuritiesCompanyCode", "CompanyName", "ExRrightsExDividend",
                  "CashDividend", "StockDividendRatio"),
}
# 選用資料集：失敗只記錄，不讓整次更新算失敗（避免它出問題時連上市資料都上傳不了）
OPTIONAL_SOURCES = {"exdiv_otc", "meetings_otc", "dividends_otc"}

SOURCES["meetings_otc"] = {  # 上櫃公司召開股東常 (臨時) 會日期、地點及採用電子投票情形等資料彙總表
    "base": TPEX_OPENAPI,
    "path": "/t187ap41_O",
    "json": "t187ap41_O.json",
    "timeout": 60,
    "need": ["出表日期", "公司代號", "公司名稱", "股東常(臨時)會", "開會日期", "是否採電子投票", "股務單位"],
}
# 舊的 mopsfin_t187ap39_O（上櫃股利分派情形-董事會通過）最新決議日停在 2021-08-03，已停更，不採用。
# 現行資料是 data.gov.tw 資料集 58285「上櫃股利分派情形」（每日更新），欄位與上市 t187ap45_L 相同，只有 CSV 下載網址。
SOURCES["dividends_otc"] = {
    "csv_url": "https://mopsfin.twse.com.tw/opendata/t187ap45_O.csv",
    "csv": "t187ap45_O.csv",
    "path": "/opendata/t187ap45_O",
    "timeout": 60,
    "need": ["出表日期", "公司代號", "公司名稱", "決議（擬議）進度", "股利年度",
             "股利所屬年(季)度", "期別", "董事會（擬議）股利分派日", "股東會日期",
             "股東配發-盈餘分配之現金股利(元/股)",
             "股東配發-法定盈餘公積發放之現金(元/股)",
             "股東配發-資本公積發放之現金(元/股)"],
}

SOURCES["quotes"] = {
    "path": "/exchangeReport/STOCK_DAY_ALL",
    "json": "STOCK_DAY_ALL.json",
    "market": "TWSE",
    "map": {"date": "Date", "code": "Code", "name": "Name", "close": "ClosingPrice", "open": "OpeningPrice",
            "high": "HighestPrice", "low": "LowestPrice", "change": "Change", "volume": "TradeVolume",
            "value": "TradeValue", "txn": "Transaction"},
}  # 上市個股日成交資訊（含 ETF）；欄位是英文；非即時，只有收盤後的資料
SOURCES["quotes_otc"] = {
    "base": TPEX_OPENAPI,
    "path": "/tpex_mainboard_daily_close_quotes",
    "json": "tpex_mainboard_daily_close_quotes.json",
    "market": "TPEX",
    "timeout": 60,  # 上櫃資料量較大
    "map": {"date": "Date", "code": "SecuritiesCompanyCode", "name": "CompanyName", "close": "Close",
            "open": "Open", "high": "High", "low": "Low", "change": "Change", "volume": "TradingShares",
            "value": "TransactionAmount", "txn": "TransactionNumber"},
}  # 上櫃股票每日收盤行情（櫃買中心）；欄位名稱與上市不同，用 map 對應到同一張表
for _s in ("quotes", "quotes_otc"):
    SOURCES[_s]["need"] = list(SOURCES[_s]["map"].values())

SCHEMA = """
CREATE TABLE IF NOT EXISTS meetings(
  code TEXT, name TEXT, kind TEXT, meet_date TEXT, evote TEXT, agent TEXT, asof TEXT,
  PRIMARY KEY(code, kind, meet_date));
CREATE TABLE IF NOT EXISTS dividends(
  code TEXT, name TEXT, progress TEXT, div_year TEXT, period TEXT, term TEXT,
  board_date TEXT, meet_date TEXT, cash_per_share REAL, asof TEXT,
  PRIMARY KEY(code, div_year, period, term, board_date));
CREATE TABLE IF NOT EXISTS exdiv_notices(
  code TEXT, ex_date TEXT, name TEXT, kind TEXT, cash_per_share REAL, stock_ratio REAL,
  etf_guess INTEGER, first_seen TEXT, last_seen TEXT, PRIMARY KEY(code, ex_date));
CREATE TABLE IF NOT EXISTS prices(
  code TEXT, trade_date TEXT, name TEXT, close REAL, open REAL, high REAL, low REAL, change REAL,
  volume INTEGER, value INTEGER, txn INTEGER, fetched TEXT, market TEXT, PRIMARY KEY(code, trade_date));
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, at TEXT, rows_in INTEGER,
  rows_kept INTEGER, bad_dates INTEGER, flagged INTEGER, status TEXT);
CREATE TABLE IF NOT EXISTS meetings_otc(
  code TEXT, name TEXT, kind TEXT, meet_date TEXT, evote TEXT, agent TEXT, asof TEXT,
  PRIMARY KEY(code, kind, meet_date));
CREATE TABLE IF NOT EXISTS dividends_otc(
  code TEXT, name TEXT, progress TEXT, div_year TEXT, period TEXT, term TEXT,
  board_date TEXT, meet_date TEXT, cash_per_share REAL, asof TEXT,
  PRIMARY KEY(code, div_year, period, term, board_date));
"""


# ---------- 正規化 ----------
def roc_to_iso(s):
    """民國年 1150604 -> 2026-06-04；無法解析回傳 None。"""
    s = (s or "").strip()
    if not s.isdigit() or len(s) not in (6, 7):
        return None
    try:
        return datetime.date(int(s[:-4]) + 1911, int(s[-4:-2]), int(s[-2:])).isoformat()
    except ValueError:
        return None


def num(s):
    """金額轉數字並四捨五入到 4 位（7.00000137 -> 7.0）；空值回傳 None。"""
    s = (s or "").strip().replace(",", "")
    if s in ("", "-", "--"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def clean(s):
    return re.sub(r"<[^>]+>", "", (s or "")).strip()


STATS = {}  # 各資料集這次略過的筆數，給主程式顯示
DERIV_WORDS = ("購", "售", "牛", "熊")


def is_warrant(code, name):
    """上櫃權證：代號 6 碼、前兩碼 70 到 73，且名稱含購、售、牛、熊。
    名稱條件是保險：萬一有不符合的商品，寧可保留也不要悄悄丟掉。"""
    return (len(code) == 6 and code[:2] in ("70", "71", "72", "73")
            and any(w in name for w in DERIV_WORDS))


# ---------- 取得資料 ----------
def ssl_context():
    """Python 3.13 起預設啟用嚴格的 X.509 檢查，證交所的憑證缺少 Subject Key Identifier 會被擋。
    這裡只關閉這一項嚴格檢查；憑證鏈與網站名稱仍然完整驗證，不是停用驗證。"""
    ctx = ssl.create_default_context()
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


def fetch_rows(name, offline_dir):
    src = SOURCES[name]
    if offline_dir:
        if "csv" in src:
            with open(pathlib.Path(offline_dir) / src["csv"], encoding="utf-8-sig", newline="") as f:
                return list(csv.DictReader(f))
        with open(pathlib.Path(offline_dir) / src["json"], encoding="utf-8") as f:
            return json.load(f)
    if "csv_url" in src:  # 這份資料沒有 OpenAPI 端點，只有 data.gov.tw 列出的 CSV 下載網址
        req = urllib.request.Request(src["csv_url"], headers={"User-Agent": "dividend-butler/0.1", "Accept": "text/csv"})
        with urllib.request.urlopen(req, timeout=src.get("timeout", 30), context=ssl_context()) as resp:
            rows = list(csv.DictReader(io.StringIO(resp.read().decode("utf-8-sig"), newline="")))
        if not rows:
            raise RuntimeError(f"{name}: CSV 沒有資料")
        return rows
    req = urllib.request.Request(src.get("base", OPENAPI) + src["path"],
                                 headers={"User-Agent": "dividend-butler/0.1", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=src.get("timeout", 30), context=ssl_context()) as resp:  # 每個資料集每天只抓一次
        rows = json.loads(resp.read().decode("utf-8"))
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise RuntimeError(f"{name}: OpenAPI 回傳格式不是預期的清單")
    return rows


def check_columns(name, rows):
    if not rows:
        raise RuntimeError(f"{name}: 回傳 0 筆資料，不寫入")
    missing = [c for c in SOURCES[name]["need"] if c not in rows[0]]
    if missing:
        raise RuntimeError(f"{name}: 缺少欄位 {missing}；實際欄位：{list(rows[0].keys())}")


# ---------- 各資料集 ----------
def norm_meetings(rows):
    asof = roc_to_iso(rows[0]["出表日期"])
    out, bad = [], 0
    for r in rows:
        d = roc_to_iso(r["開會日期"])
        bad += d is None
        out.append((r["公司代號"].strip(), r["公司名稱"].strip(), r["股東常(臨時)會"].strip(), d,
                    r["是否採電子投票"].strip(), r["股務單位"].strip(), asof))
    return out, bad, 0


def norm_dividends(rows):
    asof = roc_to_iso(rows[0]["出表日期"])
    out, bad, flagged = [], 0, 0
    for r in rows:
        parts = [num(r["股東配發-盈餘分配之現金股利(元/股)"]),
                 num(r["股東配發-法定盈餘公積發放之現金(元/股)"]),
                 num(r["股東配發-資本公積發放之現金(元/股)"])]
        cash = round(sum(p for p in parts if p is not None), 4) if any(p is not None for p in parts) else None
        if cash is not None and not (0 <= cash <= 1000):
            flagged += 1
        b = roc_to_iso(r["董事會（擬議）股利分派日"])
        bad += (b is None)
        b = b or ""  # 主鍵欄位不能是 NULL，否則重複執行會產生重複資料；空字串代表日期不明
        m = roc_to_iso(r["股東會日期"])
        out.append((r["公司代號"].strip(), r["公司名稱"].strip(), clean(r["決議（擬議）進度"]),
                    r["股利年度"].strip(), r["股利所屬年(季)度"].strip(), r["期別"].strip(),
                    b, m, cash, asof))
    return out, bad, flagged


def norm_dividends_otc(rows):
    """上櫃股利分派情形-董事會通過。股東會日期那一欄的欄位名稱在櫃買資料裡接了多餘的字，所以用「開頭是股東會日期」去找。"""
    out, bad, flagged = [], 0, 0
    for r in rows:
        asof = roc_to_iso(r["出表日期"])
        parts = [num(r["股東配發內容-盈餘分配之現金股利(元/股)"]),
                 num(r.get("股東配發內容-法定盈餘公積、資本公積發放之現金(元/股)"))]
        cash = round(sum(p for p in parts if p is not None), 4) if any(p is not None for p in parts) else None
        if cash is not None and not (0 <= cash <= 1000):
            flagged += 1
        b = roc_to_iso(r["董事會決議通過股利分派日"])
        bad += (b is None)
        b = b or ""
        mkey = next((k for k in r if k.startswith("股東會日期")), None)
        m = roc_to_iso(r[mkey]) if mkey else None
        term = (r["期別"] or "").strip()
        out.append((r["公司代號"].strip(), r["公司名稱"].strip(), "董事會通過", r["股利年度"].strip(),
                    ("期別" + term) if term else "", term, b, m, cash, asof))
    return out, bad, flagged


NORMALIZERS = {"meetings": (norm_meetings, "meetings", 7),
               "dividends": (norm_dividends, "dividends", 10),
               "meetings_otc": (norm_meetings, "meetings_otc", 7),
               "dividends_otc": (norm_dividends, "dividends_otc", 10)}


def load(db, name, rows):
    check_columns(name, rows)
    if name in ("exdiv", "exdiv_otc"):
        return load_exdiv(db, rows, name)
    if name in ("quotes", "quotes_otc"):
        return load_quotes(db, name, rows)
    fn, table, ncol = NORMALIZERS[name]
    data, bad, flagged = fn(rows)
    kept_before = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if kept_before and len(data) < kept_before * MIN_KEEP_RATIO:
        db.execute("INSERT INTO runs(source,at,rows_in,rows_kept,bad_dates,flagged,status) VALUES(?,?,?,?,?,?,?)",
                   (name, datetime.datetime.now().isoformat(timespec="seconds"), len(rows), kept_before, bad, flagged,
                    "REJECTED: 筆數驟降"))
        db.commit()
        raise RuntimeError(f"{name}: 新資料 {len(data)} 筆，少於上次 {kept_before} 筆的 70%，不覆蓋")
    marks = ",".join("?" * ncol)
    db.executemany(f"INSERT OR REPLACE INTO {table} VALUES({marks})", data)
    kept = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    db.execute("INSERT INTO runs(source,at,rows_in,rows_kept,bad_dates,flagged,status) VALUES(?,?,?,?,?,?,?)",
               (name, datetime.datetime.now().isoformat(timespec="seconds"), len(rows), kept, bad, flagged, "OK"))
    db.commit()
    return len(rows), kept, bad, flagged


def load_exdiv(db, rows, name="exdiv"):
    """除息預告：以 (代號, 除息日) 為鍵累積，不刪除舊資料，first_seen 保留第一次看到的日期。
    預告表筆數會隨季節大幅變動，所以不用「筆數驟降」檢查，只擋掉空資料。"""
    today = datetime.date.today().isoformat()
    if not rows:
        raise RuntimeError("exdiv: 回傳 0 筆，不寫入")
    data, bad = [], 0
    for r in rows:
        fd = EXDIV_FIELDS[name]
        d = roc_to_iso(r[fd[0]])
        code = (r[fd[1]] or "").strip()
        if d is None or not code:
            bad += 1
            continue
        data.append((code, d, clean(r[fd[2]]), (r[fd[3]] or "").strip().replace("除", ""),
                     num(r[fd[4]]), num(r[fd[5]]),
                     1 if code.startswith("00") else 0, today, today))  # 00 開頭只是 ETF 的粗略判斷
    db.executemany("""INSERT INTO exdiv_notices(code, ex_date, name, kind, cash_per_share, stock_ratio,
                        etf_guess, first_seen, last_seen) VALUES(?,?,?,?,?,?,?,?,?)
                      ON CONFLICT(code, ex_date) DO UPDATE SET name=excluded.name, kind=excluded.kind,
                        cash_per_share=excluded.cash_per_share, stock_ratio=excluded.stock_ratio,
                        etf_guess=excluded.etf_guess, last_seen=excluded.last_seen""", data)
    kept = db.execute("SELECT COUNT(*) FROM exdiv_notices").fetchone()[0]
    db.execute("INSERT INTO runs(source,at,rows_in,rows_kept,bad_dates,flagged,status) VALUES(?,?,?,?,?,?,?)",
               (name, datetime.datetime.now().isoformat(timespec="seconds"), len(rows), kept, bad, 0, "OK"))
    db.commit()
    return len(rows), kept, bad, 0


def load_quotes(db, name, rows):
    """收盤價：以 (代號, 交易日) 為鍵累積，同一天重複抓會覆蓋；上市與上櫃共用一張表。
    筆數低於同一資料集上一次讀入筆數的 70% 視為異常，不寫入。"""
    cfg = SOURCES[name]
    m = cfg["map"]
    prev = db.execute("SELECT rows_in FROM runs WHERE source=? AND status='OK' ORDER BY id DESC LIMIT 1",
                      (name,)).fetchone()
    if prev and len(rows) < prev[0] * MIN_KEEP_RATIO:
        db.execute("INSERT INTO runs(source,at,rows_in,rows_kept,bad_dates,flagged,status) VALUES(?,?,?,?,?,?,?)",
                   (name, datetime.datetime.now().isoformat(timespec="seconds"), len(rows), prev[0], 0, 0,
                    "REJECTED: 筆數驟降"))
        db.commit()
        raise RuntimeError(f"{name}: 新資料 {len(rows)} 筆，少於上次 {prev[0]} 筆的 70%，不寫入")
    today = datetime.date.today().isoformat()
    data, bad, flagged, skipped = [], 0, 0, 0
    for r in rows:
        d = roc_to_iso(r[m["date"]])
        code = (r[m["code"]] or "").strip()
        if d is None or not code:
            bad += 1
            continue
        if is_warrant(code, clean(r[m["name"]])):
            skipped += 1  # 權證不是這個 App 要追蹤的商品，不存
            continue
        close = num(r[m["close"]])
        if close is not None and not (0 < close < 100000):
            flagged += 1
            close = None  # 不合理的價格不收，App 顯示待確認
        vol, val, txn = num(r[m["volume"]]), num(r[m["value"]]), num(r[m["txn"]])
        data.append((code, d, clean(r[m["name"]]), close, num(r[m["open"]]), num(r[m["high"]]),
                     num(r[m["low"]]), num(r[m["change"]]),
                     int(vol) if vol is not None else None, int(val) if val is not None else None,
                     int(txn) if txn is not None else None, today, cfg["market"]))
    db.executemany("INSERT OR REPLACE INTO prices VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
    STATS[name] = skipped
    kept = db.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    db.execute("INSERT INTO runs(source,at,rows_in,rows_kept,bad_dates,flagged,status) VALUES(?,?,?,?,?,?,?)",
               (name, datetime.datetime.now().isoformat(timespec="seconds"), len(rows), kept, bad, flagged, "OK"))
    db.commit()
    return len(rows), kept, bad, flagged


def migrate(db):
    """舊版資料庫的 prices 表沒有 market 欄位，補上。"""
    cols = [r[1] for r in db.execute("PRAGMA table_info(prices)")]
    if "market" not in cols:
        db.execute("ALTER TABLE prices ADD COLUMN market TEXT")
        db.execute("UPDATE prices SET market='TWSE' WHERE market IS NULL")
        db.commit()
    # 清掉早期版本已經存進去的權證（可重複執行）
    cur = db.execute("DELETE FROM prices WHERE length(code)=6 AND substr(code,1,2) IN ('70','71','72','73') "
                     "AND (name LIKE '%購%' OR name LIKE '%售%' OR name LIKE '%牛%' OR name LIKE '%熊%')")
    db.commit()
    if cur.rowcount:
        print(f"已清除資料庫裡早期存入的權證 {cur.rowcount} 筆")
        db.execute("VACUUM")


# ---------- App 取用的查詢 ----------
def meeting_for(db, code):
    """回傳已公告的股東會日期；沒有就回傳 None，App 顯示「待確認」，不自行推測。"""
    row = db.execute("SELECT meet_date, kind, evote, agent FROM meetings WHERE code=? "
                     "ORDER BY meet_date DESC LIMIT 1", (code,)).fetchone()
    return row


def export_json(db, path):
    """把資料庫整理成 App 讀的 JSON：每檔證券一筆，含最新收盤價、最近配息、股東會、除息預告。
    沒有的資料就是 null 或空陣列，App 顯示待確認，不補值。"""
    securities = {}

    def sec(code, name, market=None):
        if code not in securities:
            securities[code] = {"code": code, "name": name, "market": market, "close": None, "closeDate": None,
                                "change": None, "dividends": [], "meeting": None, "exdiv": []}
        s_ = securities[code]
        if market and not s_["market"]:
            s_["market"] = market
        return s_

    for code, name, market, d, close, change in db.execute(
            "SELECT p.code, p.name, p.market, p.trade_date, p.close, p.change FROM prices p "
            "JOIN (SELECT code, MAX(trade_date) AS d FROM prices GROUP BY code) m "
            "ON p.code = m.code AND p.trade_date = m.d"):
        e = sec(code, name, market)
        e["close"], e["closeDate"], e["change"] = close, d, change
    for code, name, progress, year, period, term, board, cash in db.execute(
            "SELECT code, name, progress, div_year, period, term, board_date, cash_per_share FROM "
            "(SELECT * FROM dividends UNION ALL SELECT * FROM dividends_otc) "
            "WHERE board_date != '' ORDER BY code, board_date DESC"):
        e = sec(code, name)
        if len(e["dividends"]) < 4:
            e["dividends"].append({"year": year, "period": period, "term": term, "boardDate": board,
                                   "cash": cash, "progress": progress})
    for code, name, kind, d, evote, agent in db.execute(
            "SELECT code, name, kind, meet_date, evote, agent FROM "
            "(SELECT * FROM meetings UNION ALL SELECT * FROM meetings_otc) WHERE meet_date IS NOT NULL "
            "ORDER BY code, meet_date DESC"):
        e = sec(code, name)
        if e["meeting"] is None:
            e["meeting"] = {"date": d, "kind": kind, "evote": evote, "agent": agent}
    for code, name, kind, d, cash in db.execute(
            "SELECT code, name, kind, ex_date, cash_per_share FROM exdiv_notices ORDER BY code, ex_date DESC"):
        e = sec(code, name)
        if len(e["exdiv"]) < 5:
            e["exdiv"].append({"date": d, "kind": kind, "cash": cash})

    if len(securities) < 500:
        raise RuntimeError(f"匯出中止：只有 {len(securities)} 檔證券，資料庫看起來是空的")
    pd = dict(db.execute("SELECT market, MAX(trade_date) FROM prices GROUP BY market").fetchall())
    out = {"meta": {"exportedAt": datetime.datetime.now().isoformat(timespec="seconds"),
                    "attribution": ATTRIBUTION, "priceDates": pd, "count": len(securities)},
           "securities": sorted(securities.values(), key=lambda x: x["code"])}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    return len(securities), pathlib.Path(path).stat().st_size


def latest_close(db, code):
    """回傳 (交易日, 收盤價)；沒有就回傳 None。App 必須同時顯示交易日，並標示非即時。"""
    return db.execute("SELECT trade_date, close FROM prices WHERE code=? AND close IS NOT NULL "
                      "ORDER BY trade_date DESC LIMIT 1", (code,)).fetchone()


def demo(db):
    print("\n== 查詢範例 ==", ATTRIBUTION)
    for code in ["2330", "2317", "2454", "2412", "9999"]:
        m = meeting_for(db, code)
        divs = db.execute("SELECT name, progress, div_year, period, board_date, cash_per_share FROM dividends "
                          "WHERE code=? ORDER BY board_date DESC LIMIT 2", (code,)).fetchall()
        print(f"\n[{code}] 股東會：{m if m else '待確認（資料集裡沒有）'}")
        for d in divs:
            print("   配息：", d)
        if not divs:
            print("   配息：待確認（資料集裡沒有）")
    pr = db.execute("SELECT COUNT(DISTINCT code), MIN(trade_date), MAX(trade_date) FROM prices").fetchone()
    if pr[0]:
        mk = db.execute("SELECT market, COUNT(DISTINCT code) FROM prices GROUP BY market").fetchall()
        print(f"\n收盤價（非即時，只有收盤後資料）：{pr[0]} 檔（{dict(mk)}），交易日 {pr[1]} 到 {pr[2]}")
        for code in ["2330", "2317", "00400A", "00411A", "9999"]:
            lc = latest_close(db, code)
            print(f"  [{code}]", f"{lc[0]} 收盤 {lc[1]}" if lc else "待確認（資料集裡沒有）")
    n = db.execute("SELECT COUNT(*), MIN(ex_date), MAX(ex_date) FROM exdiv_notices").fetchone()
    if n[0]:
        etf = db.execute("SELECT COUNT(*) FROM exdiv_notices WHERE etf_guess=1").fetchone()[0]
        print(f"\n除息預告：庫內共 {n[0]} 筆，除息日 {n[1]} 到 {n[2]}，其中代號 00 開頭（可能是 ETF）{etf} 筆")
        for r in db.execute("SELECT ex_date, code, name, kind, cash_per_share FROM exdiv_notices "
                            "WHERE ex_date>=? ORDER BY ex_date, code LIMIT 8", (datetime.date.today().isoformat(),)):
            print("  ", r)
    print("\n最近的 5 場股東會（今天起）：")
    today = datetime.date.today().isoformat()
    for r in db.execute("SELECT meet_date, code, name, evote FROM meetings WHERE meet_date>=? "
                        "ORDER BY meet_date LIMIT 5", (today,)):
        print("  ", r)
    if not db.execute("SELECT 1 FROM meetings WHERE meet_date>=? LIMIT 1", (today,)).fetchone():
        print("   （目前沒有今天以後已公告的股東會，明年的要等公司公告後才會出現）")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="dividend.db")
    ap.add_argument("--offline", help="含 t187ap41_L.csv、t187ap45_L.csv 的資料夾")
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--export", metavar="路徑", help="跑完後把資料匯出成 App 讀的 JSON，例如 C:\\dev\\dividend-butler\\app-data.json")
    a = ap.parse_args()
    db = sqlite3.connect(a.db)
    db.executescript(SCHEMA)
    migrate(db)
    failed = False
    for name in SOURCES:
        try:
            n_in, kept, bad, flagged = load(db, name, fetch_rows(name, a.offline))
            extra = f"  略過權證 {STATS[name]}" if STATS.get(name) else ""
            print(f"{name:10s} 讀入 {n_in:5d}  庫內 {kept:5d}  日期無法解析 {bad:4d}  金額異常 {flagged}{extra}")
        except FileNotFoundError:
            print(f"{name:10s} 略過：離線資料夾沒有這個資料集的檔案")
        except Exception as e:  # 任何一個資料集失敗都不影響另一個，也不覆蓋舊資料
            if name not in OPTIONAL_SOURCES:
                failed = True
            print(f"{name:10s} 失敗：{e}", file=sys.stderr)
    if a.demo:
        demo(db)
    if a.export:
        try:
            n, size = export_json(db, a.export)
            print(f"\n已匯出 {n} 檔證券到 {a.export}（{size / 1024:.0f} KB）")
        except Exception as e:
            failed = True
            print(f"匯出失敗：{e}", file=sys.stderr)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
