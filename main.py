# -*- coding: utf-8 -*-
"""
YouTube Plus⁺ (yt-plus.next) — backend

[ytnext] 刷新の要点 (詳細は PATCH-NOTES.md)
  1. 完全 async 化 (httpx + keep-alive 接続プール)
     旧: 同期 requests + リクエスト毎に ThreadPoolExecutor(N スレッド)生成 → 同時接続で枯渇。
  2. instance の遅延/失敗をスコアリングしてレーシング、失敗枠はクールダウンで回避。
     旧: 全枠を同時ヒット + 死に枠を末尾に回すだけ、全滅時は 10 秒張り付き。
     旧旧実装の「formatStreams[0] を実際に落として検証」も廃止(=YouTube 直アクセス)。
  3. 多層キャッシュ = per-key TTL + stale-while-revalidate + single-flight。
     同一動画/同一検索の同時リクエストは上流へ 1 回だけ。全滅時は古い値で耐える。
  4. サーバから YouTube への直接アクセスをゼロに (IP ban 環境対策)
     - 配信 URL は local=true で張った instance 経由に組み替え
     - サムネ/アイコンは instance 経由 + キャッシュ + SVG プレースホルダ
     - 検索サジェストは Google 直叩き → Invidious API に変更
     - yt-dlp ルートは「別 instance で組み直し」に置換(yt-dlp 自体は使わない)
  5. 視聴ページ: 映像/音声の正しいペアリング(DASH)、実データ由来の画質一覧、字幕、
     再生位置の復元、instance 自動フェイルオーバー。
  6. 追加 API: /api/v/{id} /api/search /api/suggest /next /api/instances /api/stats
     /api/health /thumbnail /proxyimg /captions /p/{mode}
"""
from __future__ import annotations

import ast
import asyncio
import json
import gzip as _gzip
import logging
import os
import random
import re
import subprocess
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import Any, Optional, Union

import httpx
from fastapi import Cookie, FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.responses import RedirectResponse as redirect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from cache import ByteCache, TTLCache, acache, spawn

try:  # orjson があると上流 JSON のパースが 3-5 倍速くなる(無い場合は標準 library で動く)
    import orjson as _orjson
except Exception:  # noqa: BLE001
    _orjson = None


def _loads(text) -> Any:
    if _orjson is not None:
        return _orjson.loads(text)
    return json.loads(text)


def _dumps(obj, **kw) -> str:
    if _orjson is not None and not kw:
        return _orjson.dumps(obj, option=_orjson.OPT_NON_STR_KEYS).decode()
    return json.dumps(obj, **kw)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _apath(*parts: str) -> str:
    """リポジトリ相対のパス解決(起動 cwd に依存しない)。"""
    return os.path.join(BASE_DIR, *parts)

# --------------------------------------------------------------------------
# 設定 — すべて環境変数で上書き可能(コードを触らずに調整できる)
# --------------------------------------------------------------------------
def _f(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return float(default)


def _i(name: str, default: int) -> int:
    return int(_f(name, default))


def _b(name: str, default: int) -> bool:
    return _i(name, default) != 0


INSTANCE_SOURCE = os.environ.get(
    "YPP_INSTANCE_SOURCE",
    "https://raw.githubusercontent.com/yuto1106110/invidious-instance-dieu-eviter/"
    "refs/heads/main/data/valid.json",
)
INSTANCE_FILE = os.environ.get("YPP_INSTANCES_FILE", "")  # 自前運用/テスト用にローカル JSON も可

CFG = {
    # 上流タイムアウト・並列度
    "t_connect": _f("YPP_T_CONNECT", 2.0),
    "t_read": _f("YPP_T_READ", 4.5),
    "max_time": _f("YPP_MAX_WAIT", 7.0),
    "race": _i("YPP_RACE", 6),
    # 上流(公開 instance)への同時実行数の上限。0 で無制限。
    # 公開 instance を叩きすぎると self-ban されるため、既定で 48 に張る(=p95 も改善)。
    "upstream_conc": _i("YPP_UPSTREAM_CONCURRENCY", 48),
    "max_conn": _i("YPP_MAX_CONN", 256),
    # instance 選別
    "refresh": _f("YPP_INSTANCE_REFRESH", 900),
    "refresh_fail": _f("YPP_INSTANCE_REFRESH_FAIL", 45),
    "fail_streak": _i("YPP_FAIL_STREAK", 2),
    "cooldown": _f("YPP_FAIL_COOLDOWN", 60),
    # キャッシュ
    "ttl_api": _f("YPP_TTL_API", 60),
    "stale_api": _f("YPP_STALE_API", 21600),
    "ttl_video": _f("YPP_TTL_VIDEO", 150),
    "ttl_search": _f("YPP_TTL_SEARCH", 600),
    "ttl_channel": _f("YPP_TTL_CHANNEL", 900),
    "ttl_comments": _f("YPP_TTL_COMMENTS", 900),
    "ttl_trending": _f("YPP_TTL_TRENDING", 1200),
    "ttl_suggest": _f("YPP_TTL_SUGGEST", 1800),
    "ttl_thumb": _f("YPP_TTL_THUMB", 86400),
    "cache_api": _i("YPP_CACHE_API", 192),
    "cache_video": _i("YPP_CACHE_VIDEO", 192),
    "cache_search": _i("YPP_CACHE_SEARCH", 384),
    "cache_channel": _i("YPP_CACHE_CHANNEL", 128),
    "cache_comments": _i("YPP_CACHE_COMMENTS", 384),
    # 配信・画像の扱い
    "local_stream": _b("YPP_LOCAL_STREAM", 1),   # 1: 配信を instance 経由にする
    # 「サーバだけ YouTube に触れない」が前提なので、**ブラウザ側**は直リンクして良い。
    # 1 = サムネ/アイコンは元の CDN URL をそのまま渡す(自サーバの中継負荷・メモリ 0)。
    # 学校側でも塞がれている場合はテンプレの onerror が /thumbnail 中継へ自動でFall back する。
    # 明示的に 0 にすると全画像を自サーバ中継に切り替える(旧 YPP_ALLOW_YT_DIRECT も兼ねる)。
    "img_direct": _b("YPP_IMG_DIRECT", 0 if os.environ.get("YPP_ALLOW_YT_DIRECT") else 1),
    # 0 = /thumbnail /proxyimg のサーバ側リレーが i.ytimg.com を**取りに行かない**(既定/BAN 対策)。
    "thumb_yt_fetch": _b("YPP_THUMB_YT_FETCH", 0),
    # 中継する画像キャッシュは「フォールバック用」として小さく保つ(無料枠はメモリが薄い)。
    "thumb_items": _i("YPP_THUMB_ITEMS", 200),
    "thumb_mb": _i("YPP_THUMB_MB", 6),
    # 無料プラン(Render/Railway 等)は寝る → 起動後に再構築コストを出さない為の状態ファイル
    "state_file": os.environ.get("YPP_STATE_FILE", "/tmp/ypp_state.json"),
    "state_ttl": _f("YPP_STATE_TTL", 21600.0),
    "stream_path": os.environ.get("YPP_STREAM_PATH", "/latest_version"),
    # その他
    "debug": _b("YPP_DEBUG", 0),
    "gzip_min": _i("YPP_GZIP_MIN", 512),
    # starlette の既定は 9 で、それだけで 1 リクエスト 2 ms 以上かかる。6 にしても
    # サイズは 1% しか変わらないので、速い方を選ぶ。
    "gzip_level": _i("YPP_GZIP_LEVEL", 6),
    "static_max_age": _i("YPP_STATIC_MAX_AGE", 604800),
    "region": os.environ.get("YPP_REGION", "JP"),
    "lang": os.environ.get("YPP_LANG", "ja"),
    "results_per_page": _i("YPP_RESULTS_PER_PAGE", 20),
}

version = "1.1.0-next"
new_instance_version = "1.3.2"

logging.basicConfig(level=logging.DEBUG if CFG["debug"] else logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ytnext")


def _dbg(*args) -> None:
    if CFG["debug"]:
        log.debug(" ".join(str(a) for a in args))


user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]


def getRandomUserAgent() -> dict:
    """UA を 1 つ選ぶだけ。旧実装は選出のたびに print していた(ログ I/O が重い)。"""
    return {"User-Agent": random.choice(user_agents)}


# --------------------------------------------------------------------------
# 例外
# --------------------------------------------------------------------------
class APITimeoutError(Exception):
    """全ての instance が応答しなかった。"""


class NotFoundError(Exception):
    """動画/チャンネル自体が存在しない(instance の不調ではない)。"""


class UnallowedBot(Exception):
    pass


# --------------------------------------------------------------------------
# 入力サニタイズ / 表示フォーマッタ
# --------------------------------------------------------------------------
_ID_RE = re.compile(r"^[A-Za-z0-9_@.\-]{1,80}$")
_VID_RE = re.compile(r"^[A-Za-z0-9_\-]{11}$")


def clean_id(value: str, maxlen: int = 80) -> str:
    value = (value or "").strip()
    if not value or len(value) > maxlen or not _ID_RE.match(value):
        return ""
    return value


def clean_vid(value: str) -> str:
    value = (value or "").strip()
    return value if _VID_RE.match(value) else ""


def isJSON(json_str) -> bool:
    try:
        json.loads(json_str)
        return True
    except Exception:
        return False


def checkCookie(cookie) -> bool:
    return cookie == "True"


def formatViewCount(count) -> str:
    try:
        count = int(count)
    except (TypeError, ValueError):
        return str(count)
    if count >= 100000000:
        return f"{count / 100000000:.2f}".rstrip("0").rstrip(".") + "億"
    if count >= 10000:
        return f"{count // 10000}万"
    if count >= 1000:
        return f"{count:,}"
    return str(count)


def formatPublished(timestamp) -> str:
    try:
        diff = int(time.time()) - int(timestamp)
    except (TypeError, ValueError):
        return ""
    if diff < 0:
        return "予約投稿"
    steps = ((60, 1, "秒"), (3600, 60, "分"), (86400, 3600, "時間"), (86400 * 7, 86400, "日"),
             (86400 * 30, 86400 * 7, "週間"), (86400 * 365, 86400 * 30, "ヶ月"))
    for limit, div, unit in steps:
        if diff < limit:
            return "たった今" if unit == "秒" else f"{diff // div}{unit}前"
    return f"{diff // (86400 * 365)}年前"


_SUB_RE = re.compile(r"^\s*([\d.,]+)\s*(k|m|b|万|億|万|千)?", re.IGNORECASE)
_SUB_MULT = {"K": 1000, "M": 1000000, "B": 1000000000, "千": 1000, "万": 10000, "億": 100000000}


def convertSubCount(text) -> str:
    """"500K subscribers" / "50.2万人" / "1,234" など雑多な表記を人数化する。"""
    if text in (None, ""):
        return ""
    m = _SUB_RE.match(str(text))
    if not m or not m.group(1):
        return str(text)
    try:
        value = float(m.group(1).replace(",", ""))
    except ValueError:
        return str(text)
    unit = (m.group(2) or "").upper()
    return formatViewCount(int(value * _SUB_MULT.get(unit, 1))) + "人"


def formatDuration(seconds) -> str:
    try:
        seconds = int(seconds or 0)
    except (TypeError, ValueError):
        return "0:00"
    h, rem = divmod(max(0, seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def getInfo(request: Request) -> str:
    return json.dumps([version, os.environ.get("RENDER_EXTERNAL_URL", ""), request.url.path], ensure_ascii=False)


# --------------------------------------------------------------------------
# instance プール(一覧 + 死活/レイテンシ)
# --------------------------------------------------------------------------
DEFAULT_INSTANCES = {
    "video": ["https://invidious.f5.si/", "https://invidious.ritoge.com/", "https://iv.catgirl.cloud/",
              "https://invidious.lunivers.trade/", "https://yt.omada.cafe/"],
    "search": ["https://invidious.f5.si/", "https://invidious.ritoge.com/", "https://iv.catgirl.cloud/",
               "https://invidious.lunivers.trade/", "https://yt.omada.cafe/"],
    "channel": ["https://invidious.f5.si/", "https://invidious.ritoge.com/", "https://iv.catgirl.cloud/",
                "https://yt.omada.cafe/"],
    "playlist": ["https://invidious.f5.si/", "https://invidious.ritoge.com/", "https://iv.catgirl.cloud/",
                 "https://yt.omada.cafe/"],
    "comments": ["https://invidious.f5.si/", "https://invidious.ritoge.com/", "https://iv.catgirl.cloud/",
                 "https://invidious.lunivers.trade/", "https://yt.omada.cafe/"],
}
KINDS = tuple(DEFAULT_INSTANCES.keys())


def _norm_origin(raw: str) -> str:
    raw = str(raw or "").strip().strip("'\"")
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw if raw.endswith("/") else raw + "/"


class InstancePool:
    def __init__(self):
        self.data: dict = {k: list(v) for k, v in DEFAULT_INSTANCES.items()}
        self.health: dict = {}
        self.updated = 0.0
        self.next_refresh = 0.0
        self.source = "builtin"
        self.error = ""
        self._rot = {k: 0 for k in KINDS}
        self._refreshing = False
        self._lock = asyncio.Lock()
        self._hosts: set = set()
        self._rebuild_hosts()

    # -- 内部 ----------------------------------------------------------
    def _rebuild_hosts(self) -> None:
        hosts = set()
        for kind in KINDS:
            for o in self.data.get(kind, []):
                host = urllib.parse.urlsplit(o).netloc.lower()
                if host:
                    hosts.add(host)
        self._hosts = hosts

    def _score(self, origin: str, idx: int, now: float) -> tuple:
        h = self.health.get(origin)
        penalized = 1 if (h and h["penalty"] > now) else 0
        lat = h["lat"] if (h and h["ok"]) else 0.4
        streak = h["streak"] if h else 0
        return penalized, lat + 0.8 * streak + 0.02 * idx + random.random() * 0.05

    # -- 公開 ----------------------------------------------------------
    def candidates(self, kind: str, limit: int = 0) -> list:
        origins = self.data.get(kind) or self.data.get("video") or []
        if not origins:
            return []
        now = time.time()
        scored = sorted(((self._score(o, i, now), o) for i, o in enumerate(origins)), key=lambda x: x[0])
        usable = [s for s in scored if s[0][0] == 0]
        out = [o for _, o in (usable or scored)]
        n = self._rot.get(kind, 0) % len(out)
        self._rot[kind] = self._rot.get(kind, 0) + 1
        out = out[n:] + out[:n]
        return out[:limit] if limit else out

    def all_origins(self, limit: int = 0) -> list:
        seen, out = set(), []
        for kind in KINDS:
            for o in self.candidates(kind):
                if o not in seen:
                    seen.add(o)
                    out.append(o)
        return out[:limit] if limit else out

    def allowlist(self) -> set:
        return self._hosts

    def record(self, origin: str, ok: bool, latency: float = 0.0) -> None:
        h = self.health.setdefault(origin, {"ok": 0, "fail": 0, "streak": 0, "penalty": 0.0, "lat": 0.0})
        if ok:
            h["ok"] += 1
            h["streak"] = 0
            h["penalty"] = 0.0
            h["lat"] = latency if not h["lat"] else h["lat"] * 0.7 + latency * 0.3
        else:
            h["fail"] += 1
            h["streak"] += 1
            h["lat"] = (h["lat"] or 1.0) * 0.9 + max(latency, 1.0) * 0.1
            if h["streak"] >= CFG["fail_streak"]:
                h["penalty"] = time.time() + CFG["cooldown"]

    def rotate(self, kind: str) -> list:
        """旧 /api/{name}/next 互換: 先頭を末尾へ回す。"""
        lst = self.data.get(kind)
        if lst and len(lst) > 1:
            lst.append(lst.pop(0))
        return lst or []

    # -- 一覧更新 ------------------------------------------------------
    @staticmethod
    def parse(text: str) -> dict:
        obj = None
        try:
            obj = json.loads(text)
        except Exception:
            try:  # upstream は Python リテラル(' で囲む)なので両方受けつける
                obj = ast.literal_eval(text)
            except Exception:
                return {}
        if isinstance(obj, list):
            cleaned = [_norm_origin(x) for x in obj]
            cleaned = [o for o in dict.fromkeys(cleaned) if o]
            return {k: cleaned for k in KINDS} if cleaned else {}
        if not isinstance(obj, dict):
            return {}
        out = {}
        for k in KINDS:
            v = obj.get(k)
            if isinstance(v, list) and v:
                vals = [_norm_origin(x) for x in v if isinstance(x, (str, int, float))]
                out[k] = [o for o in dict.fromkeys(vals) if o]
        base = out.get("video") or next((v for v in out.values() if v), [])
        if not base:
            return {}
        for k in KINDS:  # 欠けたカテゴリは video 枠で埋める(旧実装は KeyError になっていた)
            out.setdefault(k, list(base))
        return out

    async def load_text(self) -> str:
        if INSTANCE_FILE:
            try:
                with open(INSTANCE_FILE, encoding="utf-8") as f:
                    self.source = INSTANCE_FILE
                    return f.read()
            except OSError as exc:
                self.error = f"file: {exc}"
        if INSTANCE_SOURCE:
            text = await _raw_get(INSTANCE_SOURCE)
            if text:
                self.source = INSTANCE_SOURCE
            return text
        return ""

    async def refresh(self, force: bool = False) -> bool:
        now = time.time()
        if self._refreshing:
            return False
        if not force and now < self.next_refresh:
            return False
        self._refreshing = True
        try:
            parsed = self.parse(await self.load_text())
            if parsed and any(parsed.get(k) for k in KINDS):
                async with self._lock:
                    self.data = {k: (parsed.get(k) or self.data.get(k, [])) for k in KINDS}
                self._rebuild_hosts()
                self.error = ""
                self.next_refresh = now + CFG["refresh"]
                _save_state()
                return True
            self.error = self.error or "一覧が空/解析失敗(前回値を維持)"
            self.next_refresh = now + CFG["refresh_fail"]
            return False
        except Exception as exc:  # noqa: BLE001
            self.error = repr(exc)
            self.next_refresh = now + CFG["refresh_fail"]
            return False
        finally:
            self._refreshing = False
            self.updated = time.time()
            # 一覧取得に失敗しても(=組み込み一覧で動く場合)死活結果は残す。
            # 無料プランの寝起きは、この health こそが効く。
            _save_state()

    def info(self) -> dict:
        now = time.time()
        return {
            "source": self.source,
            "updated": int(self.updated),
            "next_refresh": int(self.next_refresh),
            "error": self.error,
            "counts": {k: len(v) for k, v in self.data.items()},
            "order": {k: self.candidates(k, 5) for k in KINDS},
            "health": {
                o: {"ok": h["ok"], "fail": h["fail"], "latency_ms": round(h["lat"] * 1000),
                    "cooldown_left": max(0, int(h["penalty"] - now))}
                for o, h in sorted(self.health.items())
            },
        }


pool = InstancePool()


# --------------------------------------------------------------------------
# プールの状態をディスクへ退避(無料プランは寝る → 起動直後に「どれが生きていたか」を復元する)
#   /tmp は Render/Railway/Koyeb の無料枠で使える ephemeral disk なので、依存も設定も増やさない。
#   YPP_STATE_FILE="" で無効化。YPP_STATE_TTL(秒)より古いファイルは無視する。
# --------------------------------------------------------------------------
def _save_state() -> None:
    path = CFG["state_file"]
    if not path:
        return
    now = time.time()
    payload = {
        "saved": int(now),
        "instances": {k: list(v) for k, v in pool.data.items()},
        "health": {
            o: {"ok": h["ok"], "fail": h["fail"], "lat": round(h["lat"], 3),
                "penalty_left": int(max(0.0, min(h["penalty"] - now, CFG["cooldown"])))}
            for o, h in pool.health.items()
        },
    }
    try:
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(_dumps(payload))
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    except Exception as exc:  # noqa: BLE001  読み取り専用 FS / 権限なし 等は無視
        _dbg("state save skipped", exc)


def _load_state() -> bool:
    """復元できたら True。True の間は起動時に上流へ一覧を依頼しなくてよい。"""
    path = CFG["state_file"]
    if not path:
        return False
    try:
        with open(path, encoding="utf-8") as f:
            saved = _loads(f.read())
    except Exception:
        return False
    if not isinstance(saved, dict):
        return False
    if time.time() - float(saved.get("saved") or 0) > CFG["state_ttl"]:
        return False
    parsed = InstancePool.parse(_dumps(saved.get("instances") or {}))
    if not parsed:
        return False
    pool.data = {k: (parsed.get(k) or pool.data.get(k, [])) for k in KINDS}
    pool._rebuild_hosts()
    now = time.time()
    for o, h in (saved.get("health") or {}).items():
        if not isinstance(h, dict):
            continue
        pool.health[o] = {"ok": int(h.get("ok") or 0), "fail": int(h.get("fail") or 0),
                          "streak": 0, "lat": float(h.get("lat") or 0.0),
                          "penalty": now + float(h.get("penalty_left") or 0)}
    pool.updated = float(saved.get("saved") or 0)
    pool.next_refresh = now + CFG["refresh"]
    pool.source = f"state:{path}"
    return True


# --------------------------------------------------------------------------
# HTTP クライアント(keep-alive プール)
# --------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_IMG_CLIENT: Optional[httpx.AsyncClient] = None


def _new_client(read_timeout: float, keepalive: int) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        limits=httpx.Limits(max_connections=CFG["max_conn"],
                           max_keepalive_connections=min(80, CFG["max_conn"]),
                           keepalive_expiry=float(keepalive)),
        timeout=httpx.Timeout(CFG["t_connect"], read=read_timeout, write=10.0, pool=1.5),
        follow_redirects=True,
    )


def client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = _new_client(CFG["t_read"], 30)
    return _CLIENT


def img_client() -> httpx.AsyncClient:
    global _IMG_CLIENT
    if _IMG_CLIENT is None or _IMG_CLIENT.is_closed:
        _IMG_CLIENT = _new_client(3.5, 25)
    return _IMG_CLIENT


async def _raw_get(url: str, timeout: Optional[float] = None) -> str:
    try:
        r = await client().get(url, headers=getRandomUserAgent(),
                               timeout=httpx.Timeout(CFG["t_connect"], read=timeout or CFG["t_read"]))
        return r.text if r.status_code == 200 else ""
    except Exception as exc:  # noqa: BLE001
        _dbg("raw_get failed", url, repr(exc))
        return ""


# --------------------------------------------------------------------------
# 上流 API: 並列レーシング + キャッシュ + 失敗記録
# --------------------------------------------------------------------------
_neg_cache = TTLCache(maxsize=1024, ttl=300.0)
_UP_SEM = asyncio.Semaphore(CFG["upstream_conc"]) if CFG["upstream_conc"] > 0 else None


async def _limited_get(url: str, **kw):
    if _UP_SEM is None:
        return await client().get(url, **kw)
    async with _UP_SEM:
        return await client().get(url, **kw)
_STATS = {"html": 0, "api": 0, "img": 0, "up_ok": 0, "up_fail": 0, "up_ms": 0.0}
_START = time.time()


def _shape_check(kind: str, path: str, data: Any) -> None:
    """「この instance は正常に応答した」かを軽量に判定する。

    旧実装は formatStreams[0] を実際にダウンロードして検証していた
    (= YouTube 直アクセス + 1 リクエストあたり最大 1.5 秒の無駄な追加遅延)。
    """
    if isinstance(data, dict) and data.get("error"):
        err = str(data["error"]).lower()
        if any(w in err for w in ("not found", "unavailable", "private", "removed", "does not exist", "no such")):
            raise NotFoundError(str(data["error"])[:180])
        raise RuntimeError(f"upstream error: {err[:80]}")
    if "fields=" in path:  # 限定向け部分レスポンスは形状チェックを省略
        return
    if kind == "video" and "/videos/" in path:
        if isinstance(data, dict):
            if data.get("formatStreams") or data.get("adaptiveFormats") or data.get("hlsUrl") \
                    or data.get("liveNow") or data.get("isUpcoming") or data.get("title"):
                return
            raise RuntimeError("映像ストリームなし")
    if kind == "channel" and path.startswith("/channels/"):
        if isinstance(data, list):
            return
        if isinstance(data, dict) and (data.get("author") or data.get("authorId")
                                       or data.get("latestVideos") or data.get("latestvideo")
                                       or data.get("videos") or data.get("content")):
            return
        raise RuntimeError("チャンネル情報なし")
    if kind == "search" and path.startswith("/search?"):
        if not isinstance(data, list):
            raise RuntimeError("検索結果の形式が不正")


@acache(ttl=CFG["ttl_api"], maxsize=CFG["cache_api"], stale_ttl=CFG["stale_api"])
async def _api_cached(path: str, kind: str) -> dict:
    neg = _neg_cache.get((kind, path))
    if neg:
        raise NotFoundError(neg)

    origins = pool.candidates(kind, CFG["race"]) or pool.all_origins(CFG["race"])
    if not origins:
        await pool.refresh(True)
        origins = pool.candidates(kind, CFG["race"])
        if not origins:
            raise APITimeoutError("利用可能なAPIがありません")

    queue: asyncio.Queue = asyncio.Queue()

    async def attempt(origin: str) -> None:
        t0 = time.perf_counter()
        try:
            res = await _limited_get(origin + "api/v1" + path, headers=getRandomUserAgent(),
                                     timeout=httpx.Timeout(CFG["t_connect"], read=CFG["t_read"]))
            dt = time.perf_counter() - t0
            if res.status_code == 404:  # 動画/チャンネル側の問題 → instance は罰しない
                pool.record(origin, True, dt)
                queue.put_nowait(("nf", NotFoundError("upstream 404")))
                return
            if res.status_code != 200:
                raise RuntimeError(f"HTTP {res.status_code}")
            data = _loads(res.text)
            _shape_check(kind, path, data)
        except NotFoundError as exc:
            pool.record(origin, True, time.perf_counter() - t0)
            queue.put_nowait(("nf", exc))
            return
        except json.JSONDecodeError as exc:
            pool.record(origin, False, time.perf_counter() - t0)
            _dbg("bad json", origin, path)
            queue.put_nowait(("err", exc))
            return
        except Exception as exc:  # noqa: BLE001
            pool.record(origin, False, time.perf_counter() - t0)
            _dbg("api error", origin, path, repr(exc))
            queue.put_nowait(("err", exc))
            return
        _STATS["up_ok"] += 1
        _STATS["up_ms"] += (time.perf_counter() - t0) * 1000
        queue.put_nowait(("ok", {"data": data, "origin": origin,
                                "ms": round((time.perf_counter() - t0) * 1000, 1)}))

    tasks = [asyncio.ensure_future(attempt(o)) for o in origins]
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CFG["max_time"]
    result: Optional[dict] = None
    not_found: Optional[NotFoundError] = None
    pending = len(tasks)
    while pending:  # 全滅したら即座に抜ける(旧実装のような張り付き待ちをなくす)
        try:
            tag, item = await asyncio.wait_for(queue.get(), timeout=max(0.02, deadline - loop.time()))
        except asyncio.TimeoutError:
            break
        pending -= 1
        if tag == "ok":
            result = item
            break
        if tag == "nf" and not_found is None:
            not_found = item

    for t in tasks:
        if not t.done():
            t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if result is None:
        _STATS["up_fail"] += 1
        if not_found is not None:
            _neg_cache.set((kind, path), str(not_found))
            raise not_found
        raise APITimeoutError("APIがタイムアウトしました")
    return result


async def api(path: str, kind: str) -> dict:
    """{"data":..., "origin":..., "ms":...} を返す。全滅時は acache の stale が古い値を返す。"""
    await pool.refresh()
    return await _api_cached(path, kind)


async def api_opt(path: str, kind: str) -> Optional[Any]:
    """失敗時に None を返す方（ページ描画で部分失敗を許容する場所用)。"""
    try:
        return (await api(path, kind))["data"]
    except NotFoundError:
        return None
    except APITimeoutError:
        return None


# --------------------------------------------------------------------------
# URL / メディア処理
# --------------------------------------------------------------------------
def _host_of(url: str) -> str:
    try:
        return urllib.parse.urlsplit(url).netloc.lower()
    except ValueError:
        return ""


def local_stream_url(origin: str, video_id: str, itag) -> str:
    """指定 instance の中継エンドポイントを直接組み立てる(別枠フォールバック用)。"""
    if not origin or not video_id or itag in (None, ""):
        return ""
    return (f"{origin.rstrip('/')}{CFG['stream_path']}?id={urllib.parse.quote(str(video_id))}"
            f"&local=true&itag={urllib.parse.quote(str(itag))}")


def media_url(url: str, origin: str, *, video_id: str = "", itag=None, local: bool = True) -> str:
    """instance が返した URL をブラウザが使える形に整える。

    * 相対 URL       → instance の絶対 URL 化(旧実装の再生不能バグの主因)
    * instance 自有  → そのまま(自サーバの中継負荷ゼロ)
    * googlevideo    → local モードなら instance 中継 URL に組み替えて YouTube 直アクセスを回避
    """
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("/"):
        return origin.rstrip("/") + url
    if origin and _host_of(url) == _host_of(origin):
        return url
    if local and video_id and itag not in (None, ""):
        return local_stream_url(origin, video_id, itag)
    return url


_ALLOWED_IMG_HOSTS = {
    "i.ytimg.com", "img.youtube.com", "s.ytimg.com",
    "yt3.googleusercontent.com", "yt3.ggpht.com", "lh3.googleusercontent.com",
}


def img_url(raw: str, origin: str, video_id: str, quality: str = "mqdefault") -> str:
    """サムネイル/アイコン URL を **ブラウザ側**で使える形に組み立てる。

    制約は「サーバが YouTube を直接叩かない」事だけなので、既定(img_direct=1)では
    元々の CDN URL をそのまま返す=自サーバは何もしない(無料枠に優しい)。
    ブラウザ側でも塞がれている場合だけ onerror → /thumbnail(自前中継)が効く。

    * instance 相対   → origin を付けてそのまま(自サーバの中継負荷ゼロ・最速)
    * instance 自有   → そのまま
    * YouTube 系 CDN  → 動画なら /thumbnail(instance の /vi/ を使う)、
                        そうでなければ /proxyimg(instance 経由 → だめなら SVG 画像)
    """
    raw = (raw or "").strip()
    if not raw:
        return f"/thumbnail?v={video_id}&q={quality}" if video_id else ""
    if raw.startswith("/"):
        return (origin.rstrip("/") + raw) if origin else (
            f"/thumbnail?v={video_id}&q={quality}" if video_id else "")
    if raw.startswith("//"):
        raw = "https:" + raw
    host = _host_of(raw)
    if host and (host == _host_of(origin) or host in pool.allowlist()):
        return raw
    if host in _ALLOWED_IMG_HOSTS:
        if CFG["img_direct"]:
            # https デプロイで mixed content にならないよう、CDN 系は必ず https に寄せる
            return raw.replace("http://", "https://", 1) if raw.startswith("http://") else raw
        if video_id and "/vi/" in raw:
            return f"/thumbnail?v={video_id}&q={quality}"
        return "/proxyimg?u=" + urllib.parse.quote(raw, safe="")
    # 見知らぬホストはブラウザに指示しない(悪意/故障した instance がトラッキング先や
    # リダイレクト先を仕込んでも、/proxyimg の許可リストで止めて placeholder に見せる)。
    return "/proxyimg?u=" + urllib.parse.quote(raw, safe="") if host else raw


def _thumb_from(item: dict, origin: str, video_id: str, quality: str = "mqdefault") -> str:
    arr = item.get("videoThumbnails") or item.get("thumbnails") or item.get("authorThumbnails") or []
    best, best_w = "", -1
    if isinstance(arr, list):
        for t in arr:
            u = t.get("url", "") if isinstance(t, dict) else str(t)
            w = int(t.get("width") or 0) if isinstance(t, dict) else 0
            if u and w >= best_w and (quality != "hqdefault" or w >= 300):
                best, best_w = u, w
    return img_url(best, origin, video_id, quality)


def _author_icon(item: dict, origin: str) -> str:
    arr = item.get("authorThumbnails") or []
    raw = ""
    if isinstance(arr, list) and arr:
        last = arr[-1]
        raw = last.get("url", "") if isinstance(last, dict) else str(last)
    return img_url(raw, origin, "")


# --------------------------------------------------------------------------
# 動画
# --------------------------------------------------------------------------
def _res_num(stream: dict) -> int:
    for key in ("resolution", "qualityLabel"):
        v = stream.get(key)
        if v:
            m = re.match(r"(\d+)", str(v))
            if m:
                return int(m.group(1))
    try:
        return int(stream.get("height") or 0)
    except (TypeError, ValueError):
        return 0


def _bitrate(stream: dict) -> int:
    try:
        return int(stream.get("bitrate") or stream.get("averageBitrate") or 0)
    except (TypeError, ValueError):
        return 0


def _quality_label(stream: dict) -> str:
    lab = stream.get("qualityLabel") or stream.get("resolution") or ""
    if not lab:
        n = _res_num(stream)
        lab = f"{n}p" if n else "自動"
    return str(lab)


def build_stream_list(t: dict, origin: str, local: bool) -> dict:
    """adaptiveFormats / formatStreams から画質一覧と音声ペアを組み立てる。

    旧実装は webm「映像のみ」を最高画質に選びながら audio_url を常に None にしたまま
    (=高音質モードで声が出ない)、formatStreams は最下位 2 本を先頭に出していた。
    """
    vid = str(t.get("videoId") or "")
    vids, progs, audios = [], [], []
    for s in (t.get("adaptiveFormats") or []):
        if not isinstance(s, dict) or not s.get("url"):
            continue
        vcodec, acodec = str(s.get("vcodec") or "none"), str(s.get("acodec") or "none")
        if vcodec != "none" and acodec == "none":
            vids.append(s)
        elif vcodec != "none" and acodec != "none":
            progs.append(s)
        elif vcodec == "none" and acodec != "none":
            audios.append(s)
    for s in (t.get("formatStreams") or []):
        if isinstance(s, dict) and s.get("url"):
            progs.append(s)

    vids.sort(key=lambda x: (-_res_num(x), -_bitrate(x)))
    progs.sort(key=lambda x: (-_res_num(x), -_bitrate(x)))
    audios.sort(key=lambda x: -_bitrate(x))
    best_audio = audios[0] if audios else None
    fallback_prog = progs[0] if progs else None

    def pack(s: dict, *, dash: bool) -> Optional[dict]:
        raw = s.get("url", "")
        url = media_url(raw, origin, video_id=vid, itag=s.get("itag"), local=local)
        if not url:
            return None
        item = {
            "url": url,
            "url_direct": media_url(raw, origin, video_id=vid, itag=s.get("itag"), local=False),
            "resolution": _quality_label(s),
            "height": _res_num(s),
            "label": _quality_label(s) + (" DASH" if dash else " 音声込"),
            "type": "dash" if dash else "progressive",
            "itag": s.get("itag"),
            "container": s.get("container") or "",
            "codec": s.get("videoCodec") or s.get("encoding") or "",
            "bitrate": _bitrate(s),
            "fallbacks": [],
        }
        if dash:
            a = best_audio or fallback_prog
            if a:
                item["audio_url"] = media_url(a.get("url", ""), origin, video_id=vid,
                                               itag=a.get("itag"), local=local)
        return item

    seen, quality_streams = set(), []
    for s in vids[:8]:
        it = pack(s, dash=True)
        if it and (it["resolution"], it["type"]) not in seen:
            seen.add((it["resolution"], it["type"]))
            quality_streams.append(it)
    for s in progs[:6]:
        it = pack(s, dash=False)
        if it and (it["resolution"], "progressive") not in seen:
            seen.add((it["resolution"], "progressive"))
            quality_streams.append(it)

    # 1 つの instance が落ちても再生を続けられるよう、別 instance 経由 / 直 URL をフォールバックに持つ
    alts = [o for o in pool.candidates("video", 4) if o.rstrip("/") != origin.rstrip("/")][:2]
    for it in quality_streams:
        if it.get("itag"):
            for alt in alts:
                u = local_stream_url(alt, vid, it["itag"])
                if u:
                    it["fallbacks"].append(u)
        direct = it.pop("url_direct", "")
        if local and direct and direct != it["url"]:
            it["fallbacks"].append(direct)  # instance 中継が全滅した時の最後の砦(回線が YT に開いていれば再生可)

    hls = t.get("hlsUrl") or ""
    if hls:
        hls = media_url(hls, origin, video_id=vid, local=local)
        if local and origin and _host_of(hls) != _host_of(origin) and "manifest" not in hls:
            hls = ""  # 中継できない googlevideo マニフェストは出さない

    audio_url = ""
    if best_audio:
        audio_url = media_url(best_audio.get("url", ""), origin, video_id=vid,
                              itag=best_audio.get("itag"), local=local)
    progressive_urls = [i["url"] for i in quality_streams if i["type"] == "progressive"][:3]
    if not progressive_urls and fallback_prog:
        progressive_urls = [media_url(fallback_prog.get("url", ""), origin, video_id=vid,
                                      itag=fallback_prog.get("itag"), local=local)]

    dash_first = quality_streams[0] if quality_streams and quality_streams[0]["type"] == "dash" else None
    return {
        "quality_streams": quality_streams,
        "highstream_url": dash_first["url"] if dash_first else "",
        "highstream_audio": dash_first.get("audio_url", "") if dash_first else "",
        "audio_url": audio_url,
        "video_urls": progressive_urls,
        "hlsUrl": hls,
        "origin": origin,
    }


def _captions(t: dict, video_id: str) -> list:
    out = []
    for c in (t.get("captions") or t.get("subtitles") or [])[:30]:
        if not isinstance(c, dict):
            continue
        label = c.get("label") or c.get("name") or ""
        if not label:
            continue
        out.append({
            "label": str(label),
            "language": str(c.get("language_code") or c.get("language") or ""),
            "auto": bool(c.get("autoGenerated", c.get("auto_generated", False))),
            "url": f"/captions?v={video_id}&label={urllib.parse.quote(str(label))}",
        })
    return out


@acache(ttl=CFG["ttl_video"], maxsize=CFG["cache_video"], stale_ttl=3600)
async def video_payload(video_id: str, local: bool) -> dict:
    path = f"/videos/{urllib.parse.quote(video_id)}?hl={CFG['lang']}&gl={CFG['region']}"
    if local:
        path += "&local=true"  # 配信 URL を instance 経由にさせる
    res = await api(path, "video")
    return {"t": res["data"], "origin": res["origin"], "local": local}


async def getVideoData(videoid: str, local: bool = True) -> tuple:
    pack = await video_payload(videoid, local)
    t, origin = pack["t"], pack["origin"]
    if not isinstance(t, dict):
        raise APITimeoutError("動画情報を取得できませんでした")
    streams = build_stream_list(t, origin, local)
    vid = str(t.get("videoId") or videoid)
    recommended = t.get("recommendedVideos") or t.get("recommendedvideo") or []

    rec = []
    for i in recommended:
        if not isinstance(i, dict):
            continue
        rec.append({
            "video_id": i.get("videoId", ""),
            "title": i.get("title", "取得失敗"),
            "author_id": i.get("authorId", ""),
            "author": i.get("author", ""),
            "length_text": formatDuration(i.get("lengthSeconds", 0)),
            "view_count_text": (formatViewCount(i.get("viewCount")) if i.get("viewCount") is not None
                                else i.get("viewCountText", "")),
            "published_text": (formatPublished(i["published"]) if i.get("published")
                               else i.get("publishedText", "")),
            "thumbnail": _thumb_from(i, origin, str(i.get("videoId") or "")),
            "is_short": 0 < int(i.get("lengthSeconds") or 0) <= 60,
        })
        if len(rec) >= 24:
            break

    info = {
        "video_urls": streams["video_urls"],
        "highstream_url": streams["highstream_url"],
        "audio_url": streams["highstream_audio"] or streams["audio_url"],
        "quality_streams": streams["quality_streams"],
        "hlsUrl": streams["hlsUrl"],
        "captions": _captions(t, vid),
        "description_html": (t.get("descriptionHtml") or t.get("description") or "").replace("\n", "<br>"),
        "title": t.get("title", ""),
        "video_id": vid,
        "length_text": formatDuration(t.get("lengthSeconds", 0)),
        "length_seconds": int(t.get("lengthSeconds") or 0),
        "author_id": t.get("authorId", ""),
        "author": t.get("author", ""),
        "author_thumbnails_url": _author_icon(t, origin),
        "view_count": t.get("viewCount", 0),
        "view_count_text": formatViewCount(t.get("viewCount", 0)),
        "published_text": formatPublished(t.get("published", 0)) or t.get("publishedText", ""),
        "like_count": t.get("likeCount", 0),
        "comment_count": t.get("commentCount", 0),
        "subscribers_count": (convertSubCount(t.get("subCountText")) if t.get("subCountText")
                              else (formatViewCount(t.get("subCount")) + "人" if t.get("subCount") else "")),
        "is_live": bool(t.get("liveNow")),
        "is_upcoming": bool(t.get("isUpcoming")),
        "paid": bool(t.get("paid")),
        "category": t.get("category") or "",
        "thumb": f"/thumbnail?v={vid}&q=hqdefault",
        "thumb_direct": _thumb_from(t, origin, vid, "hqdefault"),
        "source_api": origin,
        "local": local,
        "chapters": [
            {"title": str(c.get("name") or c.get("title") or ""), "seconds": int(c.get("timeStart") or c.get("start") or 0)}
            for c in (t.get("chapters") or [])[:60] if isinstance(c, dict)
        ],
    }
    return info, rec


# --------------------------------------------------------------------------
# 検索
# --------------------------------------------------------------------------
SEARCH_TYPES = {"video", "channel", "playlist", "movie", "show", "episode", "all"}
SEARCH_SORTS = {"relevance", "rating", "upload_date", "views"}


@acache(ttl=CFG["ttl_search"], maxsize=CFG["cache_search"], stale_ttl=CFG["stale_api"])
async def search_payload(q: str, page: int, type_: str, sort_by: str, duration: str) -> dict:
    query = {"q": q, "page": page, "hl": CFG["lang"], "gl": CFG["region"], "region": CFG["region"]}
    if type_ in SEARCH_TYPES:
        query["type"] = type_
    if sort_by in SEARCH_SORTS:
        query["sort_by"] = sort_by
    if duration in ("short", "long"):
        query["duration"] = duration
    if CFG["local_stream"]:
        query["local"] = "true"   # サムネイル URL も instance 経由にさせる
    res = await api("/search?" + urllib.parse.urlencode(query), "search")
    return {"items": res["data"], "origin": res["origin"]}


def _format_search_item(d: dict, origin: str) -> Optional[dict]:
    if not isinstance(d, dict):
        return None
    kind = d.get("type") or ("video" if d.get("videoId") else "channel" if d.get("authorId") else "")
    if kind == "video":
        vid = str(d.get("videoId") or "")
        length = int(d.get("lengthSeconds") or 0)
        return {
            "type": "video", "title": d.get("title") or "取得失敗", "id": vid,
            "authorId": d.get("authorId", ""), "author": d.get("author", ""),
            "published": (formatPublished(d["published"]) if d.get("published")
                          else (d.get("publishedText") or "")),
            "length": formatDuration(length), "length_seconds": length,
            "view_count_text": (formatViewCount(d.get("viewCount")) if d.get("viewCount") is not None
                                else d.get("viewCountText", "")),
            "thumb": _thumb_from(d, origin, vid),
            "is_short": 0 < length <= 60,
            "is_live": bool(d.get("liveNow")),
        }
    if kind == "playlist":
        cover = str(d.get("playlistThumbnail") or "")
        if cover.startswith("http"):
            thumb = img_url(cover, origin, "")
        elif _VID_RE.match(cover):
            thumb = f"/thumbnail?v={cover}&q=hqdefault"
        else:
            thumb = ""
        return {
            "type": "playlist", "title": d.get("title") or "取得失敗", "id": str(d.get("playlistId") or ""),
            "count": d.get("videoCount", ""), "author": d.get("author", ""),
            "thumbnail": cover, "thumb": thumb, "length": "",
        }
    if kind == "channel" or d.get("authorId"):
        return {
            "type": "channel", "author": d.get("author") or "取得失敗",
            "id": d.get("authorId", ""), "thumbnail": "",
            "thumb": _author_icon(d, origin),
            "subscribers": convertSubCount(d.get("authorSubCount")) if d.get("authorSubCount") else "",
            "videos": d.get("authorVideos", ""), "length": "",
        }
    return None


async def getSearchData(q: str, page: int = 1, type_: str = "", sort_by: str = "", duration: str = "") -> list:
    pack = await search_payload(q, page, type_, sort_by, duration)
    out = []
    for raw in pack["items"] if isinstance(pack["items"], list) else []:
        item = _format_search_item(raw, pack["origin"])
        if item:
            out.append(item)
    return out


# --------------------------------------------------------------------------
# チャンネル / プレイリスト / コメント / トレンド
# --------------------------------------------------------------------------
def _videos_of(payload: Any) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("videos", "content", "items", "latestVideos", "latestvideo", "playlists", "comments"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    return []


@acache(ttl=CFG["ttl_channel"], maxsize=CFG["cache_channel"], stale_ttl=CFG["stale_api"])
async def channel_payloads(cid: str, sort_by: str) -> dict:
    tail = f"&local=true" if CFG["local_stream"] else ""
    base = f"/channels/{urllib.parse.quote(cid)}?hl={CFG['lang']}&gl={CFG['region']}{tail}"
    paths = {
        "main": base,
        "videos": f"{base}/videos?sort_by={urllib.parse.quote(sort_by)}",
        "shorts": f"{base}/shorts",
        "streams": f"{base}/streams",
        "playlists": f"{base}/playlists",
        "community": f"{base}/community",
    }
    # 旧実装は 6 リクエストを直列待ちしていた → 並列化してページ表示を大幅短縮
    results = await asyncio.gather(*(api(p, "channel") for p in paths.values()), return_exceptions=True)
    out = {}
    for name, r in zip(paths.keys(), results):
        if isinstance(r, BaseException):
            out[name] = {"data": None, "origin": ""}
        else:
            out[name] = r
    return out


async def getChannelData(channelid: str, sort_by: str = "newest") -> list:
    pack = await channel_payloads(channelid, sort_by)
    t = pack["main"]["data"] if isinstance(pack["main"]["data"], dict) else {}
    origin = pack["main"]["origin"] or pack["videos"]["origin"] or ""
    if not t and not any(pack[k]["data"] for k in ("videos", "shorts", "playlists")):
        raise APITimeoutError("チャンネル情報を取得できませんでした")
    cid = str(t.get("authorId") or channelid)

    def make_item(i: dict, *, force_short: bool = False, is_live: bool = False) -> dict:
        length = int(i.get("lengthSeconds") or 0)
        return {
            "type": "video", "title": i.get("title", ""), "id": i.get("videoId", ""),
            "authorId": cid, "author": t.get("author", ""),
            "published": i.get("published", 0),
            "published_text": (formatPublished(i["published"]) if i.get("published")
                               else i.get("publishedText", "")),
            "view_count": i.get("viewCount", 0),
            "view_count_text": formatViewCount(i.get("viewCount", 0)),
            "length_str": formatDuration(length),
            "thumb": _thumb_from(i, origin, str(i.get("videoId") or "")),
            "is_short": force_short or 0 < length <= 60,
            "is_live": is_live or bool(i.get("liveNow")),
        }

    videos = [make_item(i) for i in (_videos_of(pack["videos"]["data"]) or _videos_of(t.get("latestVideos")))
              if isinstance(i, dict)]
    shorts = [make_item(i, force_short=True) for i in _videos_of(pack["shorts"]["data"]) if isinstance(i, dict)]
    streams = [make_item(i, is_live=True) for i in _videos_of(pack["streams"]["data"]) if isinstance(i, dict)]

    playlists = []
    for pl in _videos_of(pack["playlists"]["data"]):
        if not isinstance(pl, dict):
            continue
        cover = str(pl.get("playlistThumbnail") or "")
        playlists.append({
            "id": pl.get("playlistId", ""), "title": pl.get("title", ""),
            "video_count": pl.get("videoCount", 0),
            "thumbnail": (img_url(cover, origin, "") if cover.startswith("http")
                          else (f"/thumbnail?v={cover}&q=hqdefault" if _VID_RE.match(cover) else "")),
        })
    playlists.sort(key=lambda p: -int(p["video_count"] or 0))

    community = []
    for post in _videos_of(pack["community"]["data"]):
        if not isinstance(post, dict):
            continue
        community.append({
            "id": post.get("commentId", ""),
            "content": (post.get("contentHtml") or post.get("contentText") or "").replace("\n", "<br>"),
            "published_text": post.get("publishedText", "") or formatPublished(post.get("published", 0)),
            "likes": formatViewCount(post.get("likeCount", 0)),
            "author": t.get("author", ""),
            "author_icon": _author_icon(t, origin),
        })

    sub_count = t.get("subCount", 0)
    subscribers = (formatViewCount(sub_count) + "人") if sub_count else convertSubCount(t.get("subCountText", ""))
    banner = ""
    if isinstance(t.get("authorBanners"), list) and t["authorBanners"]:
        b0 = t["authorBanners"][0]
        banner = img_url(b0.get("url", "") if isinstance(b0, dict) else str(b0), origin, "")
    total_videos = t.get("totalVideos")

    return [
        videos, shorts, playlists, community, streams,
        {
            "channel_name": t.get("author", "不明なチャンネル"),
            "channel_icon": _author_icon(t, origin),
            "channel_profile": (t.get("descriptionHtml") or "").replace("\n", "<br>"),
            "author_banner": banner,
            "subscribers_count": subscribers,
            "total_videos": (formatViewCount(total_videos) + "本") if total_videos else "",
            "channel_id": cid,
        },
    ]


@acache(ttl=CFG["ttl_channel"], maxsize=CFG["cache_channel"], stale_ttl=CFG["stale_api"])
async def playlist_payload(listid: str, page: int) -> dict:
    path = (f"/playlists/{urllib.parse.quote(listid)}?page={page}&hl={CFG['lang']}&gl={CFG['region']}"
            + ("&local=true" if CFG["local_stream"] else ""))
    res = await api(path, "playlist")
    return {"data": res["data"], "origin": res["origin"]}


async def getPlaylistData(listid: str, page: int = 1) -> tuple:
    pack = await playlist_payload(listid, page)
    t = pack["data"] if isinstance(pack["data"], dict) else {}
    origin = pack["origin"]
    raw = t.get("videos") or []
    out = []
    for i in raw:
        if not isinstance(i, dict):
            continue
        published = i.get("published")
        out.append({
            "type": "video", "title": i.get("title", ""), "id": i.get("videoId", ""),
            "authorId": i.get("authorId", ""), "author": i.get("author", ""),
            "length": formatDuration(i.get("lengthSeconds", 0)),
            "length_seconds": int(i.get("lengthSeconds") or 0),
            "view_count_text": (formatViewCount(i.get("viewCount")) if i.get("viewCount")
                                else i.get("viewCountText", "")),
            "published": (formatPublished(published) if published and published > 1000000000
                          else i.get("publishedText", "")),
            "thumb": _thumb_from(i, origin, str(i.get("videoId") or "")),
        })
    pages = int(t.get("pages") or t.get("pageCount") or 0)
    has_next = (pages > page) if pages else len(raw) >= CFG["results_per_page"]
    meta = {
        "title": t.get("title", ""), "description": (t.get("descriptionHtml") or "").replace("\n", "<br>"),
        "count": t.get("videoCount", len(out)), "author": t.get("author", ""),
        "author_id": t.get("authorId", ""), "updated": t.get("updated", 0), "list": listid,
        "pages": pages or page + (1 if has_next else 0),
    }
    return out, has_next, meta


@acache(ttl=CFG["ttl_comments"], maxsize=CFG["cache_comments"], stale_ttl=CFG["stale_api"])
async def comments_payload(video_id: str, sort_by: str, continuation: str) -> dict:
    query = {"hl": CFG["lang"], "gl": CFG["region"], "sort_by": sort_by}
    if not continuation:
        query["thin_mode"] = "true"  # HTML を捨ててペイロードを小さくする
    if continuation:
        query["continuation"] = continuation
    res = await api(f"/comments/{urllib.parse.quote(video_id)}?" + urllib.parse.urlencode(query), "comments")
    return {"data": res["data"], "origin": res["origin"]}


def _format_comments(payload: Any, origin: str) -> dict:
    data = payload if isinstance(payload, dict) else {}
    comments = []
    for i in data.get("comments") or []:
        if not isinstance(i, dict):
            continue
        body = i.get("contentText") or i.get("contentHtml") or ""
        comments.append({
            "author": i.get("author", ""),
            "authoricon": _author_icon(i, origin),
            "authorid": i.get("authorId", ""),
            "body": body.replace("\n", "<br>"),
            "likes": formatViewCount(i.get("likeCount", 0)),
            "published": i.get("publishedText", "") or formatPublished(i.get("published", 0)),
            "reply_count": i.get("replyCount", 0),
            "is_pinned": bool(i.get("pinned")),
        })
    return {"comments": comments, "continuation": data.get("continuation", "") or "",
            "comment_count": formatViewCount(data.get("commentCount", len(comments)))}


async def getCommentsData(videoid: str, sort_by: str = "top", continuation: str = "") -> dict:
    pack = await comments_payload(videoid, sort_by, continuation)
    return _format_comments(pack["data"], pack["origin"])


TREND_TYPES = {"default", "gaming", "music", "movies", "news", "live", "podcasts", "comedy", "sports", "learning"}


@acache(ttl=CFG["ttl_trending"], maxsize=64, stale_ttl=CFG["stale_api"])
async def trending_payload(category: str, region: str) -> dict:
    cat = category if category in TREND_TYPES else "default"
    path = (f"/trending?region={urllib.parse.quote(region)}&type={urllib.parse.quote(cat)}"
            f"&hl={CFG['lang']}&gl={urllib.parse.quote(region)}"
            + ("&local=true" if CFG["local_stream"] else ""))
    res = await api(path, "search")
    return {"data": res["data"], "origin": res["origin"]}


async def getTrending(category: str = "default", region: str = "") -> list:
    region = region if re.match(r"^[A-Z]{2}$", region or "") else CFG["region"]
    pack = await trending_payload(category, region)
    out = []
    for v in pack["data"] if isinstance(pack["data"], list) else []:
        if not isinstance(v, dict) or not v.get("videoId"):
            continue
        out.append({
            "id": v["videoId"], "title": v.get("title", ""), "author": v.get("author", ""),
            "author_id": v.get("authorId", ""), "length": formatDuration(v.get("lengthSeconds", 0)),
            "published_text": v.get("publishedText", "") or formatPublished(v.get("published", 0)),
            "view_count_text": formatViewCount(v.get("viewCount", 0)),
            "thumbnail": _thumb_from(v, pack["origin"], str(v.get("videoId"))),
        })
    return out


@acache(ttl=CFG["ttl_suggest"], maxsize=1024, stale_ttl=CFG["stale_api"])
async def suggest_payload(keyword: str) -> list:
    """Invidious 経由のサジェスト。旧実装の www.google.com 直叩きは禁止環境で死んでいた。"""
    kw = (keyword or "").strip()
    if not kw:
        return []
    data = await api_opt(f"/search/suggestions?q={urllib.parse.quote(kw)}&region={CFG['region']}", "search")
    if isinstance(data, dict):
        return [str(s) for s in (data.get("suggestions") or []) if s][:12]
    return []


# --------------------------------------------------------------------------
# サムネイル / 画像リレー
# --------------------------------------------------------------------------
THUMB_QUALITIES = {"0", "1", "2", "3", "default", "mqdefault", "hqdefault", "sddefault", "hq720", "maxresdefault"}
# YPP_THUMB_MB=0 で「中継もキャッシュしない(完全パススルー)」にできる
_thumb_cache = ByteCache(max_bytes=max(1, CFG["thumb_mb"]) * 1024 * 1024,
                         max_items=max(8, CFG["thumb_items"]), ttl=CFG["ttl_thumb"])
_thumb_failed: dict = {}
_thumb_stats = {"served": 0, "fetched": 0, "placeholder": 0}
_VI_PATH_RE = re.compile(r"^/vi/([A-Za-z0-9_\-]{11})/([A-Za-z0-9_.\-]{1,40})$")
_IMG_PATH_RE = re.compile(r"^/[A-Za-z0-9_/.*\-@%]{1,240}$")
PLACEHOLDER_SVG = (
    b'<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180">'
    b'<rect width="100%" height="100%" fill="#212121"/>'
    b'<text x="50%" y="54%" fill="#777" font-family="Roboto,Arial,sans-serif" font-size="14" '
    b'text-anchor="middle">no thumbnail</text></svg>'
)


async def _fetch_image(url: str, timeout: float = 3.0) -> Optional[tuple]:
    try:
        r = await img_client().get(url, headers=getRandomUserAgent(),
                                   timeout=httpx.Timeout(CFG["t_connect"], read=timeout))
        ctype = r.headers.get("content-type", "")
        if r.status_code == 200 and r.content and (ctype.startswith("image") or "octet-stream" in ctype):
            return r.content, ctype.split(";")[0]
    except Exception as exc:  # noqa: BLE001
        _dbg("img failed", url, repr(exc))
    return None


async def fetch_thumb(video_id: str, quality: str) -> tuple:
    key = f"thumb:{video_id}:{quality}"
    hit = _thumb_cache.get(key)
    if hit:
        _thumb_stats["served"] += 1
        return hit[0], hit[1], 200
    if _thumb_failed.get(key, 0) > time.time():
        return PLACEHOLDER_SVG, "image/svg+xml", 404

    origins = pool.all_origins(3)
    for origin in origins:
        for path in (f"vi/{video_id}/{quality}.jpg", f"vp/{video_id}/mqdefault.jpg",
                     f"vi/{video_id}/default.jpg"):
            got = await _fetch_image(origin + path)
            if got:
                _thumb_cache.set(key, got)
                _thumb_stats["fetched"] += 1
                return got[0], got[1], 200
    if CFG["thumb_yt_fetch"]:
        got = await _fetch_image(f"https://i.ytimg.com/vi/{video_id}/{quality}.jpg")
        if got:
            _thumb_cache.set(key, got)
            return got[0], got[1], 200
    _thumb_failed[key] = time.time() + 60
    _thumb_stats["placeholder"] += 1
    return PLACEHOLDER_SVG, "image/svg+xml", 404


# --------------------------------------------------------------------------
# 字幕(vtt) — instance が返した相対パスだけを追う(URL  injection / SSRF 対策)
# --------------------------------------------------------------------------
@acache(ttl=CFG["ttl_video"], maxsize=256, stale_ttl=3600)
async def caption_text(video_id: str, label: str) -> str:
    pack = await video_payload(video_id, True)
    t, origin = pack["t"], pack["origin"]
    target = ""
    for c in (t.get("captions") or t.get("subtitles") or []):
        if isinstance(c, dict) and str(c.get("label") or c.get("name") or "") == label:
            target = str(c.get("url") or "")
            break
    if not target:
        return ""
    if target.startswith("/"):
        url = origin.rstrip("/") + target
    elif _host_of(target) == _host_of(origin):
        url = target
    else:
        return ""
    try:
        r = await client().get(url, headers=getRandomUserAgent())
        if r.status_code == 200 and "WEBVTT" in r.text[:500].upper():
            return _rewrite_vtt(r.text, origin)
    except Exception as exc:  # noqa: BLE001
        _dbg("caption failed", repr(exc))
    return ""


def _rewrite_vtt(vtt: str, origin: str) -> str:
    def repl(m):
        u = m.group(2)
        return (m.group(1) + origin.rstrip("/") + u) if u.startswith("/") else m.group(0)

    return re.sub(r'(src="|image:)([^"]+)"', repl, vtt)


# --------------------------------------------------------------------------
# 検証バイナリ(yukiverify)
# --------------------------------------------------------------------------
_VERIFY_PATH = os.environ.get("YPP_VERIFY", _apath("yukiverify"))
_verify_cache = TTLCache(maxsize=4, ttl=60.0)


def getVerifyCode() -> Optional[str]:
    hit = _verify_cache.get("code", "__none__")
    if hit != "__none__":
        return hit
    code = None
    try:
        if os.path.exists(_VERIFY_PATH) and not os.access(_VERIFY_PATH, os.X_OK):
            os.chmod(_VERIFY_PATH, 0o755)  # 起動時の chmod 777 は不要/危険なので実行時に一度だけ
        result = subprocess.run([_VERIFY_PATH], encoding="utf-8", stdout=subprocess.PIPE, timeout=5)
        code = (result.stdout or "").strip() or None
    except Exception as exc:  # noqa: BLE001
        _dbg("verify error", exc)
    _verify_cache.set("code", code)
    return code


# --------------------------------------------------------------------------
# MongoDB(任意接続) + サイトトレンド
# --------------------------------------------------------------------------
MONGODB_URI = os.environ.get("MONGODB_URI")
TREND_API_URL = os.environ.get("YPP_TREND_API", "https://plus-trend-api.vercel.app")
mongo_client = db = trend_collection = None
if MONGODB_URI:
    try:
        from pymongo import MongoClient

        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=1500)
        db = mongo_client["ytplusplus"]
        trend_collection = db["trends"]
    except Exception as exc:  # noqa: BLE001
        log.warning("MongoDB を初期化できませんでした(トレンドは外部 API を使用): %s", exc)
        mongo_client = db = trend_collection = None


def cleanup_old_trends() -> None:
    if trend_collection is None:
        return
    try:
        trend_collection.delete_many({"last_watched": {"$lt": int(time.time()) - 30 * 86400}})
    except Exception:  # noqa: BLE001
        pass


async def report_trend(video_id: str, info: dict) -> None:
    """視聴数を記録する。旧実装は同期 POST で /watch を最大 3 秒止めていた。"""
    payload = {"video_id": video_id, "title": info.get("title", ""), "author": info.get("author", ""),
               "thumbnail": f"/thumbnail?v={video_id}&q=mqdefault", "length": info.get("length_text", "")}
    if trend_collection is not None:
        def _write():
            now = int(time.time())
            trend_collection.update_one(
                {"video_id": video_id},
                {"$inc": {"count": 1}, "$set": {**payload, "last_watched": now},
                 "$setOnInsert": {"first_watched": now}},
                upsert=True,
            )
        try:
            await asyncio.to_thread(_write)
            if random.randint(1, 100) == 1:
                cleanup_old_trends()
            return
        except Exception as exc:  # noqa: BLE001
            _dbg("mongo trend", exc)
    if not TREND_API_URL:
        return
    try:
        await client().post(f"{TREND_API_URL.rstrip('/')}/trend", json=payload,
                            timeout=httpx.Timeout(CFG["t_connect"], read=2.5))
    except Exception as exc:  # noqa: BLE001
        _dbg("remote trend", exc)


# --------------------------------------------------------------------------
# FastAPI アプリ
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    restored = _load_state()
    if not restored:
        await pool.refresh(True)
    log.info("instance 読み込み: %s (source=%s%s)", {k: len(v) for k, v in pool.data.items()},
             pool.source, ", 前回状態を復元" if restored else "")
    task = spawn(_maintenance())
    try:
        yield
    finally:
        if task is not None:
            task.cancel()
        for c in (_CLIENT, _IMG_CLIENT):
            if c is not None and not c.is_closed:
                await c.aclose()


async def _maintenance() -> None:
    while True:
        try:
            await asyncio.sleep(120)
            await pool.refresh()  # refresh() 内で状態を退避する
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001
            await asyncio.sleep(15)


app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=CFG["gzip_min"],
                   compresslevel=CFG["gzip_level"])

_CACHED_PREFIXES = ("/js/", "/css/", "/img/", "/genesis/")


@app.middleware("http")
async def cache_headers(request: Request, call_next):
    """静的アセット/画像に Cache-Control を付与(旧実装はヘッダーなしで毎回取り直し)。"""
    path = request.url.path
    if path.startswith("/api/"):
        _STATS["api"] += 1
    elif path.startswith(("/thumbnail", "/proxyimg")):
        _STATS["img"] += 1
    elif path.endswith(".html") or path in ("/", "/search", "/trending"):
        _STATS["html"] += 1
    response = await call_next(request)
    if path.startswith(_CACHED_PREFIXES):
        response.headers.setdefault("Cache-Control", f"public, max-age={CFG['static_max_age']}, immutable")
    elif path.startswith(("/thumbnail", "/proxyimg")):
        response.headers.setdefault("Vary", "Accept-Encoding")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    return response


app.mount("/js", StaticFiles(directory=_apath("statics", "js")), name="js")
app.mount("/css", StaticFiles(directory=_apath("statics", "css")), name="css")
app.mount("/img", StaticFiles(directory=_apath("statics", "img")), name="img")
app.mount("/genesis", StaticFiles(directory=_apath("blog"), html=True), name="genesis")

_templates = Jinja2Templates(directory=_apath("templates"))
no_robot_meta_tag = '<meta name="robots" content="noindex,nofollow">'


_page_cache = TTLCache(maxsize=_i("YPP_CACHE_PAGE", 256), ttl=_f("YPP_TTL_PAGE", 45.0))
_PAGE_VARY = ("proxy",)


def template(name: str, context: dict, status_code: int = 200):
    request = context.get("request")
    ctx = {k: v for k, v in context.items() if k != "request"}
    return _templates.TemplateResponse(request=request, name=name, context=ctx, status_code=status_code)


def _page_key(name: str, vary: str, request: Request) -> str:
    return f"{name}|{vary}|{request.url.path}?{request.url.query}"


def _page_serve(key: str, ttl: float, accept_encoding: str = ""):
    """キャッシュ済みページをそのまま返す(上流取得も整形も描画も圧縮もしない)。

    圧縮済みバイトも併せて持つので、ヒットしたたびに GZipMiddleware で
    40 KB を圧縮し直す——という一番高くつく作業も起きない。
    """
    hit = _page_cache.get(key) if key else None
    if not hit:
        return None
    body, ctype = hit[0], hit[1]
    gz = hit[2] if len(hit) > 2 else b""
    headers = {"X-Cache": "HIT", "Vary": "Accept-Encoding",
               "Cache-Control": f"public, max-age={int(ttl)}"}
    if gz and "gzip" in (accept_encoding or "").lower():
        headers["Content-Encoding"] = "gzip"
        return Response(content=gz, media_type=ctype, status_code=200, headers=headers)
    return Response(content=body, media_type=ctype, status_code=200, headers=headers)



def page_hit(request: Request, name: str, *, vary: str = "", cacheable: bool = True):
    """ルーティングの一番最初で呼ぶ。ヒットしていれば上流取得も整形も描画もスキップする。"""
    if not cacheable:
        return None
    return _page_serve(_page_key(name, vary, request), _page_cache.ttl,
                       request.headers.get("accept-encoding", ""))


def render_html(request: Request, name: str, ctx: dict, *, status_code: int = 200,
                cacheable: bool = True, ttl: float = 0.0, vary: str = "") -> Response:
    """テンプレ結果をキャッシュして再利用する(同一クエリの再描画コストをゼロにする)。

    vary には配信方式(local / direct)のように「同じ URL でも中身が変わる」要素を入れる。
    """
    ttl = ttl or _page_cache.ttl
    key = ""
    if cacheable and status_code == 200:
        key = _page_key(name, vary, request)
        hit = _page_serve(key, ttl, request.headers.get("accept-encoding", ""))
        if hit:
            return hit
    resp = template(name, ctx, status_code=status_code)
    gz = b""
    if key and getattr(resp, "body", None):
        if len(resp.body) >= CFG["gzip_min"]:
            try:  # 圧縮は 1 TTL に 1 回だけ。ヒット時は生/圧縮済みのバイトを出すだけ。
                gz = _gzip.compress(resp.body, CFG["gzip_level"], mtime=0)
            except Exception:  # noqa: BLE001  圧縮が失敗しても無圧縮で応答できる
                gz = b""
        _page_cache.set(key, (resp.body, resp.media_type, gz), ttl=ttl)
    if key:
        resp.headers["Cache-Control"] = f"public, max-age={int(ttl)}"
        resp.headers["Vary"] = "Accept-Encoding"
        if gz and "gzip" in request.headers.get("accept-encoding", "").lower():
            # 作ってある gzip をそのまま返す(ミドルウェアで二度圧する位なら自分でやる)
            hdrs = {k: v for k, v in resp.headers.items() if k.lower() != "content-length"}
            hdrs["Content-Encoding"] = "gzip"
            resp = Response(content=gz, media_type=resp.media_type, status_code=resp.status_code,
                            background=resp.background, headers=hdrs)
    return resp


def stream_mode(request: Request, proxy: Optional[str] = None) -> bool:
    """True = instance 経由(local / 禁止環境向け) / False = ブラウザから googlevideo 直。"""
    value = (proxy if proxy is not None else request.cookies.get("proxy") or "").strip()
    if value in ("", "True", "true", "local", "1"):
        return bool(CFG["local_stream"])
    if value.lower() in ("off", "direct", "0", "no", "false"):
        return False
    return value.lower() in ("local", "proxy")


# --------------------------------------------------------------------------
# ページ
# --------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def home(response: Response, request: Request, yuki: Union[str, None] = Cookie(None)):
    if checkCookie(request.cookies.get("yuki")):
        response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
        return template("home.html", {"request": request, "version": version})
    return redirect("/genesis")


async def render_video(request: Request, response: Response, video_id: str, proxy: Optional[str],
                        tpl: str, list_id: str = "", start_at: float = 0.0):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
    local = stream_mode(request, proxy)
    hit = page_hit(request, tpl, vary="local" if local else "direct")   # 再表示は上流も整形も飛ばす
    if hit is not None:
        return hit
    try:
        info, rec = await getVideoData(video_id, local)
    except NotFoundError:
        return template("error.html", {"request": request, "context": "この動画は存在しないか、視聴できません"},
                        status_code=404)
    except APITimeoutError:
        return template("apiTimeout.html", {"request": request}, status_code=504)

    spawn(report_trend(video_id, info))
    return render_html(request, tpl, {
        "request": request,
        "videoid": video_id,
        "videourls": info["video_urls"],
        "highstream_url": info["highstream_url"],
        "audio_url": info["audio_url"],
        "quality_streams": info["quality_streams"],
        "hlsUrl": info["hlsUrl"],
        "description": info["description_html"],
        "video_title": info["title"],
        "author_id": info["author_id"],
        "author_icon": info["author_thumbnails_url"],
        "author": info["author"],
        "length_text": info["length_text"],
        "length_seconds": info["length_seconds"],
        "view_count": info["view_count"],
        "view_count_text": info["view_count_text"],
        "published_text": info["published_text"],
        "like_count": info["like_count"],
        "subscribers_count": info["subscribers_count"],
        "comment_count": info["comment_count"],
        "recommended_videos": rec,
        "proxy": "True" if local else "False",
        "proxy_mode": "local" if local else "direct",
        "captions": info["captions"],
        "chapters": info["chapters"],
        "is_live": info["is_live"],
        # 画像はブラウザ側で取りに行かせる(サーバは何もしない=無料枠に優しい)。
        # ただし https デプロイで mixed content にならない物だけ使う(http:// なら自前中継へ)。
        "thumb": (info["thumb_direct"] if info["thumb_direct"].startswith("https://")
                  else info["thumb"] or info["thumb_direct"]),
        "thumb_direct": info["thumb_direct"],
        "resume": start_at,
        "start_at": start_at,
        "list_id": list_id,
        "api_origin": info["source_api"],
        # history.html / hiquo.html が読む形(watchHistory: video_id / channel / length(表示用文言))
        # に合わせて渡す。昔はキー名が不一致で /history に載っていなかった。
        "video_data_json": _dumps({
            "video_id": video_id, "id": video_id, "title": info["title"],
            "channel": info["author"], "author": info["author"], "channel_id": info["author_id"],
            "author_id": info["author_id"], "length": info["length_text"], "length_seconds": info["length_seconds"],
            "thumbnail": info["thumb"], "thumb": info["thumb"],
            "view_count": info["view_count"], "view_count_text": info["view_count_text"],
            "published": info["published_text"], "list": list_id,
        }),
    }, ttl=CFG["ttl_video"], vary="local" if local else "direct")


@app.get("/watch", response_class=HTMLResponse)
async def video(v: str, response: Response, request: Request, yuki: Union[str, None] = Cookie(None),
                proxy: Union[str, None] = Cookie(None), list: Union[str, None] = None,
                t: Union[float, None] = 0):
    vid = clean_vid(v)
    if not vid:
        return template("error.html", {"request": request, "context": "動画IDが不正です"}, status_code=400)
    return await render_video(request, response, vid, proxy, "video.html", clean_id(list or ""), float(t or 0))


@app.get("/w", response_class=HTMLResponse)
async def video_alt(v: str, response: Response, request: Request, yuki: Union[str, None] = Cookie(None),
                    proxy: Union[str, None] = Cookie(None), t: Union[float, None] = 0):
    vid = clean_vid(v)
    if not vid:
        return template("error.html", {"request": request, "context": "動画IDが不正です"}, status_code=400)
    return await render_video(request, response, vid, proxy, "hiquo.html", "", float(t or 0))


@app.get("/search", response_class=HTMLResponse)
async def search(q: str, response: Response, request: Request, page: Union[int, None] = 1,
                 yuki: Union[str, None] = Cookie(None), proxy: Union[str, None] = Cookie(None),
                 type: Union[str, None] = "", sort_by: Union[str, None] = "", duration: Union[str, None] = ""):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
    page = max(1, int(page or 1))
    mode = "local" if stream_mode(request, proxy) else "direct"
    hit = page_hit(request, "search.html", vary=mode)     # 上流を叩かずに返す最速路
    if hit is not None:
        return hit
    try:
        results = await getSearchData(q, page, type or "", sort_by or "", duration or "")
        failed = False
    except APITimeoutError:
        results, failed = [], True
    qs = {"q": q}
    for k, v in (("type", type), ("sort_by", sort_by), ("duration", duration)):
        if v:
            qs[k] = v

    def link(p: int) -> str:
        return "/search?" + urllib.parse.urlencode({**qs, "page": p})

    return render_html(request, "search.html", {
        "request": request, "results": results, "word": q, "page": page,
        "next": link(page + 1) if len(results) >= CFG["results_per_page"] else None,
        "prev": link(page - 1) if page > 1 else None,
        "proxy": "True" if stream_mode(request, proxy) else "False",
        "type": type or "", "sort_by": sort_by or "", "duration": duration or "",
        "failed": failed,
        "api_next": link(page + 1).replace("/search?", "/api/search?") if len(results) >= CFG["results_per_page"] else None,
    }, ttl=CFG["ttl_search"], vary=mode,
       cacheable=bool(results) and not failed)


@app.get("/hashtag/{tag}")
async def hashtag(tag: str, request: Request, yuki: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    return redirect("/search?" + urllib.parse.urlencode({"q": tag.lstrip("#")}))


@app.get("/channel/{channelid}", response_class=HTMLResponse)
async def channel(channelid: str, response: Response, request: Request, sort_by: str = "newest",
                  yuki: Union[str, None] = Cookie(None), proxy: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    cid = clean_id(channelid)
    if not cid:
        return template("error.html", {"request": request, "context": "チャンネルIDが不正です"}, status_code=400)
    response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
    try:
        t = await getChannelData(cid, sort_by if sort_by in ("newest", "popular", "oldest") else "newest")
    except APITimeoutError:
        return template("apiTimeout.html", {"request": request}, status_code=504)
    return template("channel.html", {
        "request": request, "results": t[0], "shorts": t[1], "playlists": t[2],
        "community": t[3], "streams": t[4], "channel_id": cid,
        "channel_name": t[5]["channel_name"], "channel_icon": t[5]["channel_icon"],
        "channel_profile": t[5]["channel_profile"], "cover_img_url": t[5]["author_banner"],
        "subscribers_count": t[5]["subscribers_count"], "total_videos": t[5]["total_videos"],
        "sort_by": sort_by, "proxy": "True" if stream_mode(request, proxy) else "False",
    })


@app.get("/playlist", response_class=HTMLResponse)
async def playlist(list: str, response: Response, request: Request, page: Union[int, None] = 1,
                   yuki: Union[str, None] = Cookie(None), proxy: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    lid = clean_id(list)
    if not lid:
        return template("error.html", {"request": request, "context": "プレイリストIDが不正です"}, status_code=400)
    response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
    page = max(1, int(page or 1))
    mode = "local" if stream_mode(request, proxy) else "direct"
    hit = page_hit(request, "search.html", vary=f"pl|{mode}")
    if hit is not None:
        return hit
    try:
        results, has_next, meta = await getPlaylistData(lid, page)
    except APITimeoutError:
        return template("apiTimeout.html", {"request": request}, status_code=504)
    return render_html(request, "search.html", {
        "request": request, "results": results, "word": meta.get("title", ""), "page": page,
        "proxy": "True" if stream_mode(request, proxy) else "False",
        "next": f"/playlist?list={lid}&page={page + 1}" if has_next else None,
        "prev": f"/playlist?list={lid}&page={page - 1}" if page > 1 else None,
        "type": "", "sort_by": "", "duration": "", "failed": False,
        "api_next": f"/api/playlist?list={lid}&page={page + 1}" if has_next else None,
        "playlist": meta,
    }, ttl=CFG["ttl_search"], vary=f"pl|{mode}", cacheable=bool(results))


@app.get("/comments")
async def comments_page(request: Request, v: str, sort_by: str = "top", continuation: str = ""):
    vid = clean_vid(v)
    if not vid:
        return HTMLResponse("")
    try:
        data = await getCommentsData(vid, sort_by if sort_by in ("top", "new") else "top", continuation)
    except (APITimeoutError, NotFoundError) as exc:
        if request.query_params.get("format") == "json":
            return JSONResponse({"error": str(exc), "comments": []}, status_code=502)
        return HTMLResponse(f'<p style="color:#888">コメントを取得できませんでした({type(exc).__name__})</p>')
    if request.query_params.get("format") == "json":
        return JSONResponse(data)
    return template("comments.html", {"request": request, "comments": data["comments"],
                                      "continuation": data["continuation"],
                                      "comment_count": data["comment_count"]})


@app.get("/api/playlist")
async def api_playlist(list: str, request: Request, page: int = 1):
    """プレイリストを JSON で(/playlist の「続きを読み込み」用)。HTML を再取得させない。"""
    lid = clean_id(list)
    if not lid:
        return JSONResponse({"error": "プレイリストIDが不正です", "results": []}, status_code=400)
    try:
        results, has_next, meta = await getPlaylistData(lid, max(1, int(page or 1)))
    except APITimeoutError:
        return JSONResponse({"error": "API が応答しません", "results": []}, status_code=504,
                            headers={"Cache-Control": "no-store"})
    return JSONResponse({"list": lid, "page": page, "title": meta.get("title", ""),
                         "author": meta.get("author", ""), "count": meta.get("count", 0),
                         "results": results, "next": page + 1 if has_next else None},
                        headers={"Cache-Control": f"public, max-age={int(CFG['ttl_search'])}"})


@app.get("/api/comments/{video_id}")
async def api_comments(video_id: str, sort_by: str = "top", continuation: str = ""):
    """comments.html を使わないプレイヤー用の JSON API(/comments?format=json と同じ実体)。"""
    vid = clean_vid(video_id)
    if not vid:
        return JSONResponse({"error": "動画IDが不正です", "comments": []}, status_code=400)
    try:
        data = await getCommentsData(vid, sort_by if sort_by in ("top", "new") else "top", continuation)
    except (APITimeoutError, NotFoundError) as exc:
        return JSONResponse({"error": str(exc), "comments": []}, status_code=502,
                            headers={"Cache-Control": "no-store"})
    return JSONResponse(data, headers={"Cache-Control": "public, max-age=%d" % int(CFG["ttl_comments"])})


@app.get("/thumbnail")
async def thumbnail(request: Request, v: str, q: Union[str, None] = "mqdefault"):
    vid = clean_vid(v)
    if not vid:
        return Response(content=PLACEHOLDER_SVG, media_type="image/svg+xml", status_code=400)
    quality = q if q in THUMB_QUALITIES else "mqdefault"
    etag = f'W/"{vid}-{quality}"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "public, max-age=86400"})
    body, ctype, status = await fetch_thumb(vid, quality)
    return Response(content=body, media_type=ctype, status_code=status,
                    headers={"ETag": etag, "Cache-Control": "public, max-age=86400"})


@app.get("/proxyimg")
async def proxyimg(request: Request, u: str):
    """画像リレー。実体は常に instance の経路(/vi/…, /ggpht/…)だけを追う。

    外部ホスト(i.ytimg.com / yt3.googleusercontent.com …)は YPP_ALLOW_YT_DIRECT=1 の時だけ
    最終手段で使う — 既定では YouTube/Google に一切触れずに、instance 経由で画像を回収する。
    """
    url = urllib.parse.unquote(u or "")
    if not url.startswith(("https://", "http://")):
        return Response(content=PLACEHOLDER_SVG, media_type="image/svg+xml", status_code=403)
    parts = urllib.parse.urlsplit(url)
    host = parts.netloc.lower()
    if host not in _ALLOWED_IMG_HOSTS and host not in pool.allowlist():
        return Response(content=PLACEHOLDER_SVG, media_type="image/svg+xml", status_code=403)
    path = parts.path or "/"
    if ".." in path or not _IMG_PATH_RE.match(path):
        return Response(content=PLACEHOLDER_SVG, media_type="image/svg+xml", status_code=403)
    query = parts.query

    m = _VI_PATH_RE.match(path)
    if m:  # /vi/<videoId>/<file>.jpg → サムネ経路(キャッシュも共用)を再利用
        body, ctype, status = await fetch_thumb(m.group(1), m.group(2).rsplit(".", 1)[0])
        return Response(content=body, media_type=ctype, status_code=status,
                        headers={"Cache-Control": "public, max-age=86400"})

    key = "img:" + path + ("?" + query if query else "")
    hit = _thumb_cache.get(key)
    if hit:
        body, ctype = hit
    else:
        got = None
        for origin in pool.all_origins(3):
            got = await _fetch_image(origin.rstrip("/") + path + (f"?{query}" if query else ""))
            if got:
                break
        if not got and CFG["thumb_yt_fetch"]:  # 既定 OFF:サーバが YouTube を取りに行く事は無い
            got = await _fetch_image(url)
        if not got:
            _thumb_cache.set(key, (PLACEHOLDER_SVG, "image/svg+xml"))
            return Response(content=PLACEHOLDER_SVG, media_type="image/svg+xml", status_code=404,
                            headers={"Cache-Control": "public, max-age=3600"})
        _thumb_cache.set(key, got)
        body, ctype = got
    return Response(content=body, media_type=ctype, headers={"Cache-Control": "public, max-age=86400"})


@app.get("/captions")
async def captions_route(request: Request, v: str, label: str = ""):
    vid = clean_vid(v)
    if not vid or not label:
        return Response(content="WEBVTT\n\n", media_type="text/vtt", status_code=400)
    text = await caption_text(vid, label)
    if not text:
        return Response(content="WEBVTT\n\n", media_type="text/vtt; charset=utf-8", status_code=204)
    return Response(content=text, media_type="text/vtt; charset=utf-8",
                    headers={"Cache-Control": f"public, max-age={int(CFG['ttl_video'])}"})


@app.get("/p/{mode}")
async def switch_mode(mode: str, request: Request, response: Response, nxt: str = ""):
    """配信方式の切替: local(instance 経由) / direct(googlevideo 直)。

    戻る先は `?next=/path?query`(旧実装は `nxt` しか見ず、切替毎回ホームに飛ばしていた)を優先しつつ、
    相対パスのみ受けつける(`//evil.com` は "/" に落とす)。
    """
    mode = (mode or "").lower()
    raw = request.query_params.get("next") or nxt or "/"
    dest = raw if raw.startswith("/") and not raw.startswith("//") else "/"
    out = redirect(dest)
    if mode not in ("local", "direct"):
        return out
    out.set_cookie("proxy", "True" if mode == "local" else "off",
                   max_age=60 * 60 * 24 * 365, path="/", samesite="lax")
    out.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7, path="/", samesite="lax")
    return out


@app.get("/next")
async def next_video(request: Request, v: str = "", list: str = ""):  # noqa: A002
    """自動再生: 次の動画を選んで 302(プレイリスト継続対応)。"""
    vid = clean_vid(v)
    if not vid:
        return redirect("/")
    lid = clean_id(list)
    if lid:
        try:
            items, _, _ = await getPlaylistData(lid, 1)
            idx = next((i for i, it in enumerate(items) if it["id"] == vid), -1)
            if 0 <= idx + 1 < len(items):
                return redirect(f"/watch?v={items[idx + 1]['id']}&list={lid}")
        except Exception:  # noqa: BLE001
            pass
    try:
        pack = await api(f"/videos/{vid}?fields=recommendedVideos", "video")
        data = pack["data"] or {}
        rec = data.get("recommendedVideos") or data.get("recommendedvideo") or []
        for r in rec:
            rid = clean_vid(str(r.get("videoId") or "")) if isinstance(r, dict) else ""
            if rid and rid != vid:
                return redirect(f"/watch?v={rid}")
    except Exception:  # noqa: BLE001
        pass
    return redirect("/trending")


# --------------------------------------------------------------------------
# JSON API
# --------------------------------------------------------------------------
def _model_dict(m: BaseModel) -> dict:
    return m.model_dump() if hasattr(m, "model_dump") else m.dict()  # pydantic v1/v2 両対応


class TrendData(BaseModel):
    video_id: str
    title: str = ""
    author: str = ""
    thumbnail: str = ""
    length: str = ""


@app.post("/trend")
async def record_trend(data: TrendData):
    if trend_collection is None:
        return {"ok": False, "reason": "no db"}
    try:
        now = int(time.time())
        trend_collection.update_one(
            {"video_id": data.video_id},
            {"$inc": {"count": 1}, "$set": {**_model_dict(data), "last_watched": now},
             "$setOnInsert": {"first_watched": now}},
            upsert=True,
        )
        if random.randint(1, 100) == 1:
            cleanup_old_trends()
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


@app.get("/api/v/{video_id}")
async def api_video(video_id: str, request: Request, proxy: Union[str, None] = Cookie(None), full: int = 0):
    """動画メタデータの JSON(次動画の先読み・共有ダイアログ用)。"""
    vid = clean_vid(video_id)
    if not vid:
        return JSONResponse({"error": "invalid id"}, status_code=400)
    try:
        info, rec = await getVideoData(vid, stream_mode(request, proxy))
    except NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except APITimeoutError as exc:
        return JSONResponse({"error": str(exc)}, status_code=504)
    out = dict(info)
    if not full:
        out.pop("description_html", None)
        rec = rec[:8]
    out["recommended_videos"] = rec
    out["provider"] = "invidious"
    return JSONResponse(out, headers={"Cache-Control": f"public, max-age={int(CFG['ttl_video'])}"})


@app.get("/api/search")
async def api_search(q: str, page: int = 1, type: str = "", sort_by: str = "", duration: str = "",
                     limit: int = 0, proxy: Union[str, None] = Cookie(None)):
    if not (q or "").strip():
        return JSONResponse({"results": []})
    try:
        items = await getSearchData(q, max(1, page), type, sort_by, duration)
    except APITimeoutError as exc:
        return JSONResponse({"error": str(exc), "results": []}, status_code=504)
    if limit:
        items = items[:limit]
    return JSONResponse({"q": q, "page": page, "count": len(items), "results": items,
                         "next": page + 1 if len(items) >= CFG["results_per_page"] else None},
                        headers={"Cache-Control": f"public, max-age={int(CFG['ttl_search'])}"})


@app.get("/api/suggest")
@app.get("/suggest")
async def api_suggest(keyword: str = "", q: str = ""):
    items = await suggest_payload(keyword or q)
    return JSONResponse(items, headers={"Cache-Control": f"public, max-age={int(CFG['ttl_suggest'])}"})


async def streams_payload(video_id: str, request: Request, proxy: Optional[str]) -> JSONResponse:
    """代替プロバイダ枠。旧 /api/ytdlp の置換 — YouTube を直接叩かずに別 instance で組み直す。"""
    vid = clean_vid(video_id)
    if not vid:
        return JSONResponse({"error": "invalid id"}, status_code=400)
    local = stream_mode(request, proxy)
    try:
        pack = await api(f"/videos/{vid}?hl={CFG['lang']}&gl={CFG['region']}" + ("&local=true" if local else ""),
                         "video")
    except APITimeoutError as exc:
        return JSONResponse({"error": str(exc)}, status_code=504)
    except NotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    t, origin = pack["data"], pack["origin"]
    built = build_stream_list(t, origin, local)
    return JSONResponse({
        "quality_streams": [{"url": s["url"], "resolution": s["label"], "itag": s.get("itag"),
                             "type": s["type"], "audio_url": s.get("audio_url", ""),
                             "fallbacks": s.get("fallbacks", [])}
                            for s in built["quality_streams"]],
        "audio_url": built["audio_url"],
        "hlsUrl": built["hlsUrl"],
        "is_live": bool(t.get("liveNow")),
        "provider": f"invidious:{_host_of(origin)}",
        "mode": "local" if local else "direct",
    })


@app.get("/api/streams/{video_id}")
async def api_streams(video_id: str, request: Request, proxy: Union[str, None] = Cookie(None)):
    return await streams_payload(video_id, request, proxy)


@app.get("/api/ytdlp/{video_id}")
async def api_ytdlp_compat(video_id: str, request: Request, proxy: Union[str, None] = Cookie(None)):
    """互換エイリアス。yt-dlp は使わない(IP ban 環境では必ず失敗するため)。"""
    return await streams_payload(video_id, request, proxy)


@app.get("/api/channel_videos/{channel_id}")
async def api_channel_videos(channel_id: str, request: Request, sort_by: str = "newest", page: int = 1):
    cid = clean_id(channel_id)
    if not cid:
        return JSONResponse({"error": "invalid id", "videos": []}, status_code=400)
    pack = await channel_payloads(cid, sort_by if sort_by in ("newest", "popular", "oldest") else "newest")
    t = pack["main"]["data"] if isinstance(pack["main"]["data"], dict) else {}
    origin = pack["main"]["origin"] or pack["videos"]["origin"]
    raw = _videos_of(pack["videos"]["data"]) or _videos_of(t.get("latestVideos")) or _videos_of(t.get("latestvideo"))
    videos = [
        {
            "id": v.get("videoId", ""), "title": v.get("title", ""), "author": t.get("author", ""),
            "length": formatDuration(v.get("lengthSeconds", 0)), "published": v.get("published", 0),
            "published_text": (formatPublished(v["published"]) if v.get("published")
                               else v.get("publishedText", "")),
            "view_count_text": formatViewCount(v.get("viewCount", 0)),
            "thumbnail": _thumb_from(v, origin, str(v.get("videoId") or "")),
        }
        for v in raw if isinstance(v, dict)
    ]
    return JSONResponse({"videos": videos,
                         "channel": {"name": t.get("author", ""), "icon": _author_icon(t, origin)}},
                        headers={"Cache-Control": f"public, max-age={int(CFG['ttl_channel'])}"})


@app.get("/api/trending")
async def api_trending(category: str = "default", region: str = "", sort: str = ""):
    try:
        videos = await getTrending(category, region)
    except APITimeoutError as exc:
        return JSONResponse({"error": str(exc), "videos": []}, status_code=504)
    if sort == "views":
        def _n(v):
            m = re.match(r"[\d,.]+", str(v.get("view_count_text", "")))
            return float(m.group(0).replace(",", "")) if m else 0.0
        videos.sort(key=_n, reverse=True)
    return JSONResponse({"videos": videos, "category": category, "region": region or CFG["region"]},
                        headers={"Cache-Control": f"public, max-age={int(CFG['ttl_trending'])}"})


@app.get("/api/site_trending")
async def api_site_trending(period: str = "7days"):
    if trend_collection is not None:
        try:
            def _read():
                return list(trend_collection.find().sort("count", -1).limit(24))

            docs = await asyncio.to_thread(_read)
            out = [{"id": d.get("video_id"), "title": d.get("title", ""), "author": d.get("author", ""),
                    "thumbnail": f"/thumbnail?v={d.get('video_id')}&q=mqdefault", "length": d.get("length", ""),
                    "count": d.get("count", 0)} for d in docs]
            return JSONResponse({"videos": out, "period": period, "source": "mongo"})
        except Exception as exc:  # noqa: BLE001
            _dbg("mongo read", exc)
    if not TREND_API_URL:
        return JSONResponse({"videos": [], "period": period})
    try:
        r = await client().get(f"{TREND_API_URL.rstrip('/')}/trending?period={urllib.parse.quote(period)}",
                               timeout=httpx.Timeout(CFG["t_connect"], read=3.0))
        return JSONResponse(r.json())
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"error": str(exc), "videos": [], "period": period})


@app.get("/api/instances")
async def api_instances():
    return JSONResponse(pool.info())


@app.get("/api/instances/refresh")
async def api_instances_refresh():
    ok = await pool.refresh(True)
    return JSONResponse({"ok": ok, **pool.info()})


@app.get("/api/stats")
async def api_stats():
    total_up = _STATS["up_ok"] + _STATS["up_fail"]
    return JSONResponse({
        "uptime_sec": int(time.time() - _START),
        "version": version,
        "requests": dict(_STATS),
        "upstream": {"calls": total_up, "avg_ms": round(_STATS["up_ms"] / max(1, _STATS["up_ok"]), 1)},
        "cache": {name: fn.cache.stats() for name, fn in (
            ("api", _api_cached), ("video", video_payload), ("search", search_payload),
            ("channel", channel_payloads), ("comments", comments_payload), ("trending", trending_payload),
            ("suggest", suggest_payload))},
        "page_cache": {**_page_cache.stats(), "ttl": _page_cache.ttl},
        "thumb": {**_thumb_stats, **_thumb_cache.stats()},
        "instances": {k: len(v) for k, v in pool.data.items()},
    })


@app.get("/api/health")
async def api_health():
    now = time.time()
    healthy = sum(1 for o, h in pool.health.items() if h["ok"] and h["penalty"] <= now)
    return JSONResponse({"ok": bool(healthy) or pool.updated > 0,
                         "instances": sum(len(v) for v in pool.data.values()),
                         "healthy": healthy, "version": version})


# --------------------------------------------------------------------------
# 固定ページ(テンプレが実在するものだけを自動登録)
# --------------------------------------------------------------------------
_GATE = {  # yuki cookie 必須のページ
    "/trending": "trending.html", "/history": "history.html",
    "/subscriptions": "subscriptions.html", "/settings": "settings.html",
}
_PUBLIC = {  # 誰でも見られるページ(ゲーム・情報系)
    "/help": "help.html", "/proxypage": "settings.html", "/url": "url.html", "/light": "url.html",
    "/sitsumon": "otoiawase.html", "/news": "news.html", "/space": "others.html", "/update": "update.html",
    "/others": "others.html", "/qanda": "Q&A.html", "/1v1lol": "game.html", "/drive": "drive.html",
    "/paper": "paper.html", "/snow": "snow.html", "/2048": "block.html", "/ose": "ose.html",
    "/game": "game.html", "/and": "android.html", "/cone": "cone.html", "/usa": "usa.html",
    "/chat": "chat.html", "/ball": "ball.html", "/bj": "bj.html", "/tools": "tools.html",
    "/re": "re.html", "/among": "among.html", "/among-1": "among-1.html", "/among-2": "among-2.html",
    "/interland": "interland.html", "/denki": "denki.html", "/dog": "dog.html", "/dash": "dash.html",
    "/dairan": "dairan.html",
}


def _register_page(path: str, tpl: str, gate: bool) -> None:
    if not os.path.exists(_apath("templates", tpl)):  # 存在しないテンプレで 500 になるのを防ぐ
        log.warning("テンプレがないためルートを登録しません: %s -> %s", path, tpl)
        return

    @app.get(path, response_class=HTMLResponse, name=f"page{path.replace('/', '_')}")
    async def _route(request: Request, yuki: Union[str, None] = Cookie(None)):  # noqa: B008
        if gate and not checkCookie(request.cookies.get("yuki")):
            return redirect("/")
        return template(tpl, {"request": request})


for _p, _t in _GATE.items():
    _register_page(_p, _t, True)
for _p, _t in _PUBLIC.items():
    _register_page(_p, _t, False)


# --------------------------------------------------------------------------
# BBS(旧実装は上流 URL 仕様変更で常に 500 になっていた)
# --------------------------------------------------------------------------
BBS_BASE = os.environ.get("YPP_BBS_BASE", "").strip().rstrip("/")


class LocalBbs:
    """YPP_BBS_BASE 未設定時に動く簡易掲示板(Mongo があれば永続化)。"""

    def __init__(self):
        self._mem: dict = {}
        self._lock = asyncio.Lock()

    async def post(self, channel: str, name: str, message: str) -> dict:
        entry = {"name": (name or "名無し")[:40], "message": message[:1000],
                 "time": int(time.time()), "channel": channel}
        async with self._lock:
            lst = self._mem.setdefault(channel, [])
            lst.insert(0, entry)
            del lst[40:]
        if trend_collection is not None:
            try:
                await asyncio.to_thread(db["bbs"].insert_one, entry)
            except Exception as exc:  # noqa: BLE001
                _dbg("bbs mongo", exc)
        return entry

    async def list(self, channel: str) -> list:
        async with self._lock:
            items = list(self._mem.get(channel, []))
        if items or trend_collection is None:
            return items
        def _read():
            out = list(db["bbs"].find({"channel": channel}).sort("time", -1).limit(40))
            for d in out:
                d.pop("_id", None)
            return out
        try:
            return await asyncio.to_thread(_read)
        except Exception:  # noqa: BLE001
            return []


_bbs = LocalBbs()


async def bbs_upstream(path: str, query: str = "") -> Optional[str]:
    if not BBS_BASE:
        return None
    try:
        url = f"{BBS_BASE}/" + path.lstrip("/") + (f"?{query}" if query else "")
        r = await client().get(url, headers=getRandomUserAgent())
        return r.text if r.status_code == 200 else None
    except Exception as exc:  # noqa: BLE001
        _dbg("bbs upstream", repr(exc))
        return None


def _bbs_html(posts: list, channel: str) -> str:
    rows = []
    for p in posts:
        when = time.strftime("%Y/%m/%d %H:%M", time.localtime(int(p.get("time") or 0))) if p.get("time") else ""
        rows.append(
            '<div style="border-bottom:1px solid rgba(128,128,128,.2);padding:10px 0">'
            f'<b>{html_escape(p.get("name", "名無し"))}</b> '
            f'<span style="color:#888;font-size:12px">{when} / #{html_escape(p.get("channel", ""))}</span>'
            f'<div style="white-space:pre-wrap;word-break:break-word">{html_escape(p.get("message", ""))}</div></div>'
        )
    return (
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">' + no_robot_meta_tag +
        '<meta name="viewport" content="width=device-width,initial-scale=1"><title>Plus⁺ BBS</title>'
        '<style>body{font-family:Roboto,Arial,sans-serif;background:#0f0f0f;color:#fff;max-width:760px;'
        'margin:0 auto;padding:16px}form{display:flex;gap:8px;margin:12px 0;flex-wrap:wrap}'
        'input{flex:1;min-width:160px;padding:9px;border-radius:8px;border:1px solid #444;background:#1b1b1b;'
        'color:#fff}button{padding:9px 18px;border-radius:8px;border:0;background:#fff;color:#000;cursor:pointer}'
        'a{color:#3ea6ff}</style></head><body>'
        '<p><a href="/">← 戻る</a></p>'
        f'<h2>Plus⁺ BBS <small style="color:#888">#{html_escape(channel)}</small></h2>'
        '<form method="get" action="/bbs/result"><input name="name" placeholder="名前" maxlength="40">'
        '<input name="message" placeholder="メッセージ" maxlength="1000" required>'
        f'<input type="hidden" name="channel" value="{html_escape(channel)}"><button>書き込む</button></form>'
        + ("".join(rows) or '<p style="color:#888">まだ書き込みがありません</p>') +
        '</body></html>'
    )


def html_escape(s: str) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


@app.get("/bbs", response_class=HTMLResponse)
async def bbs(request: Request, name: str = "", seed: str = "", channel: str = "main",
              verify: str = "false", yuki: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    ch = clean_id(channel, 24) or "main"
    upstream = await bbs_upstream("bbs", urllib.parse.urlencode(
        {"name": name, "seed": seed, "channel": ch, "verify": verify}))
    if upstream:
        return HTMLResponse(upstream.replace("AutoLink(xhr.responseText);", "urlConvertToLink(xhr.responseText);"))
    return HTMLResponse(_bbs_html(await _bbs.list(ch), ch))


@app.get("/bbs/api")
async def bbsAPI(request: Request, t: str = "", channel: str = "main", verify: str = "false"):
    ch = clean_id(channel, 24) or "main"
    upstream = await bbs_upstream("bbs/api", urllib.parse.urlencode({"t": t, "verify": verify, "channel": ch}))
    if upstream:
        return PlainTextResponse(upstream)
    return JSONResponse({"posts": await _bbs.list(ch)})


@app.get("/bbs/result")
async def write_bbs(request: Request, name: str = "", message: str = "", seed: str = "",
                    channel: str = "main", verify: str = "false", yuki: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    if "google-apps-script" in (request.headers.get("user-agent") or "").lower():
        raise UnallowedBot("GASのBotは許可されていません")
    ch = clean_id(channel, 24) or "main"
    message = (message or "").strip()
    if not message:
        return redirect(f"/bbs?channel={urllib.parse.quote(ch)}")
    q = urllib.parse.urlencode({"name": name, "message": message, "seed": seed, "channel": ch,
                                "verify": verify, "info": getInfo(request), "serververify": getVerifyCode() or ""})
    upstream = await bbs_upstream("bbs/result", q)
    if upstream is not None:
        return HTMLResponse(upstream)
    await _bbs.post(ch, name, message)
    return redirect(f"/bbs?channel={urllib.parse.quote(ch)}")


@app.get("/bbs/how", response_class=PlainTextResponse)
async def view_commonds(request: Request, yuki: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    upstream = await bbs_upstream("bbs/how")
    return upstream or "Plus⁺ BBS の使い方\n  /bbs?channel=main にアクセスして書き込んでください。"


# --------------------------------------------------------------------------
# 管理/情報系(旧ルート互換)
# --------------------------------------------------------------------------
@app.get("/info", response_class=HTMLResponse)
async def viewlist(response: Response, request: Request, yuki: Union[str, None] = Cookie(None)):
    if not checkCookie(request.cookies.get("yuki")):
        return redirect("/")
    response.set_cookie("yuki", "True", max_age=60 * 60 * 24 * 7)
    first = {k: ((pool.data.get(k) or ["(空)"])[0]) for k in KINDS}
    return template("info.html", {
        "request": request, "Youtube_API": first["video"], "Channel_API": first["channel"],
        "Comments_API": first["comments"], "Search_API": first["search"], "Playlist_API": first["playlist"],
        "pool": pool.info(), "version": version, "stats_url": "/api/stats",
    })


@app.get("/reset", response_class=PlainTextResponse)
async def reset_apis():
    ok = await pool.refresh(True)
    return "Success" if ok else "Reloaded with fallback"


@app.get("/version", response_class=PlainTextResponse)
async def displayVersion():
    return str({"version": version, "new_instance_version": new_instance_version})


@app.get("/api/update", response_class=PlainTextResponse)
async def updateAllAPI():
    await pool.refresh(True)
    return str({"API": pool.data, "checkVideo": True})


@app.get("/api/{api_name}/next", response_class=PlainTextResponse)
async def rotateAPI(api_name: str):
    if api_name not in KINDS:
        return f"API Name Error: {api_name}"
    return "Finish: " + ", ".join(pool.rotate(api_name))


@app.get("/api/video/check", response_class=PlainTextResponse)
async def displayCheckVideo():
    return "True(shape-check)"


@app.get("/api/video/check/toggle", response_class=PlainTextResponse)
async def toggleVideoCheck():
    return "形態チェックは常時有効です(旧: ダウンロード検証 → 新: 軽量の形態検証のみ)"


@app.get("/api/{api_name}", response_class=PlainTextResponse)
async def displayAPI(api_name: str):
    if api_name == "all":
        return str({"API": pool.data, "checkVideo": True})
    if api_name in KINDS:
        return str(pool.data.get(api_name, []))
    return f"API Name Error: {api_name}"


# --------------------------------------------------------------------------
# エラーハンドラ
# --------------------------------------------------------------------------
@app.exception_handler(APITimeoutError)
async def apiWait(request: Request, exception: APITimeoutError):
    return template("apiTimeout.html", {"request": request}, status_code=504)


@app.exception_handler(NotFoundError)
async def not_found_handler(request: Request, exception: NotFoundError):
    return template("error.html", {"request": request, "context": f"見つかりません: {exception}"}, status_code=404)


@app.exception_handler(UnallowedBot)
async def returnToUnallowedBot(request: Request, exception: UnallowedBot):
    return template("error.html", {"request": request, "context": "403 Forbidden"}, status_code=403)


@app.exception_handler(404)
async def error404(request: Request, exc):
    if request.url.path.startswith(("/api/", "/thumbnail", "/captions", "/proxyimg")):
        return JSONResponse({"error": "not found", "path": request.url.path}, status_code=404)
    return template("error.html", {"request": request, "context": "404 Error、あれれ"}, status_code=404)


@app.exception_handler(500)
async def error500(request: Request, exc):
    log.exception("500 on %s", request.url.path)
    return template("error.html", {"request": request, "context": "500 Internal Server Error"}, status_code=500)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8080)),
                 log_level="debug" if CFG["debug"] else "info")
