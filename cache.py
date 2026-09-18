# -*- coding: utf-8 -*-
"""
キャッシュ基盤 (ytnext 版)

旧実装の問題点:
  * `Lock()` を呼び出し毎に new していた → 排他が効かずレース条件が発生
  * 有効期限が「キャッシュ全体で1つ」→ 1件でも切れると全消去で命中率が極端に低い
  * `lru_cache` の ttl 属性を書き換えているだけで、キー毎の TTL を持たない
  * 同期 `time.sleep` 風のブロッキング更新しかでき、更新中は全リクエストが待つ

新しい実装:
  * TTLCache   : スレッドセーフな per-key TTL + LRU + ヒット率統計
  * acache()   : asyncio 版デコレータ。
                 - 同時同一キー要求を1本にまとげる single-flight(上流を叩く回数激減)
                 - stale-while-revalidate(期限切れでも古い値を即返し、裏で更新)
                 - 上流全滅時は古い値をフォールバックとして返す(SWR 期間内)
  * ByteCache  : 合計バイト上限付き LRU(サムネイル画像用)
  * cache()    : 後方互換(同期関数用の per-key TTL デコレータ)
"""
from __future__ import annotations

import asyncio
import functools
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Optional, Tuple

_MISSING = object()


# --------------------------------------------------------------------------
# 同期: per-key TTL + LRU
# --------------------------------------------------------------------------
class TTLCache:
    """スレッドセーフな TTL+LRU キャッシュ。"""

    __slots__ = ("_d", "_lock", "maxsize", "ttl", "stale_ttl", "hits", "misses", "sets")

    def __init__(self, maxsize: int = 256, ttl: float = 60.0, stale_ttl: float = 0.0):
        self._d: "OrderedDict[Any, Tuple[float, float, Any]]" = OrderedDict()
        self._lock = threading.RLock()
        self.maxsize = maxsize
        self.ttl = float(ttl)
        # stale_ttl: 期限後も「フォールバックとして」許容する秒数
        self.stale_ttl = float(stale_ttl)
        self.hits = 0
        self.misses = 0
        self.sets = 0

    def get(self, key: Any, default: Any = None) -> Any:
        found, value, _ = self.get_ex(key)
        return value if found else default

    def get_ex(self, key: Any) -> Tuple[bool, Any, bool]:
        """(見つかったか, 値, 鮮度) を返す。値が stale でも見つかったら True。"""
        now = time.time()
        with self._lock:
            ent = self._d.get(key)
            if ent is None:
                self.misses += 1
                return False, None, False
            stored, ttl, value = ent
            fresh = (now - stored) <= ttl
            if not fresh and self.stale_ttl and (now - stored) > ttl + self.stale_ttl:
                del self._d[key]
                self.misses += 1
                return False, None, False
            self._d.move_to_end(key)
            if fresh:
                self.hits += 1
            return True, value, fresh

    def set(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._d[key] = (time.time(), self.ttl if ttl is None else float(ttl), value)
            self._d.move_to_end(key)
            self.sets += 1
            while len(self._d) > self.maxsize:
                self._d.popitem(last=False)

    def evict(self, key: Any) -> None:
        with self._lock:
            self._d.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._d.clear()

    def stats(self) -> dict:
        with self._lock:
            total = self.hits + self.misses
            return {
                "size": len(self._d),
                "maxsize": self.maxsize,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0,
            }

    def __len__(self) -> int:  # pragma: no cover - 便利関数
        with self._lock:
            return len(self._d)


class ByteCache:
    """合計バイト上限付きの LRU(画像など大きな値向け)。"""

    def __init__(self, max_bytes: int = 32 * 1024 * 1024, max_items: int = 512, ttl: float = 43200.0):
        self._d: "OrderedDict[Any, Tuple[float, int, Any]]" = OrderedDict()
        self._lock = threading.RLock()
        self.max_bytes = int(max_bytes)
        self.max_items = int(max_items)
        self.ttl = float(ttl)
        self.bytes_used = 0
        self.hits = 0
        self.misses = 0

    def get(self, key: Any) -> Optional[Tuple[Any, int]]:
        """(value, size) 相当のタプルを返す(miss は None)。"""
        now = time.time()
        with self._lock:
            ent = self._d.get(key)
            if ent is None:
                self.misses += 1
                return None
            stored, size, value = ent
            if now - stored > self.ttl:
                del self._d[key]
                self.bytes_used -= size
                self.misses += 1
                return None
            self._d.move_to_end(key)
            self.hits += 1
            return value

    def set(self, key: Any, value: Any, size: Optional[int] = None, ttl: Optional[float] = None) -> None:
        size = len(value) if size is None else int(size)
        if size > self.max_bytes:
            return
        with self._lock:
            old = self._d.pop(key, None)
            if old:
                self.bytes_used -= old[1]
            self._d[key] = (time.time(), size, value)
            self.bytes_used += size
            while len(self._d) > self.max_items or self.bytes_used > self.max_bytes:
                if not self._d:
                    break
                _, (stored, sz, _v) = self._d.popitem(last=False)
                self.bytes_used -= sz

    def stats(self) -> dict:
        with self._lock:
            total = self.hits + self.misses
            return {
                "items": len(self._d),
                "bytes": self.bytes_used,
                "max_bytes": self.max_bytes,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0,
            }


# --------------------------------------------------------------------------
# asyncio: single-flight + stale-while-revalidate
# --------------------------------------------------------------------------
def _make_key(args: Tuple[Any, ...], kwargs: dict) -> Any:
    if kwargs:
        return args + (tuple(sorted(kwargs.items())),)
    return args


def acache(
    ttl: float = 60.0,
    maxsize: int = 512,
    stale_ttl: float = 0.0,
    *,
    key: Optional[Callable[..., Any]] = None,
    serve_stale: bool = True,
) -> Callable:
    """async 関数向けの TTL/-single-flight キャッシュデコレータ。

    ttl        : 鮮度保持時間(秒)。これを過ぎると「stale」扱い。
    stale_ttl  : stale をいつまで許すか(秒)。0 で無効。
    serve_stale: ttl 切れ時に、古い値を即返し裏で更新する(SWR)。
    """

    def deco(fn):
        cachebox = TTLCache(maxsize=maxsize, ttl=ttl, stale_ttl=stale_ttl if serve_stale else 0.0)
        inflight: "dict[Any, asyncio.Future]" = {}

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            k = key(*args, **kwargs) if key else _make_key(args, kwargs)
            found, value, fresh = cachebox.get_ex(k)
            if found and fresh:
                return value

            fut = inflight.get(k)
            if fut is not None:                      # 同キーの同時リクエストは1本に寄せる
                try:
                    return await asyncio.shield(fut)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if found:                        # 更新失敗 → stale を返す
                        return value
                    raise
            if found and serve_stale:                # SWR: 古い値を返し、裏で更新
                task = asyncio.ensure_future(_refresh(k, args, kwargs))
                _BACKGROUND.add(task)
                task.add_done_callback(_BACKGROUND.discard)
                return value

            task = asyncio.ensure_future(_call(k, args, kwargs))
            inflight[k] = task
            try:
                return await asyncio.shield(task)
            finally:
                inflight.pop(k, None)

        async def _call(k, args, kwargs):
            result = await fn(*args, **kwargs)
            cachebox.set(k, result)
            return result

        async def _refresh(k, args, kwargs):
            try:
                cachebox.set(k, await fn(*args, **kwargs))
            except Exception:
                # 更新失敗は握りつぶし(次のリクエストで再試行)。stale な値は残す。
                pass

        wrapper.cache = cachebox
        wrapper.cache_clear = cachebox.clear
        wrapper.cache_stats = cachebox.stats
        return wrapper

    return deco


# 裏更新タスクのガベージコレクション防止用セット
_BACKGROUND: "set[asyncio.Task]" = set()


def spawn(coro) -> Optional[asyncio.Task]:
    """fire-and-forget。イベントループが居る時だけタスク化する。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover
        loop = None
    if loop is None:  # pragma: no cover - テスト環境など
        return None
    task = loop.create_task(coro)
    _BACKGROUND.add(task)
    task.add_done_callback(_BACKGROUND.discard)
    return task


# --------------------------------------------------------------------------
# 後方互換: 同期関数用デコレータ(旧 API 維持)
# --------------------------------------------------------------------------
def cache(seconds: int, max_size: int = 256, typed: bool = False):  # noqa: ARG001
    def deco(fn):
        box = TTLCache(maxsize=max_size, ttl=float(seconds))

        @functools.wraps(fn)
        def inner(*args, **kwargs):
            k = _make_key(args, kwargs)
            found, value, fresh = box.get_ex(k)
            if found and fresh:
                return value
            value = fn(*args, **kwargs)
            box.set(k, value)
            return value

        inner.clear_cache = box.clear
        inner.cache_info = box.stats
        inner.cache = box
        return inner

    return deco
