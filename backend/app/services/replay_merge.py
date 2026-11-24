"""重采交叠区间去重与归并重算引擎。

本模块只做纯计算和账本（merge book）的读写，不依赖 store / FastAPI，
保证同一套规则可以被领域服务、路由和测试复用。

核心约束：
1. 同一测点同一时刻的观测，在任意一次重采的产物里最多被采纳一次；
2. 已确认时段属于点级公共事实，后续申请与其交叠时只能沿用，不能撤销、
   不能把其中的原始行二次采纳为新观测，也不能对交叠窗口重复重算；
3. 归并窗口的状态机为：待重算 -> 重算中 -> 已重算 -> 已确认，
   交叠窗口直接落「已确认」并记录重算依据与来源窗口引用。
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

# 窗口状态：待重算（本申请新覆盖、尚无确认结果）
WINDOW_PENDING = "待重算"
# 窗口状态：重算中（已落盘，中断后可从该状态恢复）
WINDOW_RUNNING = "重算中"
# 窗口状态：已重算（本申请已算出结果，但申请尚未整体确认）
WINDOW_RECOMPUTED = "已重算"
# 窗口状态：已确认（本申请确认，或沿用其他申请已确认的交叠窗口）
WINDOW_CONFIRMED = "已确认"
# 窗口状态：已取消（申请中断后被取消，仅作用于未确认部分）
WINDOW_CANCELLED = "已取消"

# 原始行来源：本次申请新采纳的重采行
ORIGIN_FRESH = "本次重采原始行"
# 原始行来源：其他申请已确认时段内的行，仅作血缘引用，不重复产出观测
ORIGIN_CONFIRMED = "已确认原始行"


def parse_ts(value: str | float | datetime) -> float:
    """把 ISO 字符串 / epoch / datetime 统一成 epoch 秒。"""
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    return datetime.fromisoformat(text).timestamp()


def fmt_ts(epoch: float) -> str:
    """epoch 秒格式化回 ISO 字符串（秒级，与示例数据口径一致）。"""
    return datetime.fromtimestamp(epoch).isoformat(timespec="seconds")


def merge_intervals(intervals: list[dict[str, Any]]) -> list[dict[str, float]]:
    """把若干 [start, end) 区间合并成互不交叠的并集，按起点排序。"""
    bounded = sorted(
        ((parse_ts(item["start"]), parse_ts(item["end"])) for item in intervals if item),
        key=lambda pair: pair[0],
    )
    merged: list[list[float]] = []
    for lo, hi in bounded:
        if hi <= lo:
            continue
        if not merged or lo > merged[-1][1]:
            merged.append([lo, hi])
        elif hi > merged[-1][1]:
            merged[-1][1] = hi
    return [{"start": lo, "end": hi} for lo, hi in merged]


def interval_contains(intervals: list[dict[str, Any]], epoch: float) -> bool:
    """时刻是否落在任一 [start, end) 区间内。"""
    return any(
        parse_ts(item["start"]) <= epoch < parse_ts(item["end"])
        for item in intervals
    )


def split_windows(
    requested: dict[str, Any],
    confirmed_union: list[dict[str, Any]],
    window_minutes: int,
    replay_no: str,
) -> list[dict[str, Any]]:
    """按固定窗口长度切分本次申请区间，并标注每个窗口的重算依据。

    与点级已确认时段交叠的窗口直接落「已确认」，记录依据和引用来源；
    其余窗口落「待重算」。部分交叠在本平台口径下不会出现：窗口边界与
    已确认区间都按申请边界对齐，切分前先用已确认并集裁剪。
    """
    lo = parse_ts(requested["start"])
    hi = parse_ts(requested["end"])
    step = max(int(window_minutes), 1) * 60
    windows: list[dict[str, Any]] = []
    index = 1
    cursor = lo
    while cursor < hi:
        next_edge = min(cursor + step, hi)
        confirmed_hit = [
            item for item in confirmed_union
            if parse_ts(item["start"]) < next_edge and parse_ts(item["end"]) > cursor
        ]
        common = {
            "window_id": f"{replay_no}#W{index}",
            "replay_no": replay_no,
            "start": fmt_ts(cursor),
            "end": fmt_ts(next_edge),
            "start_epoch": cursor,
            "end_epoch": next_edge,
            "rollup_ref": None,
            "rollup_value": None,
            "source_timestamps": [],
        }
        if confirmed_hit:
            common.update({
                "status": WINDOW_CONFIRMED,
                "basis": (
                    f"与已确认时段{fmt_ts(confirmed_hit[0]['start'])}~"
                    f"{fmt_ts(confirmed_hit[0]['end'])}重叠：沿用已确认重算结果，"
                    "原始行不二次采纳、窗口不重复重算"
                ),
            })
        else:
            common.update({
                "status": WINDOW_PENDING,
                "basis": "本次申请新覆盖区间：基于去重后的原始行执行重算",
            })
        windows.append(common)
        index += 1
        cursor = next_edge
    return windows


def dedupe_source_rows(
    rows: list[dict[str, Any]],
    lo: float,
    hi: float,
    confirmed_union: list[dict[str, Any]],
    replay_no: str,
    source_file: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """在 [lo, hi) 内按时间戳去重采纳原始行。

    返回 (采纳行, 被丢弃的重复行)。已确认时段内的行标记为
    ``已确认原始行``——只保留血缘引用，不允许再作为新观测写出；
    其余标记为 ``本次重采原始行``。
    """
    chosen: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    seen: set[float] = set()
    for row in sorted(rows, key=lambda item: parse_ts(item.get("timestamp", 0))):
        try:
            ts = parse_ts(row.get("timestamp", ""))
        except (ValueError, TypeError):
            continue
        if not (lo <= ts < hi):
            continue
        if ts in seen:
            dropped.append({
                "timestamp": fmt_ts(ts),
                "replay_no": replay_no,
                "reason": "同一时刻重复观测，去重保留首条",
            })
            continue
        seen.add(ts)
        in_confirmed = interval_contains(confirmed_union, ts)
        chosen.append({
            "timestamp": fmt_ts(ts),
            "timestamp_epoch": ts,
            "value": row.get("calibrated_value_V", row.get("raw_value_mV", 0)),
            "source_file": source_file,
            "origin": ORIGIN_CONFIRMED if in_confirmed else ORIGIN_FRESH,
            "replay_no": replay_no,
        })
    return chosen, dropped


def new_book(point: str) -> dict[str, Any]:
    """创建一个测点的空归并账本。"""
    return {
        "point": point,
        # 点级已确认时段并集：所有已完成确认的申请共同贡献，只增不改、不被撤销
        "confirmed_ranges": [],
        "runs": {},
    }


def load_book(path: Path, point: str) -> dict[str, Any]:
    """读取测点账本，文件不存在时返回空账本。"""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            book = json.load(f)
        book.setdefault("point", point)
        book.setdefault("confirmed_ranges", [])
        book.setdefault("runs", {})
        return book
    return new_book(point)


def save_book(path: Path, book: dict[str, Any]) -> None:
    """原子写入账本，避免中断时留下半截 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def book_path(data_dir: Path, point: str) -> Path:
    return data_dir / "replay_merge" / f"{point}_merge_book.json"
