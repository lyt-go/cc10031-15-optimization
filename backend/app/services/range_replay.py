"""区间重采领域服务：维护传感器故障或标定变更引起的历史区间重采与覆盖方式。

重采围绕测点级「归并账本」(merge book) 组织，解决两份重采申请交叠时的
去重与重算问题：

- 账本按测点保存：每次补采实际采纳的原始行血缘、点级已确认时段、
  本次待重算/已重算/已确认的归并窗口、断点检查点；
- 已确认时段是点级公共事实，后续申请与其交叠时只沿用、不撤销，
  交叠窗口直接标记「已确认」并记录依据，原始行不二次采纳；
- 重采产物按 replay 编号整份重写（非追加），同一时刻的观测在任意
  一份产物里最多出现一次，续跑中断任务不会把已写过的行算两次；
- 成功和中断的申请各自维护检查点，可分别恢复、确认或取消。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services import replay_merge as rm
from app.store import store


class RangeReplayService(BaseModuleService):
    MODULE = "range_replay"
    ENTITY = "重采任务"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["重采编号", "测点编号", "起始时刻", "截止时刻", "重采原因", "覆盖方式", "重采点数", "重采水位"]
    REQUIRED_FIELDS = ["重采编号", "测点编号", "起始时刻"]
    STATUS_ORDER = ["草稿", "待重采", "重采中", "已完成", "已取消"]
    ACTION_RULES = {"启动重采": "重采中", "标记完成": "已完成", "取消重采": "已取消"}
    NEGATIVE_ACTIONS = ["取消重采"]

    # 默认归并窗口长度（分钟），与 rollup_window 的 5 分钟口径对齐
    DEFAULT_WINDOW_MINUTES = 5

    # ------------------------------------------------------------------
    # 账本 / 产物路径
    # ------------------------------------------------------------------

    def _book_file(self, point_key: str):
        return rm.book_path(self._data_dir("."), point_key)

    def _artifact_file(self, replay_no: str) -> Any:
        return self._data_dir("replayed_ranges") / f"{replay_no}_replay.jsonl"

    def _checkpoint_file(self, replay_no: str) -> Any:
        return self._data_dir("checkpoints") / f"replay_{replay_no}.json"

    def _load_book(self, point_key: str) -> dict[str, Any]:
        return rm.load_book(self._book_file(point_key), point_key)

    def _persist_book(self, book: dict[str, Any]) -> None:
        rm.save_book(self._book_file(book["point"]), book)

    def _load_source_rows(self, point_key: str) -> tuple[list[dict[str, Any]], str]:
        """读取标定后数据，缺失时回退到边缘缓冲原始数据。"""
        cal_file = self._data_dir("calibrated") / f"{point_key}_calibrated.jsonl"
        rows = self._read_jsonl(cal_file)
        if rows:
            return rows, str(cal_file.name)
        edge_file = self._data_dir("edge_samples") / f"{point_key}.jsonl"
        return self._read_jsonl(edge_file), str(edge_file.name)

    # ------------------------------------------------------------------
    # 申请登记 / 归并切窗
    # ------------------------------------------------------------------

    def register_replay(
        self,
        replay_id: int,
        window_minutes: int = DEFAULT_WINDOW_MINUTES,
    ) -> dict[str, Any]:
        """把一份重采申请登记进测点账本，完成交叠分析与归并窗口切分。

        同一测点上若已有申请确认过部分时段，交叠窗口直接落「已确认」，
        其余窗口落「待重算」；原始行在真正启动时再去重采纳。
        """
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        point_key = replay["测点编号"]
        replay_no = replay["重采编号"]
        try:
            start = rm.parse_ts(replay["起始时刻"])
            end = rm.parse_ts(replay["截止时刻"])
        except (ValueError, TypeError):
            return {"error": "起始时刻/截止时刻必须是合法时间，且需同时提供"}
        if end <= start:
            return {"error": "截止时刻必须晚于起始时刻"}

        book = self._load_book(point_key)
        if replay_no in book["runs"]:
            return {"error": f"重采申请 {replay_no} 已登记，重复提交不会产生第二份观测"}

        confirmed_union = rm.merge_intervals(book["confirmed_ranges"])
        windows = rm.split_windows(
            requested={"start": start, "end": end},
            confirmed_union=confirmed_union,
            window_minutes=window_minutes,
            replay_no=replay_no,
        )
        pending_windows = [w for w in windows if w["status"] == rm.WINDOW_PENDING]
        overlap_windows = [w for w in windows if w["status"] == rm.WINDOW_CONFIRMED]

        run = {
            "replay_id": replay_id,
            "replay_no": replay_no,
            "point": point_key,
            "start": rm.fmt_ts(start),
            "end": rm.fmt_ts(end),
            "reason": replay.get("重采原因", "传感器故障"),
            "overwrite_mode": replay.get("覆盖方式", "追加"),
            "window_minutes": int(window_minutes),
            "status": "待重采",
            # 本次申请实际采纳的原始行（含已确认行的血缘引用）
            "adopted_rows": [],
            "dropped_duplicates": [],
            "windows": windows,
            "checkpoint": {
                "processed_rows": 0,
                "recomputed_windows": 0,
                "total_windows": len(windows),
                "pending_windows": len(pending_windows),
                "interrupted": False,
                "updated_at": time.time(),
            },
            "delivered_range": None,
            "created_at": time.time(),
        }
        book["runs"][replay_no] = run
        self._persist_book(book)
        # 同步内存任务状态
        self.run_action(replay_id, "启动重采")
        run["status"] = "重采中"
        return {
            "replay_no": replay_no,
            "point": point_key,
            "requested_range": [run["start"], run["end"]],
            "confirmed_overlap_windows": [w["window_id"] for w in overlap_windows],
            "pending_windows": [w["window_id"] for w in pending_windows],
            "window_count": len(windows),
        }

    # ------------------------------------------------------------------
    # 执行 / 续跑
    # ------------------------------------------------------------------

    def replay_range(
        self,
        replay_id: int,
        *,
        window_minutes: int = DEFAULT_WINDOW_MINUTES,
        recompute_rollup: bool = True,
        max_windows: int | None = None,
    ) -> dict[str, Any]:
        """执行（或断点续跑）一份重采申请。

        ``max_windows`` 用于模拟中断：本次最多重算这么多个待重算/重算中
        窗口，剩余窗口保留检查点，下次调用即可续跑。
        """
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        point_key = replay["测点编号"]
        replay_no = replay["重采编号"]
        book = self._load_book(point_key)
        run = book["runs"].get(replay_no)
        if run is None:
            register = self.register_replay(replay_id, window_minutes=window_minutes)
            if "error" in register:
                return register
            book = self._load_book(point_key)
            run = book["runs"][replay_no]
        if run["status"] == "已取消":
            return {"error": f"重采申请 {replay_no} 已取消，不可续跑"}
        if run["status"] == "已完成":
            return self.resume_state(replay_no)

        lo = rm.parse_ts(run["start"])
        hi = rm.parse_ts(run["end"])
        confirmed_union = rm.merge_intervals(book["confirmed_ranges"])

        # 1) 原始行去重采纳：已确认行只留血缘引用，重复时间戳只保留首条
        source_rows, source_file = self._load_source_rows(point_key)
        adopted, dropped = rm.dedupe_source_rows(
            source_rows, lo, hi, confirmed_union, replay_no, source_file,
        )
        run["adopted_rows"] = adopted
        run["dropped_duplicates"] = dropped
        fresh_rows = [row for row in adopted if row["origin"] == rm.ORIGIN_FRESH]

        # 2) 把采纳行挂到待处理窗口，作为各窗口的重算依据
        for window in run["windows"]:
            if window["status"] in (rm.WINDOW_PENDING, rm.WINDOW_RUNNING):
                window["source_timestamps"] = [
                    row["timestamp"] for row in fresh_rows
                    if window["start_epoch"] <= row["timestamp_epoch"] < window["end_epoch"]
                ]

        # 3) 逐窗口重算；已确认交叠窗口永不参与，保证不会算两次
        targets = [
            w for w in run["windows"]
            if w["status"] in (rm.WINDOW_PENDING, rm.WINDOW_RUNNING)
        ]
        budget = len(targets) if max_windows is None else max(0, int(max_windows))
        interrupted = budget < len(targets)
        for window in targets:
            if budget <= 0:
                break
            window["status"] = rm.WINDOW_RUNNING
            self._persist_book(book)  # 中断点：重算中状态已落盘
            if recompute_rollup:
                ok = self._recompute_window_rollup(book, run, window, fresh_rows)
                if not ok:
                    # 重算失败回退为待重算，保留检查点供续跑重试
                    window["status"] = rm.WINDOW_PENDING
                    interrupted = True
                    break
            else:
                window["rollup_value"] = self._window_value(window, fresh_rows)
                window["rollup_ref"] = f"replay:{replay_no}:{window['window_id']}"
            window["status"] = rm.WINDOW_RECOMPUTED
            budget -= 1

        # 4) 产物整份重写（只写本次新采纳的观测，已确认行不重复产出）
        output_file = self._artifact_file(replay_no)
        self._overwrite_jsonl(output_file, self._build_artifacts(run))

        # 5) 检查点与可交付范围
        done = self._refresh_checkpoint(run, interrupted=interrupted)
        run["delivered_range"] = self._delivered_range(run)
        self._persist_book(book)
        self._write_checkpoint(f"replay_{replay_no}", dict(run["checkpoint"]))

        replay["重采点数"] = str(len(fresh_rows))
        replay["重采水位"] = str(done["recomputed_windows"])
        if interrupted:
            # 保留重采中状态与检查点，等待 resume_replay 续跑
            run["status"] = "重采中"
            replay["status"] = "重采中"
            replay["pending"] = True
        else:
            run["status"] = "已完成"
            self.run_action(replay_id, "标记完成")
        self._persist_book(book)

        return {
            "replay_id": replay["重采编号"],
            "replay_no": replay_no,
            "point": point_key,
            "records_adopted": len(adopted),
            "records_fresh": len(fresh_rows),
            "records_from_confirmed": len(adopted) - len(fresh_rows),
            "duplicates_dropped": len(dropped),
            "recomputed_windows": done["recomputed_windows"],
            "pending_windows": done["pending_windows"],
            "interrupted": interrupted,
            "delivered_range": run["delivered_range"],
            "artifact": str(output_file),
        }

    def resume_replay(
        self,
        replay_id: int,
        max_windows: int | None = None,
    ) -> dict[str, Any]:
        """续跑中断申请：只补算剩余窗口，已写观测不重复计算。"""
        return self.replay_range(replay_id, max_windows=max_windows)

    # ------------------------------------------------------------------
    # 确认 / 取消
    # ------------------------------------------------------------------

    def confirm_replay(self, replay_id: int) -> dict[str, Any]:
        """确认一份申请：其已重算窗口升为点级已确认时段。

        只确认本申请新覆盖的窗口；沿用其他申请的交叠窗口本就是已确认，
        确认动作不会撤销或改动任何其他申请的已确认结果。
        """
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        replay_no = replay["重采编号"]
        book = self._load_book(replay["测点编号"])
        run = book["runs"].get(replay_no)
        if run is None:
            return {"error": f"重采申请 {replay_no} 尚未登记"}
        if run["status"] == "已取消":
            return {"error": f"重采申请 {replay_no} 已取消，不能确认"}

        promoted = []
        for window in run["windows"]:
            if window["status"] == rm.WINDOW_RECOMPUTED:
                window["status"] = rm.WINDOW_CONFIRMED
                window["basis"] = "申请已确认：重算结果升为点级已确认事实"
                promoted.append(window)
        if promoted:
            book["confirmed_ranges"].append({
                "start": promoted[0]["start"],
                "end": promoted[-1]["end"],
                "source_replay": replay_no,
                "windows": [w["window_id"] for w in promoted],
            })
            # 合并点级已确认并集，落盘前规整为无交叠区间并保持 ISO 字符串
            merged_ranges = rm.merge_intervals(book["confirmed_ranges"])
            book["confirmed_ranges"] = [
                {
                    "start": rm.fmt_ts(item["start"]),
                    "end": rm.fmt_ts(item["end"]),
                    "source_replay": replay_no,
                }
                for item in merged_ranges
            ]
        run["status"] = "已完成"
        run["checkpoint"]["interrupted"] = False
        run["delivered_range"] = self._delivered_range(run)
        self._persist_book(book)
        self.run_action(replay_id, "标记完成")
        return {
            "replay_no": replay_no,
            "confirmed_windows": len(promoted),
            "confirmed_ranges": book["confirmed_ranges"],
            "delivered_range": run["delivered_range"],
        }

    def cancel_replay(self, replay_id: int) -> dict[str, Any]:
        """取消一份中断/未完成申请，只作废其未确认窗口。

        点级已确认时段与其他申请的已确认结果一律不动；交叠窗口本就
        沿用既有确认事实，也不会被撤销。
        """
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        replay_no = replay["重采编号"]
        book = self._load_book(replay["测点编号"])
        run = book["runs"].get(replay_no)
        if run is None:
            return {"error": f"重采申请 {replay_no} 尚未登记"}
        cancelled_windows = []
        for window in run["windows"]:
            if window["status"] in (rm.WINDOW_PENDING, rm.WINDOW_RUNNING, rm.WINDOW_RECOMPUTED):
                window["status"] = rm.WINDOW_CANCELLED
                cancelled_windows.append(window["window_id"])
        run["status"] = "已取消"
        run["checkpoint"]["interrupted"] = False
        run["delivered_range"] = self._delivered_range(run)
        self._persist_book(book)
        self.run_action(replay_id, "取消重采")
        return {
            "replay_no": replay_no,
            "cancelled_windows": cancelled_windows,
            "confirmed_ranges_kept": book["confirmed_ranges"],
        }

    # ------------------------------------------------------------------
    # 可观测 / 可恢复状态查询
    # ------------------------------------------------------------------

    def resume_state(self, replay_no: str) -> dict[str, Any]:
        """返回一份申请的恢复状态：检查点、窗口清单与当前可交付范围。"""
        for point_book in self._all_books():
            run = point_book["runs"].get(replay_no)
            if run is not None:
                return {
                    "replay_no": replay_no,
                    "point": point_book["point"],
                    "status": run["status"],
                    "checkpoint": run["checkpoint"],
                    "delivered_range": run["delivered_range"],
                    "windows": [
                        {
                            "window_id": w["window_id"],
                            "start": w["start"],
                            "end": w["end"],
                            "status": w["status"],
                            "basis": w["basis"],
                            "rollup_ref": w["rollup_ref"],
                            "source_timestamps": w["source_timestamps"],
                        }
                        for w in run["windows"]
                    ],
                }
        return {"error": f"重采申请 {replay_no} 无账本记录"}

    def merge_book(self, point_key: str) -> dict[str, Any]:
        """返回测点级归并账本：原始行血缘、确认时段、全部窗口与检查点。"""
        book = self._load_book(point_key)
        return {
            "point": point_key,
            "confirmed_ranges": book["confirmed_ranges"],
            "runs": [
                {
                    "replay_no": run["replay_no"],
                    "status": run["status"],
                    "requested_range": [run["start"], run["end"]],
                    "adopted_rows": run["adopted_rows"],
                    "dropped_duplicates": run["dropped_duplicates"],
                    "checkpoint": run["checkpoint"],
                    "delivered_range": run["delivered_range"],
                    "windows": run["windows"],
                }
                for run in sorted(book["runs"].values(), key=lambda item: item["start"])
            ],
        }

    # ------------------------------------------------------------------
    # 兼容旧入口：批量故障重采
    # ------------------------------------------------------------------

    def batch_replay_faulty(self, reason: str = "传感器故障校准") -> dict[str, Any]:
        """批量重采所有因传感器故障导致异常的测点历史区间数据。

        每份申请独立登记进各自测点账本，交叠去重与窗口重算规则与单份
        申请完全一致；汇总文件仍按批次整份重写，避免重复汇总行。
        """
        all_replays = store.rows(self.MODULE)
        faulty = [r for r in all_replays if "故障" in (r.get("重采原因", ""))]
        results = []
        for row in faulty:
            res = self.replay_range(row["id"])
            results.append(res)
        summary_file = self._data_dir("replayed_ranges") / "batch_replay_summary.jsonl"
        self._overwrite_jsonl(summary_file, [
            {"reason": reason, "total_replayed": len(results), "at": time.time()}
        ])
        return {"reason": reason, "total_replayed": len(results), "results": results}

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _all_books(self) -> list[dict[str, Any]]:
        merge_dir = self._data_dir("replay_merge")
        books = []
        for path in sorted(merge_dir.glob("*_merge_book.json")):
            with open(path, "r", encoding="utf-8") as f:
                books.append(rm.json.load(f))
        return books

    def _overwrite_jsonl(self, filepath, records: list[dict[str, Any]]) -> int:
        """整份重写 JSONL（替代基类的追加写），保证续跑不产生重复行。"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(rm.json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return len(records)

    def _build_artifacts(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        """构造本次重采产物：只包含本次新采纳的观测。"""
        return [{
            "replay_id": run["replay_id"],
            "replay_no": run["replay_no"],
            "point": run["point"],
            "reason": run["reason"],
            "overwrite_mode": run["overwrite_mode"],
            "original_timestamp": row["timestamp"],
            "source_file": row["source_file"],
            "origin": row["origin"],
            "replay_value": row["value"],
            "replayed_at": time.time(),
        } for row in run["adopted_rows"] if row["origin"] == rm.ORIGIN_FRESH]

    def _window_value(self, window: dict[str, Any], fresh_rows: list[dict[str, Any]]) -> float:
        values = [
            float(row["value"]) for row in fresh_rows
            if window["start_epoch"] <= row["timestamp_epoch"] < window["end_epoch"]
        ]
        return round(sum(values) / len(values), 4) if values else 0.0

    def _recompute_window_rollup(
        self,
        book: dict[str, Any],
        run: dict[str, Any],
        window: dict[str, Any],
        fresh_rows: list[dict[str, Any]],
    ) -> None:
        """调用归并服务按窗口去重后的原始行重算，重算依据写回窗口。"""
        try:
            rollup_svc = self._safe_import(
                "app.services.rollup_window", "RollupWindowService")()
            result = rollup_svc.recompute_for_replay(
                point_key=book["point"],
                window=window,
                fresh_rows=fresh_rows,
                replay_no=run["replay_no"],
            )
        except Exception as exc:  # 重算失败保留待重算状态，留给续跑
            window["basis"] = f"重算失败待续跑：{exc}"
            return False
        window["rollup_ref"] = result["rollup_ref"]
        window["rollup_value"] = result["rollup_value"]
        window["basis"] = (
            f"基于去重原始行 {len(window['source_timestamps'])} 条重算，"
            f"归并产物 {result['artifact']}（按 window_id 整份重写，未追加）"
        )
        return True

    def _refresh_checkpoint(self, run: dict[str, Any], *, interrupted: bool) -> dict[str, Any]:
        recomputed = sum(
            1 for w in run["windows"]
            if w["status"] in (rm.WINDOW_RECOMPUTED, rm.WINDOW_CONFIRMED)
        )
        pending = sum(
            1 for w in run["windows"]
            if w["status"] in (rm.WINDOW_PENDING, rm.WINDOW_RUNNING)
        )
        fresh_rows = [r for r in run["adopted_rows"] if r["origin"] == rm.ORIGIN_FRESH]
        run["checkpoint"].update({
            "processed_rows": len(fresh_rows),
            "recomputed_windows": recomputed,
            "pending_windows": pending,
            "interrupted": interrupted,
            "updated_at": time.time(),
        })
        return run["checkpoint"]

    def _delivered_range(self, run: dict[str, Any]) -> dict[str, Any]:
        """本次申请当前可交付范围：已重算/已确认窗口覆盖的时段并集。"""
        usable = [
            w for w in run["windows"]
            if w["status"] in (rm.WINDOW_RECOMPUTED, rm.WINDOW_CONFIRMED)
        ]
        if not usable:
            return {"windows": [], "ranges": []}
        intervals = rm.merge_intervals([
            {"start": w["start_epoch"], "end": w["end_epoch"]} for w in usable
        ])
        return {
            "windows": [w["window_id"] for w in usable],
            "ranges": [[rm.fmt_ts(item["start"]), rm.fmt_ts(item["end"])] for item in intervals],
        }
