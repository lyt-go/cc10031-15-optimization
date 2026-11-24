"""批量故障重采交叠去重与归并重算的集成测试。

场景：同一测点两份重采申请
  A: 10:00~10:30（先完成并确认，等价于此前告警确认过的数据）
  B: 10:20~10:40（续跑，与 A 在 10:20~10:30 交叠）
验证：原始行血缘、各次补采覆盖范围、交叠窗口重算依据、中断/成功申请
分别可恢复、同一观测不会被算两次、取消不影响他人已确认结果。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services import replay_merge as rm
from app.services.base import DATA_ROOT


def _write_calibrated(tmp: Path, point: str, rows: list[dict]) -> Path:
    cal_dir = tmp / "calibrated"
    cal_dir.mkdir(parents=True, exist_ok=True)
    f = cal_dir / f"{point}_calibrated.jsonl"
    with open(f, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return f


def _samples(point: str) -> list[dict]:
    """10:00 起每 5 分钟一条，到 10:35；10:10 再插一条重复时间戳。"""
    rows = []
    for minute in range(0, 40, 5):
        rows.append({
            "point": point,
            "timestamp": f"2026-11-24T10:{minute:02d}:00",
            "calibrated_value_V": 1.0 + minute / 10.0,
        })
    rows.append({
        "point": point,
        "timestamp": "2026-11-24T10:10:00",
        "calibrated_value_V": 9.99,  # 与既有 10:10 观测同刻，去重时丢弃
    })
    return rows


def _make_replay(replay_id: int, no: str, point: str, start: str, end: str) -> None:
    from app.store import store
    store.rows("range_replay").append({
        "id": replay_id,
        "重采编号": no,
        "测点编号": point,
        "起始时刻": start,
        "截止时刻": end,
        "重采原因": "传感器故障",
        "覆盖方式": "追加",
        "重采点数": "0",
        "重采水位": "0",
        "status": "草稿",
        "pending": True,
        "abnormal": False,
    })


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            import app.services.range_replay as rr
            import app.services.rollup_window as rw
            rr.DATA_ROOT = Path(tmp)
            rw.DATA_ROOT = Path(tmp)
            yield Path(tmp)


class TestReplayMergeEngine:
    """纯引擎：区间并集、切窗、去重。"""

    def test_merge_intervals_unifies_overlap(self):
        merged = rm.merge_intervals([
            {"start": "2026-11-24T10:00:00", "end": "2026-11-24T10:30:00"},
            {"start": "2026-11-24T10:20:00", "end": "2026-11-24T10:40:00"},
        ])
        assert merged == [{
            "start": rm.parse_ts("2026-11-24T10:00:00"),
            "end": rm.parse_ts("2026-11-24T10:40:00"),
        }]

    def test_split_windows_marks_confirmed_overlap(self):
        confirmed = [{"start": rm.parse_ts("2026-11-24T10:00:00"),
                      "end": rm.parse_ts("2026-11-24T10:30:00")}]
        windows = rm.split_windows(
            requested={"start": "2026-11-24T10:20:00", "end": "2026-11-24T10:40:00"},
            confirmed_union=confirmed,
            window_minutes=5,
            replay_no="RANG-B",
        )
        assert len(windows) == 4
        assert [w["status"] for w in windows] == [
            rm.WINDOW_CONFIRMED, rm.WINDOW_CONFIRMED,
            rm.WINDOW_PENDING, rm.WINDOW_PENDING,
        ]
        assert "沿用已确认重算结果" in windows[0]["basis"]

    def test_dedupe_drops_duplicate_timestamp_and_flags_confirmed(self):
        rows = _samples("SENS-X")
        confirmed = [{"start": rm.parse_ts("2026-11-24T10:00:00"),
                      "end": rm.parse_ts("2026-11-24T10:30:00")}]
        lo = rm.parse_ts("2026-11-24T10:20:00")
        hi = rm.parse_ts("2026-11-24T10:40:00")
        adopted, dropped = rm.dedupe_source_rows(
            rows, lo, hi, confirmed, "RANG-B", "calibrated.jsonl")
        origins = {row["timestamp"]: row["origin"] for row in adopted}
        # 交叠段两行只作血缘引用，新时段两行为本次采纳
        assert origins["2026-11-24T10:20:00"] == rm.ORIGIN_CONFIRMED
        assert origins["2026-11-24T10:25:00"] == rm.ORIGIN_CONFIRMED
        assert origins["2026-11-24T10:30:00"] == rm.ORIGIN_FRESH
        assert origins["2026-11-24T10:35:00"] == rm.ORIGIN_FRESH
        assert len(adopted) == 4
        # 10:10 的重复时间戳不在 [10:20,10:40) 区间内，区间内无重复可丢
        assert dropped == []

    def test_dedupe_drops_duplicate_inside_range(self, setup_data_dir):
        rows = _samples("SENS-X")
        lo = rm.parse_ts("2026-11-24T10:00:00")
        hi = rm.parse_ts("2026-11-24T10:30:00")
        adopted, dropped = rm.dedupe_source_rows(
            rows, lo, hi, [], "RANG-A", "calibrated.jsonl")
        assert len(adopted) == 6
        assert len(dropped) == 1
        assert dropped[0]["timestamp"] == "2026-11-24T10:10:00"


class TestTwoOverlappingReplays:
    """题面主场景：A 先完成确认，B 续跑处理交叠。"""

    def test_complete_a_then_run_b_with_overlap(self, setup_data_dir):
        from app.services.range_replay import RangeReplayService
        from app.store import store

        point = "SENS-OVL"
        _write_calibrated(setup_data_dir, point, _samples(point))
        _make_replay(301, "RANG-A", point,
                     "2026-11-24T10:00:00", "2026-11-24T10:30:00")
        _make_replay(302, "RANG-B", point,
                     "2026-11-24T10:20:00", "2026-11-24T10:40:00")

        svc = RangeReplayService()

        # ---- 申请 A：登记 -> 执行 -> 确认（模拟此前告警确认过的数据）----
        reg_a = svc.register_replay(301, window_minutes=5)
        assert reg_a["window_count"] == 6
        assert reg_a["pending_windows"] == [f"RANG-A#W{i}" for i in range(1, 7)]

        run_a = svc.replay_range(301)
        assert "error" not in run_a
        assert run_a["records_fresh"] == 6          # 10:00..10:25
        assert run_a["duplicates_dropped"] == 1     # 10:10 重复时间戳
        assert run_a["interrupted"] is False
        assert run_a["delivered_range"]["ranges"] == [
            ["2026-11-24T10:00:00", "2026-11-24T10:30:00"]]

        confirm_a = svc.confirm_replay(301)
        assert confirm_a["confirmed_windows"] == 6
        assert store.find("range_replay", 301)["status"] == "已完成"

        # A 的产物：6 条观测，整份重写无重复
        art_a = setup_data_dir / "replayed_ranges" / "RANG-A_replay.jsonl"
        a_lines = [json.loads(line) for line in art_a.read_text().strip().split("\n")]
        assert len(a_lines) == 6
        assert len({line["original_timestamp"] for line in a_lines}) == 6

        # ---- 申请 B：与 A 在 10:20~10:30 交叠 ----
        reg_b = svc.register_replay(302, window_minutes=5)
        assert reg_b["confirmed_overlap_windows"] == ["RANG-B#W1", "RANG-B#W2"]
        assert reg_b["pending_windows"] == ["RANG-B#W3", "RANG-B#W4"]

        run_b = svc.replay_range(302)
        # 交叠两行只作血缘引用，新观测只有 10:30、10:35 两条
        assert run_b["records_adopted"] == 4
        assert run_b["records_from_confirmed"] == 2
        assert run_b["records_fresh"] == 2
        assert run_b["recomputed_windows"] == 4    # 2 已确认 + 2 新重算
        assert run_b["pending_windows"] == 0

        # B 的产物绝不能再把 10:20/10:25 并作两次观测
        art_b = setup_data_dir / "replayed_ranges" / "RANG-B_replay.jsonl"
        b_lines = [json.loads(line) for line in art_b.read_text().strip().split("\n")]
        b_timestamps = {line["original_timestamp"] for line in b_lines}
        assert b_timestamps == {
            "2026-11-24T10:30:00", "2026-11-24T10:35:00"}
        assert len(b_lines) == 2

        # 归并产物：8 个 window_id 各一条，交叠窗口未进重算文件
        rollup_file = setup_data_dir / "rollup_windows" / f"{point}_replay_rollup.jsonl"
        rollup_rows = [json.loads(line) for line in rollup_file.read_text().strip().split("\n")]
        rollup_window_ids = {row["window_id"] for row in rollup_rows}
        assert rollup_window_ids == {f"RANG-A#W{i}" for i in range(1, 7)} | {
            "RANG-B#W3", "RANG-B#W4"}
        assert "RANG-B#W1" not in rollup_window_ids

        # 账本可追踪：原始行血缘、覆盖范围、窗口状态与依据
        book = svc.merge_book(point)
        assert book["confirmed_ranges"]  # 点级已确认时段保留
        run_b_state = svc.resume_state("RANG-B")
        statuses = {w["window_id"]: w["status"] for w in run_b_state["windows"]}
        assert statuses["RANG-B#W1"] == rm.WINDOW_CONFIRMED
        assert statuses["RANG-B#W3"] == rm.WINDOW_RECOMPUTED
        assert "去重原始行" in run_b_state["windows"][2]["basis"]

        # 重复提交 B 被拒，不会产生第二份观测
        again = svc.register_replay(302, window_minutes=5)
        assert "error" in again
        # 对已完成申请再跑一次：走恢复分支，产物行数不变
        svc.replay_range(302)
        b_lines_again = [json.loads(line) for line in art_b.read_text().strip().split("\n")]
        assert len(b_lines_again) == 2

    def test_interrupted_replay_resumes_without_double_counting(self, setup_data_dir):
        from app.services.range_replay import RangeReplayService

        point = "SENS-INT"
        _write_calibrated(setup_data_dir, point, _samples(point))
        _make_replay(303, "RANG-C", point,
                     "2026-11-24T10:00:00", "2026-11-24T10:30:00")

        svc = RangeReplayService()
        svc.register_replay(303, window_minutes=5)

        # 第一次只算 2 个窗口，模拟中断
        first = svc.replay_range(303, max_windows=2)
        assert first["interrupted"] is True
        assert first["recomputed_windows"] == 2
        assert first["pending_windows"] == 4
        artifact = setup_data_dir / "replayed_ranges" / "RANG-C_replay.jsonl"
        rows_after_first = [json.loads(x) for x in artifact.read_text().strip().split("\n")]

        # 中断态可独立恢复：检查点落盘
        state = svc.resume_state("RANG-C")
        assert state["status"] == "重采中"
        assert state["checkpoint"]["pending_windows"] == 4
        assert state["checkpoint"]["interrupted"] is True
        cp_file = setup_data_dir / "checkpoints" / "replay_RANG-C.json"
        assert cp_file.exists()

        # 续跑：剩余 4 个窗口补算，观测不翻倍
        second = svc.resume_replay(303)
        assert second["interrupted"] is False
        assert second["recomputed_windows"] == 6
        assert second["pending_windows"] == 0
        rows_after_resume = [json.loads(x) for x in artifact.read_text().strip().split("\n")]
        timestamps = [row["original_timestamp"] for row in rows_after_resume]
        assert len(timestamps) == 6
        assert len(set(timestamps)) == 6  # 同一观测未被算两次

        # 归并窗口同样只有 6 条，无重复 window_id
        rollup_file = setup_data_dir / "rollup_windows" / f"{point}_replay_rollup.jsonl"
        rollup_rows = [json.loads(x) for x in rollup_file.read_text().strip().split("\n")]
        assert len(rollup_rows) == 6
        assert len({row["window_id"] for row in rollup_rows}) == 6

    def test_cancel_keeps_other_runs_confirmed_results(self, setup_data_dir):
        from app.services.range_replay import RangeReplayService
        from app.store import store

        point = "SENS-CAN"
        _write_calibrated(setup_data_dir, point, _samples(point))
        _make_replay(304, "RANG-D", point,
                     "2026-11-24T10:00:00", "2026-11-24T10:30:00")
        _make_replay(305, "RANG-E", point,
                     "2026-11-24T10:20:00", "2026-11-24T10:40:00")

        svc = RangeReplayService()
        svc.replay_range(304)
        svc.confirm_replay(304)

        # E 中断后取消
        svc.register_replay(305, window_minutes=5)
        svc.replay_range(305, max_windows=1)
        cancel = svc.cancel_replay(305)
        # D 的已确认结果原样保留
        assert cancel["confirmed_ranges_kept"]
        assert store.find("range_replay", 304)["status"] == "已完成"
        assert store.find("range_replay", 305)["status"] == "已取消"

        state_e = svc.resume_state("RANG-E")
        # 交叠窗口沿用 D 的确认事实，取消不影响它们
        assert state_e["windows"][0]["status"] == rm.WINDOW_CONFIRMED
        assert state_e["windows"][2]["status"] == rm.WINDOW_CANCELLED

        # 已取消申请不可续跑
        again = svc.resume_replay(305)
        assert "error" in again

        # D 仍可独立恢复，交付范围完整
        state_d = svc.resume_state("RANG-D")
        assert state_d["status"] == "已完成"
        assert state_d["delivered_range"]["ranges"] == [
            ["2026-11-24T10:00:00", "2026-11-24T10:30:00"]]

    def test_invalid_range_rejected(self, setup_data_dir):
        from app.services.range_replay import RangeReplayService

        point = "SENS-BAD"
        _write_calibrated(setup_data_dir, point, _samples(point))
        _make_replay(306, "RANG-F", point,
                     "2026-11-24T10:30:00", "2026-11-24T10:00:00")
        result = RangeReplayService().register_replay(306)
        assert "error" in result
