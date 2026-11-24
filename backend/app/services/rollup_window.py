"""窗口归并领域服务：维护固定、滑动与会话窗口的时序归并聚合指标与窗口条数。
对降采样后的序列数据按窗口类型进行窗口归并计算，生成归并结果数据集。"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RollupWindowService(BaseModuleService):
    MODULE = "rollup_window"
    ENTITY = "归并窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "测点编号", "窗口类型", "窗口长度", "滑动步长", "聚合指标", "窗口条数", "归并水位"]
    REQUIRED_FIELDS = ["窗口编号", "测点编号", "窗口类型"]
    STATUS_ORDER = ["草稿", "待归并", "已归并", "已作废"]
    ACTION_RULES = {"启动归并": "待归并", "确认归并": "已归并", "标记作废": "已作废"}
    # 数据归并操作：按窗口归并规则执行聚合计算生成归并结果数据集
    NEGATIVE_ACTIONS = []

    def rollup_windows(self, rollup_id: int) -> dict[str, Any]:
        """按窗口归并规则对降采样后的时序数据执行聚合计算，生成归并结果数据集。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        self.run_action(rollup_id, "启动归并")
        point_key = entry["测点编号"]
        window_type = entry.get("窗口类型", "固定窗口")
        window_len = int(entry.get("窗口长度", "5"))
        agg_metric = entry.get("聚合指标", "均值")

        # 读取降采样后的数据
        ds_files = list(self._data_dir("downsampled").glob(f"*{point_key}*_downsampled.jsonl")) if self._data_dir("downsampled").exists() else []
        records = []
        for f in ds_files[:3]:
            records.extend(self._read_jsonl(f))
        if not records:
            filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
            records = self._read_jsonl(filtered_file)

        # 按窗口类型执行归并聚合计算
        merged = []
        for i in range(0, len(records), window_len):
            window = records[i:i + window_len]
            values = []
            # 数据产物输出：写入归并聚合结果到窗口数据JSONL文件
            for r in window:
                v = r.get("aggregated_value", r.get("calibrated_value_V", 0))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
            if not values:
                continue
            if agg_metric in ("均值", "avg"):
                agg_val = round(sum(values) / len(values), 4)
            elif agg_metric in ("求和", "sum"):
                agg_val = round(sum(values), 4)
            elif agg_metric in ("最大值", "max"):
                agg_val = round(max(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            merged.append({
                "window_id": entry["窗口编号"],
                "point": point_key,
                "window_type": window_type,
                "window_start": i + 1,
                "window_end": i + len(window),
                "record_count": len(window),
                "rollup_value": agg_val,
                "metric": agg_metric,
            })
        # 将窗口归并结果写入归并窗口数据文件
        output_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        written = self._write_jsonl(output_file, merged)
        entry["窗口条数"] = str(written)
        entry["归并水位"] = str(written)
        self.run_action(rollup_id, "确认归并")
        return {"rollup_id": entry["窗口编号"], "point": point_key,
                "window_type": window_type, "window_period_count": written,
                "metric": agg_metric}

    # ------------------------------------------------------------------
    # 重采交叠窗口重算：由 range_replay 账本驱动，按 window_id 整份重写
    # ------------------------------------------------------------------

    def recompute_for_replay(
        self,
        *,
        point_key: str,
        window: dict[str, Any],
        fresh_rows: list[dict[str, Any]],
        replay_no: str,
        metric: str = "均值",
    ) -> dict[str, Any]:
        """对单个归并窗口按去重后的本次重采原始行执行重算。

        关键去重保证：
        - 输入 ``fresh_rows`` 已经过账本去重，同一时刻只有一条观测；
        - 产物文件按 window_id 整体重写（非追加），续跑/重复提交不会
          把同一窗口的观测并作两次；
        - 交叠窗口由重采服务在调用前判为「已确认」，不会进入本方法。
        """
        values = [
            float(row["value"]) for row in fresh_rows
            if window["start_epoch"] <= row["timestamp_epoch"] < window["end_epoch"]
        ]
        if metric in ("求和", "sum"):
            agg_val = round(sum(values), 4)
        elif metric in ("最大值", "max"):
            agg_val = round(max(values), 4) if values else 0.0
        else:
            agg_val = round(sum(values) / len(values), 4) if values else 0.0

        record = {
            "window_id": window["window_id"],
            "replay_no": replay_no,
            "point": point_key,
            "window_start": window["start"],
            "window_end": window["end"],
            "record_count": len(values),
            "source_timestamps": window.get("source_timestamps", []),
            "rollup_value": agg_val,
            "metric": metric,
            "recomputed_at": time.time(),
        }
        output_file = self._data_dir("rollup_windows") / f"{point_key}_replay_rollup.jsonl"
        self._overwrite_window_record(output_file, window["window_id"], record)
        return {
            "window_id": window["window_id"],
            "rollup_ref": f"rollup:{point_key}:{window['window_id']}",
            "rollup_value": agg_val,
            "artifact": str(output_file.name),
        }

    def _overwrite_window_record(self, filepath, window_id: str, record: dict) -> None:
        """按 window_id 整份重写归并产物，同名窗口只保留最新一条。"""
        existing = [r for r in self._read_jsonl(filepath) if r.get("window_id") != window_id]
        existing.append(record)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            for row in existing:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def compute_cumulative_rollup(self, rollup_id: int) -> dict[str, Any]:
        """对归并窗口数据计算累计聚合指标写入累计快照数据文件。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        # 先执行归并
        rollup_result = self.rollup_windows(rollup_id)
        if "error" in rollup_result:
            return rollup_result
        # 读取归并结果计算累计
        rollup_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        records = self._read_jsonl(rollup_file)
        cumulative = []
        running_sum = 0.0
        for i, r in enumerate(records):
            val = float(r.get("rollup_value", 0))
            running_sum += val
            cumulative.append({
                "window_index": i + 1,
                "point": r["point"],
                "window_value": val,
                "cumulative_value": round(running_sum, 4),
                "window_id": r["window_id"],
            })
        # 写入累计聚合数据文件
        cum_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_cumulative.jsonl"
        self._write_jsonl(cum_file, cumulative)
        return {"rollup_id": entry["窗口编号"], "cumulative_periods": len(cumulative),
                "final_cumulative_value": round(running_sum, 4)}