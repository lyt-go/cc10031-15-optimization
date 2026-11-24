"""区间重采交叠去重与归并重算的专用路由。

通用资源路由只覆盖 CRUD 与状态动作；重采的登记、断点续跑、确认、取消
和账本查询属于领域操作，在这里显式暴露，路由层不做业务判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.range_replay import RangeReplayService

router = APIRouter(prefix="/api/range_replay", tags=["区间重采"])


class ReplayRunPayload(BaseModel):
    window_minutes: int = Field(default=RangeReplayService.DEFAULT_WINDOW_MINUTES, ge=1)
    recompute_rollup: bool = True
    # 限制本次重算的窗口数，用于模拟中断；为空表示一次跑完
    max_windows: int | None = Field(default=None, ge=1)


class RegisterPayload(BaseModel):
    window_minutes: int = Field(default=RangeReplayService.DEFAULT_WINDOW_MINUTES, ge=1)


def _svc() -> RangeReplayService:
    return RangeReplayService()


def _unwrap(result: dict[str, Any]) -> dict[str, Any]:
    result.setdefault("ok", "error" not in result)
    return result


@router.post("/{replay_id}/register")
def register_replay(replay_id: int, payload: RegisterPayload) -> dict[str, Any]:
    return _unwrap(_svc().register_replay(replay_id, window_minutes=payload.window_minutes))


@router.post("/{replay_id}/run")
def run_replay(replay_id: int, payload: ReplayRunPayload) -> dict[str, Any]:
    return _unwrap(_svc().replay_range(
        replay_id,
        window_minutes=payload.window_minutes,
        recompute_rollup=payload.recompute_rollup,
        max_windows=payload.max_windows,
    ))


@router.post("/{replay_id}/resume")
def resume_replay(replay_id: int, payload: ReplayRunPayload) -> dict[str, Any]:
    return _unwrap(_svc().resume_replay(replay_id, max_windows=payload.max_windows))


@router.post("/{replay_id}/confirm")
def confirm_replay(replay_id: int) -> dict[str, Any]:
    return _unwrap(_svc().confirm_replay(replay_id))


@router.post("/{replay_id}/cancel")
def cancel_replay(replay_id: int) -> dict[str, Any]:
    return _unwrap(_svc().cancel_replay(replay_id))


@router.get("/state/{replay_no}")
def replay_state(replay_no: str) -> dict[str, Any]:
    return _unwrap(_svc().resume_state(replay_no))


@router.get("/book/{point_key}")
def merge_book(point_key: str) -> dict[str, Any]:
    return _unwrap(_svc().merge_book(point_key))
