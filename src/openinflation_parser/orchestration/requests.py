from __future__ import annotations

from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict

from .models import normalize_city_id


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PingRequest(RequestModel):
    action: Literal["ping"]


class SubmitStoreRequest(RequestModel):
    action: Literal["submit_store"]
    store_code: str
    parser: str | None = None
    output_dir: str | None = None
    country_id: int | None = None
    city_id: int | str | None = None
    api_timeout_ms: float | None = None
    category_limit: int | None = None
    pages_per_category: int | None = None
    max_pages_per_category: int | None = None
    products_per_page: int | None = None
    full_catalog: bool | None = None
    include_images: bool | None = None
    strict_validation: bool | None = None


class StatusRequest(RequestModel):
    action: Literal["status"]
    job_id: str | None = None


class JobsRequest(RequestModel):
    action: Literal["jobs"]


class WorkersRequest(RequestModel):
    action: Literal["workers"]


class ShutdownRequest(RequestModel):
    action: Literal["shutdown"]


class HelpRequest(RequestModel):
    action: Literal["help"]


class UnknownRequest(RequestModel):
    action: str


ParsedRequest: TypeAlias = (
    PingRequest
    | SubmitStoreRequest
    | StatusRequest
    | JobsRequest
    | WorkersRequest
    | ShutdownRequest
    | HelpRequest
    | UnknownRequest
)


ACTION_TO_MODEL: dict[str, type[RequestModel]] = {
    "ping": PingRequest,
    "submit_store": SubmitStoreRequest,
    "status": StatusRequest,
    "jobs": JobsRequest,
    "workers": WorkersRequest,
    "shutdown": ShutdownRequest,
    "help": HelpRequest,
}


def parse_request(payload: dict[str, Any]) -> ParsedRequest:
    action_raw = payload.get("action")
    if not isinstance(action_raw, str) or not action_raw.strip():
        raise ValueError("Field 'action' is required.")

    action = action_raw.strip().lower()
    model_cls = ACTION_TO_MODEL.get(action)
    if model_cls is None:
        return UnknownRequest(action=action)

    normalized = dict(payload)
    normalized["action"] = action
    if action == "submit_store":
        normalized["city_id"] = normalize_city_id(normalized.get("city_id"))
    return model_cls.model_validate(normalized)
