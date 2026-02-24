from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel


M = TypeVar("M", bound=BaseModel)


def build_model(
    model_cls: type[M],
    payload: dict[str, Any],
    *,
    strict_validation: bool,
) -> M:
    """Build model with optional strict pydantic validation."""
    if strict_validation:
        return model_cls.model_validate(payload)
    return model_cls.model_construct(**payload)
