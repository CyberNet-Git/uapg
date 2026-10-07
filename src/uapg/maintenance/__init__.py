"""Обслуживание схемы истории на работающей БД."""

from .online_indexes import (
    ALL_SCOPES,
    CORE_INDEX_SPECS,
    MAINTENANCE_APPLICATION_NAME,
    SCOPE_CORE,
    SCOPE_TRGM,
    SCOPE_V2,
    V2_INDEX_SPECS,
    IndexApplyResult,
    IndexPlan,
    IndexPlanItem,
    IndexSpec,
    apply_plan,
    build_plan,
    connect_maintenance,
    trgm_index_spec,
)

__all__ = [
    "ALL_SCOPES",
    "CORE_INDEX_SPECS",
    "MAINTENANCE_APPLICATION_NAME",
    "SCOPE_CORE",
    "SCOPE_TRGM",
    "SCOPE_V2",
    "V2_INDEX_SPECS",
    "IndexApplyResult",
    "IndexPlan",
    "IndexPlanItem",
    "IndexSpec",
    "apply_plan",
    "build_plan",
    "connect_maintenance",
    "trgm_index_spec",
]
