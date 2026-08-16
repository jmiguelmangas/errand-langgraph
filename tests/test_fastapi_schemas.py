from __future__ import annotations

from typing import Any

from errand_langgraph.fastapi.schemas import build_input_type


def test_build_input_type_falls_back_to_dict_on_introspection_failure() -> None:
    class _NotIntrospectable:
        def get_input_schema(self) -> Any:
            raise RuntimeError("no schema for you")

    assert build_input_type(_NotIntrospectable()) == dict[str, Any]
