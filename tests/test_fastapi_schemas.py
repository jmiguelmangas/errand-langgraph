from __future__ import annotations

from typing import Any

import pytest

from errand_langgraph.fastapi.schemas import build_input_type


def test_build_input_type_falls_back_to_dict_on_introspection_failure() -> None:
    class _NotIntrospectable:
        def get_input_schema(self) -> Any:
            raise RuntimeError("no schema for you")

    with pytest.warns(UserWarning, match="falling back to dict"):
        result = build_input_type(_NotIntrospectable())

    assert result == dict[str, Any]
