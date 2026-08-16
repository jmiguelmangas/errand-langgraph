"""FastAPI adapter for errand-langgraph.

``errand_langgraph`` never imports this subpackage itself, and this module
only imports the real ``fastapi`` package lazily inside :func:`mount_graph`
-- so neither ``import errand_langgraph`` nor ``import
errand_langgraph.fastapi`` requires FastAPI to be installed; only calling
``mount_graph`` does.
"""

from __future__ import annotations

from .router import mount_graph

__all__ = ["mount_graph"]
