"""Per-``job_id`` in-memory pubsub for streamed graph events.

DESIGN.md sec 6.3: streaming only works because the client and the worker
running the graph share a process -- there's no broker, just this. Two
requirements from there, and how each is met:

- **Bounded, drop-oldest, not unbounded.** A run whose SSE client never
  connects (or disconnects and never comes back) must not make the
  worker's memory grow with every event the graph produces. Met by
  ``_Topic.buffer``: a ``collections.deque(maxlen=...)`` naturally evicts
  its oldest entry as new ones arrive -- no separate eviction logic needed.
- **A late/reconnecting subscriber replays "the available buffer", not the
  full history.** :meth:`EventBus.subscribe` seeds the new subscriber's
  queue from ``_Topic.buffer`` before adding it to the live fanout list --
  so a client that reconnects mid-run sees the same bounded tail the
  buffer holds, never more.

Per-*subscriber* queues (the live fanout side, as opposed to the shared
replay buffer) are intentionally left unbounded: each one is transient --
one per open SSE connection, continuously drained by the response
generator -- so it never accumulates the way an abandoned run's shared
buffer could. Bounding those too would just add ``QueueFull`` handling for
no real memory-safety benefit.

**Known limitation, not hidden:** topics are never pruned from
``EventBus._topics`` -- a process that runs a huge number of graphs over a
long lifetime will accumulate one entry per ``job_id`` forever. Matches
``InMemoryJobStore``'s own default in ``errand_jobs`` (unbounded unless the
caller opts into pruning); out of scope for 0.3.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

_DEFAULT_BUFFER_SIZE = 200


@dataclass
class _Topic:
    buffer: deque[dict[str, Any]]
    queues: list[asyncio.Queue[dict[str, Any] | None]] = field(default_factory=list)
    closed: bool = False
    seq: int = 0


class EventBus:
    """Publishes graph-stream events per ``job_id``, fans out to subscribers.

    Example::

        bus = EventBus()
        async for event in bus.subscribe(job_id):
            ...  # events published by another task, as they arrive

        # elsewhere, from the run itself:
        bus.publish(job_id, {"type": "chunk", "data": {...}})
        bus.close(job_id)  # signals subscribers the run is done
    """

    def __init__(self, *, buffer_size: int = _DEFAULT_BUFFER_SIZE) -> None:
        self._buffer_size = buffer_size
        self._topics: dict[str, _Topic] = {}

    def _topic(self, job_id: str) -> _Topic:
        topic = self._topics.get(job_id)
        if topic is None:
            topic = _Topic(buffer=deque(maxlen=self._buffer_size))
            self._topics[job_id] = topic
        return topic

    def publish(self, job_id: str, event: dict[str, Any]) -> None:
        """Publish ``event`` to ``job_id``'s topic and every live subscriber."""
        topic = self._topic(job_id)
        topic.seq += 1
        envelope = {"seq": topic.seq, **event}
        topic.buffer.append(envelope)
        for queue in topic.queues:
            queue.put_nowait(envelope)

    def close(self, job_id: str) -> None:
        """Mark ``job_id``'s topic finished -- no more events will arrive.

        Safe to call even if nothing was ever published (e.g. a run that
        failed before its first event): still creates the topic and closes
        it, so a subscriber that only starts listening after the run is
        long over gets an immediate, clean end-of-stream instead of hanging.
        """
        topic = self._topic(job_id)
        topic.closed = True
        for queue in topic.queues:
            queue.put_nowait(None)

    async def subscribe(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        """Yield ``job_id``'s buffered tail, then live events as they arrive.

        Returns (stops iterating) once :meth:`close` has been called and
        every buffered/live event has been yielded.
        """
        topic = self._topic(job_id)
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        for envelope in topic.buffer:
            queue.put_nowait(envelope)
        if topic.closed:
            queue.put_nowait(None)
        else:
            topic.queues.append(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            if queue in topic.queues:
                topic.queues.remove(queue)
