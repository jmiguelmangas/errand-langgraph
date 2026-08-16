from __future__ import annotations

import asyncio

from errand_langgraph.events import EventBus


async def test_publish_then_subscribe_replays_buffer() -> None:
    bus = EventBus()
    bus.publish("job-1", {"type": "chunk", "data": 1})
    bus.publish("job-1", {"type": "chunk", "data": 2})
    bus.close("job-1")

    events = [event async for event in bus.subscribe("job-1")]

    assert [e["data"] for e in events] == [1, 2]
    assert [e["seq"] for e in events] == [1, 2]


async def test_subscribe_before_publish_gets_live_events() -> None:
    bus = EventBus()
    received: list[dict] = []

    async def _consume() -> None:
        async for event in bus.subscribe("job-1"):
            received.append(event)

    consumer = asyncio.create_task(_consume())
    await asyncio.sleep(0)  # let the consumer subscribe before we publish

    bus.publish("job-1", {"type": "chunk", "data": "a"})
    bus.publish("job-1", {"type": "chunk", "data": "b"})
    bus.close("job-1")

    await asyncio.wait_for(consumer, timeout=2.0)
    assert [e["data"] for e in received] == ["a", "b"]


async def test_subscribe_to_never_published_topic_closes_immediately() -> None:
    bus = EventBus()
    bus.close("job-1")  # e.g. a run that failed before its first event

    events = [event async for event in bus.subscribe("job-1")]
    assert events == []


async def test_buffer_drops_oldest_beyond_buffer_size() -> None:
    bus = EventBus(buffer_size=2)
    bus.publish("job-1", {"data": 1})
    bus.publish("job-1", {"data": 2})
    bus.publish("job-1", {"data": 3})
    bus.close("job-1")

    events = [event async for event in bus.subscribe("job-1")]
    assert [e["data"] for e in events] == [2, 3]


async def test_late_subscriber_replays_buffer_then_gets_live_events() -> None:
    bus = EventBus()
    bus.publish("job-1", {"data": "before"})

    received: list[dict] = []

    async def _consume() -> None:
        async for event in bus.subscribe("job-1"):
            received.append(event)

    consumer = asyncio.create_task(_consume())
    await asyncio.sleep(0)

    bus.publish("job-1", {"data": "after"})
    bus.close("job-1")

    await asyncio.wait_for(consumer, timeout=2.0)
    assert [e["data"] for e in received] == ["before", "after"]
