import logging
import threading
import time
from typing import Optional

import pytest

from pushbus import PushBus, current_provenance, safe_repr


def test_positional_inputs_and_outputs():
    bus = PushBus()
    got = []

    @bus.subscribe("in", "left", "right")
    def split(value: int):
        return value, value + 1

    @bus.subscribe("left", _outputs="seen")
    def seen(value: int):
        got.append(value)
        return value

    bus.publish(4, "in")
    assert got == [4]


def test_named_inputs_and_disposable():
    bus = PushBus()
    got = []

    @bus.subscribe(a="a/(?P<id>\\w+)", b="b/(?P<id>\\w+)", _outputs="out/{id}")
    def join(a: int, b: int):
        got.append((a, b))
        return a + b

    bus.publish(1, "a/x")
    bus.publish(2, "b/x")
    assert got == [(1, 2)]
    join.dispose()
    bus.publish(3, "a/x")
    assert got == [(1, 2)]


def test_single_input_and_extraction_capture():
    bus = PushBus()
    got = []

    @bus.subscribe(_inputs=r"in/(?P<name>[^/]+)", _outputs="out/{name}")
    def callback(value: int):
        got.append(value)
        return value + 1

    bus.publish(4, "in/alice")
    assert got == [4]


def test_shared_key_joins_and_provenance():
    bus = PushBus()
    got = []

    @bus.subscribe(
        _inputs={
            "left": r"left/(?P<id>\w+)/(?P<left_only>\w+)",
            "right": r"right/(?P<id>\w+)",
        },
        _outputs="joined/{id}/{left_only}",
    )
    def join(left: int, right: int):
        got.append((left, right, current_provenance()))
        return left + right

    bus.publish(2, "left/x/source")
    bus.publish(3, "right/x")
    assert got == [(2, 3, {"left": "left/x/source", "right": "right/x"})]


def test_no_shared_key_cartesian_and_consume():
    bus = PushBus()
    got = []

    @bus.subscribe(_inputs={"a": "a", "b": "b"}, _outputs="done")
    def consume(a: int, b: int):
        got.append((a, b))
        return None

    bus.publish(1, "a")
    bus.publish(2, "b")
    bus.publish(3, "b")
    assert got == [(1, 2)]


def test_non_consuming_join_needs_a_new_update():
    bus = PushBus()
    got = []

    @bus.subscribe(_inputs={"a": "a", "b": "b"}, _outputs="done", _consume=False)
    def retain(a: int, b: int):
        got.append((a, b))
        return None

    bus.publish(1, "a")
    bus.publish(2, "b")
    bus.publish(2, "b")
    assert got == [(1, 2), (1, 2)]


def test_optional_input_is_none_when_absent():
    bus = PushBus()
    got = []

    @bus.subscribe(_inputs={"required": "r", "maybe": "m"}, _outputs="done")
    def optional(required: int, maybe: Optional[int]):
        got.append((required, maybe))
        return None

    bus.publish(9, "r")
    assert got == [(9, None)]


def test_tuple_outputs_are_mapped_positionally():
    bus = PushBus()
    got = []

    @bus.subscribe(_inputs="in", _outputs=("left", "right"))
    def split(value: int):
        return value, value + 1

    @bus.subscribe(_inputs="left", _outputs="seen")
    def left(value: int):
        got.append(("left", value))
        return value

    @bus.subscribe(_inputs="right", _outputs="seen")
    def right(value: int):
        got.append(("right", value))
        return value

    bus.publish(5, "in")
    assert got == [("left", 5), ("right", 6)]


def test_grace_period_debounces_and_max_grace_forces():
    bus = PushBus()
    got = []
    done = threading.Event()

    @bus.subscribe(_inputs="in", _outputs="out", _grace_period=0.20, _max_grace_period=0.08)
    def delayed(value: int):
        got.append(value)
        done.set()
        return value

    bus.publish(1, "in")
    time.sleep(0.02)
    bus.publish(2, "in")
    assert not got
    assert done.wait(0.16)
    assert got == [1, 2]  # all buffered values are consumed by the forced join


def test_user_errors_are_isolated_and_payload_repr_is_bounded(caplog):
    bus = PushBus()
    good = []

    class BadRepr:
        def __repr__(self):
            raise RuntimeError("bad repr")

    @bus.subscribe(_inputs="bad", _outputs="out")
    def failing(value: object):
        raise ValueError("expected")

    @bus.subscribe(_inputs="good", _outputs="out")
    def succeeding(value: int):
        good.append(value)
        return value

    with caplog.at_level(logging.ERROR):
        bus.publish(BadRepr(), "bad")
        bus.publish(7, "good")
    assert good == [7]
    assert any("user function failed" in record.message for record in caplog.records)
    assert all(len(record.message) < 300 for record in caplog.records)


def test_statistics_are_dynamic_per_key():
    bus = PushBus()

    @bus.subscribe(_inputs=r"in/(?P<key>\w+)", _outputs="out")
    def callback(value: int):
        return value

    bus.publish(1, "in/a")
    bus.publish(2, "in/b")
    stats = bus.statistics()
    assert sum(item["calls"] for item in stats["edges"].values()) == 2
    node = next(iter(stats["nodes"].values()))
    assert node["calls"] == 2
    assert node["successes"] == 2


def test_safe_repr_limit_and_concurrent_updates():
    bus = PushBus()
    lock = threading.Lock()
    got = []

    @bus.subscribe(_inputs="in", _outputs="out", _consume=False)
    def callback(value: int):
        with lock:
            got.append(value)
        return value

    threads = [threading.Thread(target=bus.publish, args=(i, "in")) for i in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(got) == 20
    assert len(safe_repr("x" * 200)) == 100


def test_internal_output_mapping_errors_are_not_user_errors():
    bus = PushBus()

    @bus.subscribe(_inputs="in/(?P<known>\\w+)", _outputs="out/{missing}")
    def callback(value: int):
        return value

    with pytest.raises(KeyError):
        bus.publish(1, "in/x")
