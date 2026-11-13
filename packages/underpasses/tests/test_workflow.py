"""Behavioral tests for bounded execution and stage processing."""

from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
from bag3d.common.types import PostgresTableIdentifier
from shapely.geometry import MultiPolygon, box

from bag3d.underpasses import workflow
from bag3d.underpasses.assets.underpasses import UnderpassesConfig


def test_stages_keep_original_polygon_boundaries():
    bag = box(0, 0, 10, 10)
    bgt = box(0, 0, 6, 10)
    count, initial = workflow.process_chunk(
        [("building", bag, bgt)],
        stage="bag_minus_bgt",
        buffer_distance=0.2,
        snap_tolerance=0.01,
    )
    assert count == 1
    assert initial[0][1].equals(box(6, 0, 10, 10))
    _, snapped = workflow.process_chunk(
        [("building", bag, bgt)],
        stage="snapped_differences",
        buffer_distance=0.2,
        snap_tolerance=0.01,
    )
    _, final = workflow.process_chunk(
        [(identificatie, geom, None) for identificatie, geom in snapped],
        stage="geometries",
        buffer_distance=0.2,
        snap_tolerance=0.01,
    )
    assert len(final) == 1
    assert final[0][1].equals(box(6, 0, 10, 10))


def test_final_filter_splits_parts_and_discards_slivers():
    geometry = MultiPolygon([box(0, 0, 2, 2), box(4, 0, 6, 2), box(8, 0, 8.1, 2)])
    _, rows = workflow.process_chunk(
        [("building", geometry, None)],
        stage="geometries",
        buffer_distance=0.2,
        snap_tolerance=0.01,
    )
    assert len(rows) == 2
    assert all(geom.area == 4 for _, geom in rows)


def test_ordered_results_bounds_prefetch_and_ignores_completion_order(monkeypatch):
    state = {"consumed": 0, "yielded": 0, "max_ahead": 0}
    completion_order = []
    waiting = []

    class Executor:
        def __init__(self, max_workers):
            assert max_workers == 2

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, function, item):
            future = Future()
            waiting.append((item, future))
            # Complete the initial four tasks in reverse order.
            if len(waiting) == 4:
                for value, pending in reversed(waiting):
                    pending.set_result(function(value))
                    completion_order.append(value)
            elif item >= 4:
                future.set_result(function(item))
            return future

    def chunks():
        for item in range(20):
            state["consumed"] += 1
            state["max_ahead"] = max(
                state["max_ahead"], state["consumed"] - state["yielded"]
            )
            yield item

    monkeypatch.setattr(workflow, "ProcessPoolExecutor", Executor)
    result = []
    for item in workflow.ordered_results(lambda value: value * 2, chunks(), 2):
        result.append(item)
        state["yielded"] += 1
    assert result == list(range(0, 40, 2))
    assert completion_order == [3, 2, 1, 0]
    assert state["max_ahead"] == 4


@pytest.mark.parametrize("concurrency", [1, 2])
def test_processing_failure_propagates(concurrency):
    # int() is picklable, so this exercises real worker failure propagation too.
    with pytest.raises(ValueError):
        list(workflow.ordered_results(int, ["1", "invalid", "3"], concurrency))


def test_database_failure_propagates(monkeypatch):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = RuntimeError("database unavailable")
    monkeypatch.setattr(workflow, "connect", lambda _: connection)
    with pytest.raises(RuntimeError, match="database unavailable"):
        workflow.run_geometry_stage(
            "unused",
            "geometries",
            PostgresTableIdentifier("test", "source"),
            PostgresTableIdentifier("test", "target"),
            concurrency=1,
            chunk_size=1,
            buffer_distance=0.2,
            snap_tolerance=0.01,
        )


@pytest.mark.parametrize(
    "values",
    [
        {"concurrency": 0},
        {"chunk_size": 0},
        {"snap_tolerance": -0.01},
        {"buffer_distance": -0.2},
        {"buffer_distance": float("inf")},
        {"snap_tolerance": float("nan")},
    ],
)
def test_invalid_configuration_rejected(values):
    with pytest.raises(ValueError):
        UnderpassesConfig(**values)


def test_configuration_defaults_and_environment(monkeypatch):
    monkeypatch.delenv("BAG3D_CONCURRENCY_TOOL_UNDERPASSES", raising=False)
    config = UnderpassesConfig()
    assert (config.concurrency, config.chunk_size) == (4, 1000)
    assert (config.snap_tolerance, config.buffer_distance) == (0.01, 0.2)
    monkeypatch.setenv("BAG3D_CONCURRENCY_TOOL_UNDERPASSES", "2")
    assert UnderpassesConfig().concurrency == 2
