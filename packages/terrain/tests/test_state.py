import pytest

from bag3d.terrain.state import checkpoint, file_record, valid_record


def test_checkpoint_resumes_and_repairs_changed_or_missing_outputs(tmp_path):
    target = tmp_path / "data"
    calls = []

    def produce():
        calls.append(True)
        target.write_bytes(b"complete")
        return [target]

    marker = tmp_path / "checkpoint.json"
    checkpoint(marker, {"input": "a"}, produce)
    checkpoint(marker, {"input": "a"}, produce)
    assert len(calls) == 1
    target.write_bytes(b"corrupt!")
    checkpoint(marker, {"input": "a"}, produce)
    target.unlink()
    checkpoint(marker, {"input": "a"}, produce)
    checkpoint(marker, {"input": "b"}, produce)
    assert len(calls) == 4
    assert valid_record(file_record(target))


def test_failed_work_is_not_checkpointed(tmp_path):
    def fail():
        raise RuntimeError("interrupted")

    marker = tmp_path / "checkpoint.json"
    with pytest.raises(RuntimeError, match="interrupted"):
        checkpoint(marker, {}, fail)
    assert not marker.exists()
