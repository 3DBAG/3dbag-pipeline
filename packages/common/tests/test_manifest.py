from pathlib import Path

import pytest

from bag3d.common.utils import manifest as manifest_utils


@pytest.fixture(autouse=True)
def clear_manifest_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(manifest_utils, "_manifest_cache", None)


def test_find_manifest_prefers_env_var(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_manifest = tmp_path / "custom-manifest.json"
    env_manifest.write_text('{"tools": {}}', encoding="utf-8")

    monkeypatch.setenv("BAG3D_MANIFEST_PATH", str(env_manifest))

    assert manifest_utils._find_manifest() == env_manifest


def test_find_manifest_uses_current_working_directory_parents(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest_path = tmp_path / "3dbag-manifest.json"
    manifest_path.write_text('{"tools": {}}', encoding="utf-8")
    nested_dir = tmp_path / "packages" / "export" / "src"
    nested_dir.mkdir(parents=True)

    monkeypatch.delenv("BAG3D_MANIFEST_PATH", raising=False)
    monkeypatch.chdir(nested_dir)

    assert manifest_utils._find_manifest() == manifest_path


def test_find_manifest_uses_module_parents_when_cwd_has_no_manifest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest_path = tmp_path / "repo" / "3dbag-manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text('{"tools": {}}', encoding="utf-8")

    module_path = (
        manifest_path.parent
        / "packages"
        / "common"
        / "src"
        / "bag3d"
        / "common"
        / "utils"
        / "manifest.py"
    )
    module_path.parent.mkdir(parents=True)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()

    monkeypatch.delenv("BAG3D_MANIFEST_PATH", raising=False)
    monkeypatch.chdir(outside_dir)
    monkeypatch.setattr(manifest_utils, "__file__", str(module_path))

    assert manifest_utils._find_manifest() == manifest_path


def test_find_manifest_raises_when_no_candidates_exist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    module_path = outside_dir / "bag3d" / "common" / "utils" / "manifest.py"
    module_path.parent.mkdir(parents=True)

    monkeypatch.delenv("BAG3D_MANIFEST_PATH", raising=False)
    monkeypatch.chdir(outside_dir)
    monkeypatch.setattr(manifest_utils, "__file__", str(module_path))

    with pytest.raises(FileNotFoundError, match="Cannot find 3dbag-manifest.json"):
        manifest_utils._find_manifest()
