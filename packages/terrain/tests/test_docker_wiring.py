"""The Docker UI must discover the same workflow as the Python workspace."""

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]


def load_yaml(path):
    return yaml.safe_load((ROOT / path).read_text())


def test_terrain_workspace_has_a_matching_code_server():
    workspace = load_yaml("docker/dagster/workspace.yaml")
    endpoint = next(
        entry["grpc_server"]
        for entry in workspace["load_from"]
        if entry["grpc_server"]["location_name"] == "terrain"
    )
    services = load_yaml("docker/compose.yaml")["services"]
    server = services[endpoint["host"]]
    dockerfile = ROOT / server["build"]["dockerfile"]
    command_line = next(
        line for line in dockerfile.read_text().splitlines() if line.startswith("CMD ")
    )
    command = json.loads(command_line.removeprefix("CMD "))
    assert command[command.index("-p") + 1] == str(endpoint["port"])
    assert command[command.index("-m") + 1] == "bag3d.terrain.code_location"
    assert f"-p {endpoint['port']}" in server["healthcheck"]["test"][1]
    assert server["image"] == "${BAG3D_TERRAIN_IMAGE}"
    for name in ("dagster-webserver", "dagster-daemon"):
        assert (
            services[name]["depends_on"][endpoint["host"]]["condition"]
            == "service_healthy"
        )


def test_docker_workspace_is_not_hidden_by_an_old_dagster_home_volume():
    services = load_yaml("docker/compose.yaml")["services"]
    for name in ("dagster-webserver", "dagster-daemon"):
        assert (
            "./dagster/workspace.yaml:/opt/dagster/dagster_home/workspace.yaml:ro"
            in services[name]["volumes"]
        )


def test_terrain_development_source_is_mounted_and_image_is_registered():
    service = load_yaml("docker/compose.dev.yaml")["services"]["bag3d-terrain"]
    assert (
        "../packages/terrain/src:/opt/3dbag-pipeline/packages/terrain/src"
        in service["volumes"]
    )
    manifest = json.loads((ROOT / "3dbag-manifest.json").read_text())
    assert manifest["images"]["terrain"]["repository"] == "3dgi/3dbag-pipeline-terrain"
