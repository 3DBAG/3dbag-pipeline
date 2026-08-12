import json

from bag3d.common.resources.cjindex import (
    CityIndexResource,
    iter_package_refs,
    open_ready_index,
    read_package_feature_json,
)


def test_cityjson_011_package_access(tmp_path):
    source = tmp_path / "10" / "434" / "716" / "716.city.jsonl"
    source.parent.mkdir(parents=True)
    root = {
        "type": "CityJSON",
        "version": "2.0",
        "transform": {"scale": [1, 1, 1], "translate": [0, 0, 0]},
        "CityObjects": {},
        "vertices": [],
    }
    feature = {
        "type": "CityJSONFeature",
        "id": "building-1",
        "CityObjects": {
            "building-1": {"type": "Building", "attributes": {}, "geometry": []}
        },
        "vertices": [],
    }
    source.write_text("\n".join(json.dumps(item) for item in (root, feature)) + "\n")

    index = open_ready_index(CityIndexResource(dataset_dir=str(tmp_path)))
    try:
        assert index.feature_bounds_summary().package_count == 1
        pages = list(iter_package_refs(index, 1))
        assert len(pages) == 1
        ref = pages[0][0]
        assert ref.model_id == "building-1"
        assert index.package_source_paths(pages[0]) == [str(source)]
        assert read_package_feature_json(index, ref)["id"] == "building-1"
    finally:
        index.close()
