from bag3d.floors_estimation.code_location import defs
from bag3d.floors_estimation.jobs import job_floors_estimation


def test_all_jobs_resolvable():
    assert defs.get_job_def(job_floors_estimation.name) is not None


def test_job_asset_selections_are_complete():
    assert defs.get_job_def("floors_estimation").asset_layer.executable_asset_keys
