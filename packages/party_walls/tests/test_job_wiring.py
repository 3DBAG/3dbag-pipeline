from bag3d.party_walls.code_location import defs
from bag3d.party_walls.jobs import job_party_walls


def test_all_jobs_resolvable():
    assert defs.get_job_def(job_party_walls.name) is not None
