from bag3d.party_walls.code_location import defs
from bag3d.party_walls.jobs import job_nl_party_walls, job_nl_party_walls_index


def test_all_jobs_resolvable():
    for job_def in (job_nl_party_walls, job_nl_party_walls_index):
        assert defs.get_job_def(job_def.name) is not None
