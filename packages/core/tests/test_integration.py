from bag3d.core.code_location import defs


def test_job_nl_reconstruct():
    resolved_job = defs.get_job_def("nl_reconstruct")
    print(resolved_job)
    result = resolved_job.execute_in_process()

    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
