from dagster import DefaultScheduleStatus, ScheduleDefinition

from bag3d.core.jobs import job_integration_data

integration_data_schedule = ScheduleDefinition(
    name="integration_data_monthly",
    job=job_integration_data,
    cron_schedule="0 2 1 * *",
    execution_timezone="Europe/Amsterdam",
    default_status=DefaultScheduleStatus.STOPPED,
)
