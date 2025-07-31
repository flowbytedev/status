
from app.assets import ping

from dagster import Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from app.assets.ping import get_server_details


# Define a job for ping monitoring
ping_servers_job = define_asset_job(name="ping_servers", selection=[get_server_details])



ping_servers_schedule = ScheduleDefinition(
    name="ping_servers_job_schedule",
    job_name="ping_servers",
    cron_schedule="*/5 * * * *",
    execution_timezone="Asia/Beirut",
    description="Monitors server connectivity by pinging servers from STATUSAPP database every 5 minutes, posts status updates to API, and creates incidents for offline servers"
)


defs = Definitions(
    assets=[
        get_server_details
    ],
    jobs=[
        ping_servers_job
    ],
    schedules=[
        ping_servers_schedule

    ],
    sensors=[

    ]
)
