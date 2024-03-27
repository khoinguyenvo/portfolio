import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from pipeline.assets import (
    dbt_models_assets,
    raw_customers,
    raw_events,
    raw_login,
    raw_order_attendant,
    raw_order_events,
    raw_order_items,
    raw_organizers,
    raw_sale_orders,
    refresh_datasets,
)
from pipeline.assets.constants import dbt_project_dir

defs = Definitions(
    assets=[
        raw_sale_orders,
        raw_order_items,
        raw_order_attendant,
        raw_customers,
        raw_organizers,
        raw_events,
        raw_order_events,
        raw_login,
        dbt_models_assets,
        refresh_datasets,
    ],
    jobs=[
        define_asset_job(
            name="techhaus",
            selection=AssetSelection.all(),
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="schedule",
            job_name="techhaus",
            cron_schedule="0 2 * * *",
            execution_timezone="Asia/Saigon",
            default_status=DefaultScheduleStatus.RUNNING,
        )
    ],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir))},
)
