"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""

from dagster_dbt import build_schedule_from_dbt_selection

from pipeline.assets import (
    dbt_models_assets,
    raw_customers,
    raw_events,
    raw_login,
    raw_order_events,
    raw_order_items,
    raw_organizers,
    raw_sale_orders,
    refresh_datasets,
)

schedules = [
    build_schedule_from_dbt_selection(
        [
            raw_customers,
            raw_events,
            raw_login,
            raw_order_events,
            raw_order_items,
            raw_organizers,
            raw_sale_orders,
            refresh_datasets,
            dbt_models_assets,
        ],
        job_name="materialize_dbt_models",
        cron_schedule="@Daily",
        dbt_select="fqn:*",
    ),
]
