import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset
from dagster_dbt import DbtCliResource, dbt_assets

from .base import BigQueryOps, PowerBIOps, S3Ops
from .constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path)
def dbt_models_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli((["build"]), context=context).stream()


@asset(compute_kind="Python", group_name="extract")
def raw_sale_orders(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_sale_orders")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorder/saleorder.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_order_items(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_order_items")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorderitem/saleorderitem.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_order_attendant(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_order_attendant")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorderattendant/saleorderattendant.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_customers(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_customers")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/customer/customer.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()
    data = data.assign(
        birthday=pd.to_datetime(data.birthday, format="mixed", errors="coerce"),
        nationality=data.nationality.str.replace("vietnam", "VN"),
    )
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_organizers(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_organizers")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorderevent/saleorderevent.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()
    data = data.assign(
        merchant_name_en=data.merchant_name_en.str.title()
    ).drop_duplicates(subset="merchant_id")
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_events(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_events")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/event/event.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()

    # Transform the DataFrame before loading
    data = data.assign(
        start_date=pd.to_datetime(data.start_date),
        end_date=pd.to_datetime(data.end_date),
        event_name=data.event_name.str.title(),
        event_type=data.event_type.str.title(),
    )
    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_order_events(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_order_events")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/saleorderevent/saleorderevent.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()

    # Transform the DataFrame before loading
    data = data.drop_duplicates(subset=["event_id", "merchant_id"])

    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(compute_kind="Python", group_name="extract")
def raw_login(context: AssetExecutionContext) -> MaterializeResult:
    gc = BigQueryOps(table="activeTix.raw.raw_login")
    s3 = S3Ops(
        bucket="activetix",
        key="datalakehouse/login/login.json",
        columns=gc.get_columns(),
    )
    data = s3.get_data()

    # Transform the DataFrame before loading
    data = (
        data.sort_values(by=["customer_code", "updated_at"], ascending=True)
        .query("customer_code.notnull()")
        .drop_duplicates(subset="customer_code", keep="last")
        .assign(
            created_at=pd.to_datetime(data.created_at),
            updated_at=pd.to_datetime(data.updated_at),
        )
    )

    try:
        gc.load_table(dataframe=data)
    except Exception as e:
        raise f"Failed to load table: {e}"
    finally:
        context.log.info(f"Total rows {data.shape[0]}")
    return MaterializeResult(metadata={"number_of_rows": data.shape[0]})


@asset(deps=["stg_orders", "stg_events", "stg_organizers", "bi_customers"])
def refresh_datasets():
    tenant_id = "<Tenant_id>"
    app_id = "<App_id>"
    dataset_id = "<Dataset_id>"
    curr = PowerBIOps(tenant_id=tenant_id, app_id=app_id, dataset_id=dataset_id)
    curr.refresh_dataset()
