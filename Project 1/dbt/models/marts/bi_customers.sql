{{ config(
    materialized = "table",
    partition_by ={ "field": "last_login_date_id",
    "data_type": "int64",
    "range":{ "start": 20200000,
    "end": 20250101,
    "interval": 15 }}
) }}

WITH order_summary AS (

    SELECT
        customer_id,
        SUM(
            CASE
                WHEN order_status = 'dat_hang_thanh_cong' THEN 1
                ELSE 0
            END
        ) AS orders_completed,
        SUM(
            CASE
                WHEN order_status = 'dat_hang_thanh_cong' THEN quantity
                ELSE 0
            END
        ) AS tickets_purchased,
        SUM(
            CASE
                WHEN order_status = 'dat_hang_thanh_cong' THEN net_revenue
                ELSE 0
            END
        ) AS total_spending
    FROM
        {{ ref("stg_orders") }}
    GROUP BY
        ALL
),
final_table AS (
    SELECT
        C.*,
        COALESCE(
            orders_completed,
            0
        ) AS orders_completed,
        COALESCE(
            tickets_purchased,
            0
        ) AS tickets_purchased,
        COALESCE(
            total_spending,
            0
        ) AS total_spending
    FROM
        {{ ref("stg_customers") }} C
        LEFT JOIN order_summary o
        ON C.id = o.customer_id
    ORDER BY last_login_date_id
)
SELECT
    *
FROM
    final_table
