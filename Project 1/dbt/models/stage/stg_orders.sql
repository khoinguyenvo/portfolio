{{ config(
    materialized = "table",
    partition_by ={ "field": "date_id",
    "data_type": "int64",
    "range":{ "start": 20200000,
    "end": 20250101,
    "interval": 15 }}
) }}

WITH order_list AS (

    SELECT
        code AS order_id,
        event_id,
        customer_code AS customer_id,
        order_status,
        order_type,
        UPPER(REPLACE(payment_method, '_', ' ')) AS payment_method,
        created_at,
        purchased_at
    FROM
        {{ source(
            'raw',
            'raw_sale_orders'
        ) }}
    WHERE
        event_id IN (
            SELECT
                id
            FROM
                {{ ref("stg_events") }}
        )
        AND merchant_id IS NOT NULL
),
order_items AS (
    SELECT
        order_code AS order_id,
        quantity,
        subtotal AS gross_revenue,
        discount_amount AS discount,
        grand_total AS net_revenue
    FROM
        {{ source(
            'raw',
            'raw_order_items'
        ) }}
),
merged_table AS (
    SELECT
        CAST(format_date('%Y%m%d', purchased_at) AS int64) AS date_id,
        o.order_id,
        event_id,
        customer_id,
        quantity,
        CASE
            WHEN order_status = 'dat_hang_thanh_cong' THEN gross_revenue
            ELSE 0
        END AS gross_revenue,
        CASE
            WHEN order_status = 'dat_hang_thanh_cong' THEN discount
            ELSE 0
        END AS discount,
        CASE
            WHEN order_status = 'dat_hang_thanh_cong' THEN net_revenue
            ELSE 0
        END AS net_revenue,
        order_status,
        payment_method,
        CASE
            WHEN date_diff(
                created_at,
                purchased_at,
                MINUTE
            ) < 0
            OR date_diff(
                created_at,
                purchased_at,
                MINUTE
            ) > 15 THEN 0
            ELSE date_diff(
                created_at,
                purchased_at,
                MINUTE
            )
        END AS checkout_time,
        format_date(
            '%I %p',
            purchased_at
        ) AS order_tod
    FROM
        order_list AS o
        INNER JOIN order_items AS oi
        ON o.order_id = oi.order_id
)
SELECT
    *
FROM
    merged_table
