{{ config(
    materialized = "table",
    partition_by ={ "field": "last_login_date_id",
    "data_type": "int64",
    "range":{ "start": 20200000,
    "end": 20250101,
    "interval": 15 }}
) }}

WITH source AS (

    SELECT
        code AS id,
        is_activated,
        ifnull(lg.type, 'Website') AS account_type,
        CASE
            WHEN nationality IN (
                SELECT
                    alpha2_code
                FROM
                    {{ source(
                        'raw',
                        'stg_countries'
                    ) }}
            ) THEN nationality
            ELSE NULL
        END AS nationality,
        CAST(
            birthday AS DATE
        ) birthday,
        CAST(
            format_date(
                '%Y%m%d',
                c.created_at
            ) AS int64
        ) AS creation_date_id,
                format_date(
            '%I %p',
            c.created_at
        ) AS created_at_tod,
        CAST(
            format_date(
                '%Y%m%d',
                last_login_at
            ) AS int64
        ) AS last_login_date_id,
        format_date(
            '%I %p',
            last_login_at
        ) AS last_login_tod
    FROM
        {{ source(
            'raw',
            'raw_customers'
        ) }} c
    LEFT JOIN {{ source('raw', 'raw_login') }} lg
    ON c.code = lg.customer_code
)
SELECT
    *
FROM
    source
