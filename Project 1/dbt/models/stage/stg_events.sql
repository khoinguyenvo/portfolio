WITH cte AS (
    SELECT
        event_id,
        event_name_en AS event_name,
        merchant_id AS organizer_id,
        CASE
            WHEN regexp_contains(LOWER(event_name_en), r'(run|marathon|trail|tri|duathlon|race)') THEN 'Sports'
            ELSE 'Others'END AS event_type,
            CASE
                WHEN time_en NOT LIKE '%-%' THEN time_en
                ELSE REGEXP_REPLACE(regexp_extract(time_en, r' - (.*)'), r'[, ]{2,}', ' ')
            END AS event_date
            FROM
                {{ source(
                    'raw',
                    'raw_order_events'
                ) }}
            WHERE
                LOWER(event_name_en) NOT LIKE '%test%'
                AND LOWER(merchant_name_en) NOT LIKE '%test%'
        ),
        enriched_order_events AS (
            SELECT
                event_id,
                event_name,
                organizer_id,
                event_type,
                CASE
                    WHEN regexp_contains(
                        event_date,
                        '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                    ) THEN parse_date(
                        '%d/%m/%Y',
                        event_date
                    )
                    WHEN regexp_contains(
                        event_date,
                        '^[0-9]{2} [a-zA-Z]{3} [0-9]{4}$'
                    ) THEN parse_date(
                        '%d %b %Y',
                        event_date
                    )
                    WHEN regexp_contains(
                        event_date,
                        '^[0-9]{2} [a-zA-Z]{3}, [0-9]{4}$'
                    ) THEN parse_date(
                        '%d %b, %Y',
                        event_date
                    )
                    ELSE NULL
                END AS start_date,
                date_add(
                    CASE
                        WHEN regexp_contains(
                            event_date,
                            '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                        ) THEN parse_date(
                            '%d/%m/%Y',
                            event_date
                        )
                        WHEN regexp_contains(
                            event_date,
                            '^[0-9]{2} [a-zA-Z]{3} [0-9]{4}$'
                        ) THEN parse_date(
                            '%d %b %Y',
                            event_date
                        )
                        WHEN regexp_contains(
                            event_date,
                            '^[0-9]{2} [a-zA-Z]{3}, [0-9]{4}$'
                        ) THEN parse_date(
                            '%d %b, %Y',
                            event_date
                        )
                        ELSE NULL
                    END,
                    INTERVAL 2 DAY
                ) AS end_date
            FROM
                cte
        ),
        exclusive_event_lists AS (
            SELECT
                *
            FROM
                enriched_order_events
            WHERE
                event_id NOT IN (
                    SELECT
                        event_id
                    FROM
                        {{ source(
                            'raw',
                            'raw_events'
                        ) }}
                )
        ),
        final_table AS (
            SELECT
                e.event_id as id,
                e.event_name,
                e.event_type,
                oe.organizer_id,
                CAST(
                    e.start_date AS DATE
                ) start_date,
                CAST(
                    e.end_date AS DATE
                ) end_date
            FROM
                {{ source(
                    'raw',
                    'raw_events'
                ) }}
                e
                LEFT JOIN enriched_order_events oe
                ON e.event_id = oe.event_id
            WHERE
                e.event_id NOT IN (
                    SELECT
                        event_id
                    FROM
                        exclusive_event_lists
                )
                AND LOWER(e.event_name) NOT LIKE '%demo%'
                AND LOWER(e.event_name) NOT LIKE '%email%'
                AND LOWER(e.event_name) NOT LIKE '%test%'
            UNION
                DISTINCT
            SELECT
                event_id as id,
                event_name,
                event_type,
                organizer_id,
                start_date,
                end_date
            FROM
                exclusive_event_lists
        )
    SELECT
        *
    FROM
        final_table
