WITH source AS (
    SELECT
        merchant_id AS id,
        merchant_name_en AS organizer_name
    FROM
        {{ source(
            'raw',
            'raw_organizers'
        ) }}
)
SELECT
    *
FROM
    source
