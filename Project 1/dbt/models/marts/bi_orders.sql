WITH source AS
(SELECT CAST(FORMAT_DATE('%Y%m%d', purchased_at) AS int64)                            AS date_id,
       oa.order_code                                                                  AS order_id,
       event_id,
       merchant_id                                                                    AS organizer_id,
       customer_code                                                                  AS customer_id,
       oa.product_id                                                                  AS product_id,
       category_en                                                                    AS category,
       quantity,
       CASE WHEN order_status = 'dat_hang_thanh_cong' THEN subtotal ELSE 0 END        AS gross_revenue,
       CASE WHEN order_status = 'dat_hang_thanh_cong' THEN discount_amount ELSE 0 END AS discount,
       CASE WHEN order_status = 'dat_hang_thanh_cong' THEN grand_total ELSE 0 END     AS profit,
       order_status                                                                   AS order_status,
       REPLACE(UPPER(order_type), '_', ' ')                                           AS order_type,
       REPLACE(UPPER(payment_method), '_', ' ')                                       AS payment_method,
              CASE
           WHEN date_diff(
                        purchased_at,
                        created_at,
                        SECOND
                ) >= 0
               AND date_diff(
                           purchased_at,
                           created_at,
                           SECOND
                   ) <= 900
               THEN
               date_diff(
                       purchased_at,
                       created_at,
                       MINUTE
               )
           ELSE 0
           END                                                                        AS checkout_time,
       FORMAT_DATE('%I %p', purchased_at)                                             AS order_tod
FROM {{ source('raw','raw_order_items') }} oi
        LEFT JOIN {{ source('raw','raw_sale_orders') }} o
                ON oi.order_code = o.code
        LEFT JOIN {{ source('raw', 'raw_order_attendant') }} oa
                ON oi.id = oa.item_id AND oi.product_id = oa.product_id
WHERE event_id IN (SELECT id FROM {{ ref('stg_events') }})
  AND merchant_id is not null)
SELECT DISTINCT *
FROM source
ORDER BY date_id DESC, event_id DESC, customer_id DESC, organizer_id DESC
