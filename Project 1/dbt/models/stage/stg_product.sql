WITH CTE AS
        (SELECT DISTINCT  product_id,
                          event_id,
                          TRIM(UPPER(product_name_en))      AS product_name,
                          TRIM(UPPER(ticket_phase_name_en)) AS ticket_phase
          FROM {{ source('raw', 'raw_order_items') }} oi
                   LEFT JOIN {{ source('raw', 'raw_sale_orders') }} o
                             ON oi.order_code = o.code
          WHERE product_name_en != ""
            AND event_id IN (SELECT id
                             FROM {{ ref("stg_events") }})
          ORDER BY product_name),

extracted_table AS 
        (SELECT DISTINCT 
                product_id, 
                TRIM(phase) AS phase
        FROM (
            SELECT product_id,
                    event_id,
                    product_name,
                    REGEXP_EXTRACT(product_name, r'.* - (.*)') AS phase
            FROM CTE)
        ),

cleaned_phased_table AS 
    (SELECT 
        product_id,
        phase,
        REPLACE(
            REPLACE(
                REGEXP_REPLACE(phase, r'TICKETS|TICKET|COMERS|COMER|PRICE|PACKAGE',''),
                    ' GR', 'GR'
                ), 
            'STANDARD', 'REGULAR'
        ) AS cleaned_phase
        FROM extracted_table),

final_table AS 
    (SELECT DISTINCT 
        product_id,
        event_id,
        product_name,
        CASE
            WHEN
                CASE
                    WHEN CONTAINS_SUBSTR(phase, '(')
                        THEN
                        REGEXP_EXTRACT(phase, r'\((.*?)\)')
                    WHEN REGEXP_CONTAINS(phase, r'ĐĂNG KÝ SIÊU SỚM|SUPER BIRD')
                        THEN 'SUPER EARLY BIRD'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'SỚM|EARLR|EARLY BIRD|EARLY|EARLY SHARK|VÉ ĐĂNG KÝ SỚM|ĐĂNG KÝ SỚM')
                        THEN 'EARLY BIRD'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'ĐĂNG KÝ SIÊU TRỄ|SUPPER LATE|LATEST REGISTRATION')
                        THEN 'SUPER LATE'
                    WHEN REGEXP_CONTAINS(phase, r'ĐĂNG KÝ TRỄ|LATE GR|GR LATE') THEN 'LATE'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'STANDARD|STANDAR|GENERAL|BASIC|VÉ TIÊU CHUẨN|VÉ CƠ BẢN|ĐĂNG KÝ TIÊU CHUẨN|REGULAR  1')
                        THEN 'REGULAR'
                    WHEN REGEXP_CONTAINS(phase, r'FLASH SALES|BLACK FRIDAY|FLASH SALE')
                        THEN 'FLASH SALES'
                    WHEN REGEXP_CONTAINS(phase, r'PRESALES') THEN 'PRE-SALES'
                    ELSE TRIM(phase) END
                    NOT IN
                ('SUPER EARLY BIRD', 'EARLY BIRD', 'SUPER LATE', 'LATE', 'REGULAR',
                'FLASH SALES', 'PRE-SALES') THEN 'OTHERS'
            ELSE
                CASE
                    WHEN CONTAINS_SUBSTR(phase, '(')
                        THEN
                        REGEXP_EXTRACT(phase, r'\((.*?)\)')
                    WHEN REGEXP_CONTAINS(phase, r'ĐĂNG KÝ SIÊU SỚM|SUPER BIRD')
                        THEN 'SUPER EARLY BIRD'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'SỚM|EARLR|EARLY BIRD|EARLY|EARLY SHARK|VÉ ĐĂNG KÝ SỚM|ĐĂNG KÝ SỚM')
                        THEN 'EARLY BIRD'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'ĐĂNG KÝ SIÊU TRỄ|SUPPER LATE|LATEST REGISTRATION')
                        THEN 'SUPER LATE'
                    WHEN REGEXP_CONTAINS(phase, r'ĐĂNG KÝ TRỄ|LATE GR|GR LATE') THEN 'LATE'
                    WHEN REGEXP_CONTAINS(phase,
                                        r'STANDARD|STANDAR|GENERAL|BASIC|VÉ TIÊU CHUẨN|VÉ CƠ BẢN|ĐĂNG KÝ TIÊU CHUẨN|REGULAR  1')
                        THEN 'REGULAR'
                    WHEN REGEXP_CONTAINS(phase, r'FLASH SALES|BLACK FRIDAY|FLASH SALE')
                        THEN 'FLASH SALES'
                    WHEN REGEXP_CONTAINS(phase, r'PRESALES') THEN 'PRE-SALES'
                    ELSE TRIM(phase) END
            END AS phase
    FROM (
        SELECT DISTINCT t1.product_id,
            event_id,
            product_name,
            CASE
                WHEN
                    cleaned_phase IS NULL THEN LAG(cleaned_phase)
                                                    over (PARTITION BY t1.product_id, event_id ORDER BY t1.event_id, t1.product_id, product_name)
                ELSE cleaned_phase END AS phase
        FROM CTE t1
        INNER JOIN cleaned_phased_table t2
        ON t1.product_id = t2.product_id
        )
    )
SELECT *
FROM final_table
ORDER BY event_id ASC