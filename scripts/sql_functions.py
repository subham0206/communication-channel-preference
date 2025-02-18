"""
==================================================================================================
==================================================================================================

SECTION 1: Training & Testing Functions

==================================================================================================
==================================================================================================
"""

# Function to create the Atttentive SQL query string
def get_query_attentive_data(brand):
    '''
    Script to extract past one year data from attentive platform
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_data AS
(WITH conversion_ranked AS (
    SELECT
        b.email,
        b.timestamp,
        b.message_id,
        ROW_NUMBER() OVER (PARTITION BY b.email, b.message_id ORDER BY b.timestamp ASC) AS row_num
    FROM
        NMEDWPRD_DB.EDW.ATT_CONVERSION_T b
    WHERE
        b.timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND b.brand = '{brand}'
),
classified_messages AS (
    SELECT
        a.email AS messaging_email,
        a.timestamp AS messaging_timestamp,
        c.timestamp AS conversion_timestamp,
        CASE
            WHEN     a.message_name ILIKE '%new to sale%' OR
                     a.message_name ILIKE '%price decrease%' OR
                     a.message_name ILIKE '%new arrivals%' OR
                     a.message_name ILIKE '%best sellers%' OR
                     a.message_name ILIKE '%weekly picks%' OR
                     a.message_name ILIKE '%vip%' OR
                     a.message_name ILIKE '%sweep up promo%' OR
                     a.message_name ILIKE '%purchase anniversary%' OR
                     a.message_name ILIKE '%next best category%' OR
                     a.message_name ILIKE '%birthday%' OR
                     a.message_name ILIKE '%low inventory%' OR
                     a.message_name ILIKE '%at-risk audience expansion%' OR
                     a.message_name ILIKE '%post purchase repeat buyer%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                     a.message_name ILIKE '%beauty replenishment%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer%' OR
                     a.message_name ILIKE '%my favorites%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                     a.message_name ILIKE '%sale%' OR
                     a.message_name ILIKE '%launch%' OR
                     a.message_name ILIKE '%new offer%' OR
                     a.message_name ILIKE '%discount%' OR
                     a.message_name ILIKE '%offer%' OR
                     a.message_name ILIKE '%special%' OR
                     a.message_name ILIKE '%promo%' OR
                     a.message_name ILIKE '%deal%' OR
                     a.message_name ILIKE '%limited time%' OR
                     a.message_name ILIKE '%coupon%' OR
                     a.message_name ILIKE '%free%' OR
                     a.message_name ILIKE '%exclusive%' OR
                     a.message_name ILIKE '%buy now%' OR
                     a.message_name ILIKE '%clearance%' OR
                     a.message_name ILIKE '%flash sale%' OR
                     a.message_name ILIKE '%holiday sale%' OR
                     a.message_name ILIKE '%seasonal offer%' OR
                     a.message_name ILIKE '%member exclusive%' OR
                     a.message_name ILIKE '%early access%' OR
                     a.message_name ILIKE '%limited edition%' OR
                     a.message_name ILIKE '%two-for-one%' OR
                     a.message_name ILIKE '%bundle offer%' OR
                     a.message_name ILIKE '%friends and family%' OR
                     a.message_name ILIKE '%special event%' OR
                     a.message_name ILIKE '%flash promo%' OR
                     a.message_name ILIKE '%pre-sale%' OR
                     a.message_name ILIKE '%limited quantities%' OR
                     a.message_name ILIKE '%doorbuster%' OR
                     a.message_name ILIKE '%cyber monday%' OR
                     a.message_name ILIKE '%new%' OR
                     a.message_name ILIKE '%black friday%' OR
                     a.message_name ILIKE '%gift with purchase%' OR
                     a.message_name ILIKE '%loyalty rewards%' OR
                     a.message_name ILIKE '%bonus points%' OR
                     a.message_name ILIKE '%free shipping%' OR
                     a.message_name ILIKE '%buy more save more%' OR
                     a.message_name ILIKE '%pre-order%' OR
                     a.message_name ILIKE '%kick%'
                     THEN 'Promotional'
                WHEN a.message_name ILIKE '%abandonment%' OR
                     a.message_name ILIKE '%cart abandonment%' OR
                     a.message_name ILIKE '%search abandonment%' OR
                     a.message_name ILIKE '%reminder%' OR
                     a.message_name ILIKE '%follow-up%' OR
                     a.message_name ILIKE '%thank you%' OR
                     a.message_name ILIKE '%survey%' OR
                     a.message_name ILIKE '%feedback%' OR
                     a.message_name ILIKE '%abandoned cart%' OR
                     a.message_name ILIKE '%renewal%' OR
                     a.message_name ILIKE '%welcome%' OR
                     a.message_name ILIKE '%personalized%' OR
                     a.message_name ILIKE '%reactivate%' OR
                     a.message_name ILIKE '%engagement%' OR
                     a.message_name ILIKE '%re-engagement%' OR
                     a.message_name ILIKE '%lost customer%' OR
                     a.message_name ILIKE '%inactivity%' OR
                     a.message_name ILIKE '%churn prevention%' OR
                     a.message_name ILIKE '%win-back%' OR
                     a.message_name ILIKE '%first purchase%' OR
                     a.message_name ILIKE '%order confirmation%' OR
                     a.message_name ILIKE '%delivery confirmation%' OR
                     a.message_name ILIKE '%replenishment reminder%' OR
                     a.message_name ILIKE '%subscription reminder%' OR
                     a.message_name ILIKE '%trial expiration%' OR
                     a.message_name ILIKE '%last chance%' OR
                     a.message_name ILIKE '%return reminder%' OR
                     a.message_name ILIKE '%missed opportunity%'
                     THEN 'Trigger'
                ELSE NULL
            END AS message_type
    FROM
        NMEDWPRD_DB.EDW.ATT_MESSAGING_T a
    LEFT JOIN
        conversion_ranked c
    ON
        a.message_id = c.message_id AND a.email = c.email AND c.row_num = 1
    WHERE
        a.timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND a.brand = '{brand}'
),
CUST_WITH_L12NS AS (
        SELECT 
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE 
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING 
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.messaging_timestamp IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.conversion_timestamp IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(B.EMAIL_ADDRESS)) = LOWER(TRIM(A.messaging_email))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS sms_received,
    SUM(click_date) AS sms_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query

# Function to create the Bluecore SQL query string
def get_query_bluecore_data(brand):
    '''
    Script to extract past one year data from bluecore
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_data AS 
(WITH classified_messages AS (
    SELECT
        d.EMAIL,
        d.EVENT_TIME,
        c.EVENT_TIME AS click_event_time,
        CASE
            WHEN d.SUBJECT_LINE ILIKE '%new to sale%' OR
                 d.SUBJECT_LINE ILIKE '%price decrease%' OR
                 d.SUBJECT_LINE ILIKE '%new arrivals%' OR
                 d.SUBJECT_LINE ILIKE '%best sellers%' OR
                 d.SUBJECT_LINE ILIKE '%weekly picks%' OR
                 d.SUBJECT_LINE ILIKE '%vip%' OR
                 d.SUBJECT_LINE ILIKE '%sweep up promo%' OR
                 d.SUBJECT_LINE ILIKE '%purchase anniversary%' OR
                 d.SUBJECT_LINE ILIKE '%next best category%' OR
                 d.SUBJECT_LINE ILIKE '%birthday%' OR
                 d.SUBJECT_LINE ILIKE '%low inventory%' OR
                 d.SUBJECT_LINE ILIKE '%at-risk audience expansion%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%beauty replenishment%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%my favorites%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%sale%' OR
                 d.SUBJECT_LINE ILIKE '%launch%' OR
                 d.SUBJECT_LINE ILIKE '%new offer%' OR
                 d.SUBJECT_LINE ILIKE '%discount%' OR
                 d.SUBJECT_LINE ILIKE '%offer%' OR
                 d.SUBJECT_LINE ILIKE '%special%' OR
                 d.SUBJECT_LINE ILIKE '%promo%' OR
                 d.SUBJECT_LINE ILIKE '%deal%' OR
                 d.SUBJECT_LINE ILIKE '%limited time%' OR
                 d.SUBJECT_LINE ILIKE '%coupon%' OR
                 d.SUBJECT_LINE ILIKE '%free%' OR
                 d.SUBJECT_LINE ILIKE '%exclusive%' OR
                 d.SUBJECT_LINE ILIKE '%buy now%' OR
                 d.SUBJECT_LINE ILIKE '%clearance%' OR
                 d.SUBJECT_LINE ILIKE '%flash sale%' OR
                 d.SUBJECT_LINE ILIKE '%holiday sale%' OR
                 d.SUBJECT_LINE ILIKE '%seasonal offer%' OR
                 d.SUBJECT_LINE ILIKE '%member exclusive%' OR
                 d.SUBJECT_LINE ILIKE '%early access%' OR
                 d.SUBJECT_LINE ILIKE '%limited edition%' OR
                 d.SUBJECT_LINE ILIKE '%two-for-one%' OR
                 d.SUBJECT_LINE ILIKE '%bundle offer%' OR
                 d.SUBJECT_LINE ILIKE '%friends and family%' OR
                 d.SUBJECT_LINE ILIKE '%special event%' OR
                 d.SUBJECT_LINE ILIKE '%flash promo%' OR
                 d.SUBJECT_LINE ILIKE '%pre-sale%' OR
                 d.SUBJECT_LINE ILIKE '%limited quantities%' OR
                 d.SUBJECT_LINE ILIKE '%doorbuster%' OR
                 d.SUBJECT_LINE ILIKE '%cyber monday%' OR
                 d.SUBJECT_LINE ILIKE '%new%' OR
                 d.SUBJECT_LINE ILIKE '%black friday%' OR
                 d.SUBJECT_LINE ILIKE '%gift with purchase%' OR
                 d.SUBJECT_LINE ILIKE '%loyalty rewards%' OR
                 d.SUBJECT_LINE ILIKE '%bonus points%' OR
                 d.SUBJECT_LINE ILIKE '%free shipping%' OR
                 d.SUBJECT_LINE ILIKE '%buy more save more%' OR
                 d.SUBJECT_LINE ILIKE '%pre-order%' OR
                 d.SUBJECT_LINE ILIKE '%kick%'
                 THEN 'Promotional'
            WHEN d.SUBJECT_LINE ILIKE '%abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%cart abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%search abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%reminder%' OR
                 d.SUBJECT_LINE ILIKE '%follow-up%' OR
                 d.SUBJECT_LINE ILIKE '%thank you%' OR
                 d.SUBJECT_LINE ILIKE '%survey%' OR
                 d.SUBJECT_LINE ILIKE '%feedback%' OR
                 d.SUBJECT_LINE ILIKE '%abandoned cart%' OR
                 d.SUBJECT_LINE ILIKE '%renewal%' OR
                 d.SUBJECT_LINE ILIKE '%welcome%' OR
                 d.SUBJECT_LINE ILIKE '%personalized%' OR
                 d.SUBJECT_LINE ILIKE '%reactivate%' OR
                 d.SUBJECT_LINE ILIKE '%engagement%' OR
                 d.SUBJECT_LINE ILIKE '%re-engagement%' OR
                 d.SUBJECT_LINE ILIKE '%lost customer%' OR
                 d.SUBJECT_LINE ILIKE '%inactivity%' OR
                 d.SUBJECT_LINE ILIKE '%churn prevention%' OR
                 d.SUBJECT_LINE ILIKE '%win-back%' OR
                 d.SUBJECT_LINE ILIKE '%first purchase%' OR
                 d.SUBJECT_LINE ILIKE '%order confirmation%' OR
                 d.SUBJECT_LINE ILIKE '%delivery confirmation%' OR
                 d.SUBJECT_LINE ILIKE '%replenishment reminder%' OR
                 d.SUBJECT_LINE ILIKE '%subscription reminder%' OR
                 d.SUBJECT_LINE ILIKE '%trial expiration%' OR
                 d.SUBJECT_LINE ILIKE '%last chance%' OR
                 d.SUBJECT_LINE ILIKE '%return reminder%' OR
                 d.SUBJECT_LINE ILIKE '%missed opportunity%'
                 THEN 'Trigger'
            ELSE NULL
        END AS message_type
    FROM
        NMEDWPRD_DB.EDW.BLUECORE_DELIVER_T d
    LEFT JOIN
        NMEDWPRD_DB.EDW.BLUECORE_CLICK_T c
        ON d.NQE = c.NQE
    WHERE
        d.EVENT_TIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE  
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id, 
        CASE WHEN A.EVENT_TIME IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_event_time IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.EMAIL)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS blu_email_received,
    SUM(click_date) AS blu_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query

# Function to create the Sendgrid SQL query string
def get_query_sendgrid_data(brand):
    '''
    Script to extract past one year data from sendgrid
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_data AS 
(WITH classified_messages AS (
    SELECT
        external_address,
        delivered_timestamp,
        click_timestamp,
        CASE
            WHEN message_subject ILIKE '%new to sale%' OR
                 message_subject ILIKE '%price decrease%' OR
                 message_subject ILIKE '%new arrivals%' OR
                 message_subject ILIKE '%best sellers%' OR
                 message_subject ILIKE '%weekly picks%' OR
                 message_subject ILIKE '%vip%' OR
                 message_subject ILIKE '%sweep up promo%' OR
                 message_subject ILIKE '%purchase anniversary%' OR
                 message_subject ILIKE '%next best category%' OR
                 message_subject ILIKE '%birthday%' OR
                 message_subject ILIKE '%low inventory%' OR
                 message_subject ILIKE '%at-risk audience expansion%' OR
                 message_subject ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                 message_subject ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                 message_subject ILIKE '%beauty replenishment%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                 message_subject ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                 message_subject ILIKE '%my favorites%' OR
                 message_subject ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                 message_subject ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                 message_subject ILIKE '%sale%' OR
                 message_subject ILIKE '%launch%' OR
                 message_subject ILIKE '%new offer%' OR
                 message_subject ILIKE '%discount%' OR
                 message_subject ILIKE '%offer%' OR
                 message_subject ILIKE '%special%' OR
                 message_subject ILIKE '%promo%' OR
                 message_subject ILIKE '%deal%' OR
                 message_subject ILIKE '%limited time%' OR
                 message_subject ILIKE '%coupon%' OR
                 message_subject ILIKE '%free%' OR
                 message_subject ILIKE '%exclusive%' OR
                 message_subject ILIKE '%buy now%' OR
                 message_subject ILIKE '%clearance%' OR
                 message_subject ILIKE '%flash sale%' OR
                 message_subject ILIKE '%holiday sale%' OR
                 message_subject ILIKE '%seasonal offer%' OR
                 message_subject ILIKE '%member exclusive%' OR
                 message_subject ILIKE '%early access%' OR
                 message_subject ILIKE '%limited edition%' OR
                 message_subject ILIKE '%two-for-one%' OR
                 message_subject ILIKE '%bundle offer%' OR
                 message_subject ILIKE '%friends and family%' OR
                 message_subject ILIKE '%special event%' OR
                 message_subject ILIKE '%flash promo%' OR
                 message_subject ILIKE '%pre-sale%' OR
                 message_subject ILIKE '%limited quantities%' OR
                 message_subject ILIKE '%doorbuster%' OR
                 message_subject ILIKE '%cyber monday%' OR
                 message_subject ILIKE '%new%' OR
                 message_subject ILIKE '%black friday%' OR
                 message_subject ILIKE '%gift with purchase%' OR
                 message_subject ILIKE '%loyalty rewards%' OR
                 message_subject ILIKE '%bonus points%' OR
                 message_subject ILIKE '%free shipping%' OR
                 message_subject ILIKE '%buy more save more%' OR
                 message_subject ILIKE '%pre-order%' OR
                 message_subject ILIKE '%kick%'
                 THEN 'Promotional'
            WHEN message_subject ILIKE '%abandonment%' OR
                 message_subject ILIKE '%cart abandonment%' OR
                 message_subject ILIKE '%search abandonment%' OR
                 message_subject ILIKE '%reminder%' OR
                 message_subject ILIKE '%follow-up%' OR
                 message_subject ILIKE '%thank you%' OR
                 message_subject ILIKE '%survey%' OR
                 message_subject ILIKE '%feedback%' OR
                 message_subject ILIKE '%abandoned cart%' OR
                 message_subject ILIKE '%renewal%' OR
                 message_subject ILIKE '%welcome%' OR
                 message_subject ILIKE '%personalized%' OR
                 message_subject ILIKE '%reactivate%' OR
                 message_subject ILIKE '%engagement%' OR
                 message_subject ILIKE '%re-engagement%' OR
                 message_subject ILIKE '%lost customer%' OR
                 message_subject ILIKE '%inactivity%' OR
                 message_subject ILIKE '%churn prevention%' OR
                 message_subject ILIKE '%win-back%' OR
                 message_subject ILIKE '%first purchase%' OR
                 message_subject ILIKE '%order confirmation%' OR
                 message_subject ILIKE '%delivery confirmation%' OR
                 message_subject ILIKE '%replenishment reminder%' OR
                 message_subject ILIKE '%subscription reminder%' OR
                 message_subject ILIKE '%trial expiration%' OR
                 message_subject ILIKE '%last chance%' OR
                 message_subject ILIKE '%return reminder%' OR
                 message_subject ILIKE '%missed opportunity%'
                 THEN 'Trigger'
            ELSE NULL
        END AS message_type
    FROM
        NMEDWPRD_DB.COMHUB.CONVERSATION_T
    WHERE
        source_id <> 'CCC'
        AND date_created BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND conversation_direction <> 'Inbound'
        AND message_type = 'EMAIL'
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id,
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.delivered_timestamp IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_timestamp IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.external_address)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS send_email_received,
    SUM(click_date) AS send_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query

# Function to create the Epsilon SQL query string
def get_query_epsilon_data(brand):
    '''
    Script to extract past one year data from Epsilon
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_epsilon_data AS 
    (WITH classified_messages AS (SELECT 
        a.email,
        a.send_date,
        a.click_date
    FROM
        PDWDM.CHEETAH_EMAIL_CONSOLIDATED_T a 
    JOIN
        NMEDWPRD_DB.EPSILON.EPS_ACTIVITY_T b ON a.message_id = b.service_communication_id
    WHERE 
        b.brand = '{brand}'
        AND a.send_date BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE 
        AND b.original_event_timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE 
        AND b.email_email_type = 'Promo'
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.send_date IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_date IS NULL THEN 0 ELSE 1 END AS click_date,
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.email)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    SUM(send_date) AS eps_email_received,
    SUM(click_date) AS eps_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id);
    """
    return query

# Function to create the Customer Transactional SQL query string
def get_query_customer_transactional_features_data (brand):
    '''
    Script to extract customer transactional features i.e. profile, rfm, vintage all combined
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary

    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_data AS
(WITH l12m_metrics AS (
    SELECT
        curr_cmd_id,
        COUNT(*) AS total_items,
        SUM(CASE WHEN itm_item_status = 'R' THEN 1 ELSE 0 END) AS returned_items,
        (returned_items/total_items) AS return_percentage,
        SUM(CASE WHEN CHANNEL = 'Online' THEN 1 ELSE 0 END) AS online_items,
        (online_items/total_items) AS online_percentage,
        SUM(CASE WHEN ((itm_item_status = 'S') AND (dsc_unit_discount_amt > 0)) THEN 1 ELSE 0 END) AS discounted_items,
        (discounted_items/total_items) AS discounted_percentage,
        ROUND(AVG(CASE WHEN itm_item_status = 'S' THEN total_itm_purchase_amount ELSE NULL END)) AS avg_item_price,
        SUM(total_itm_purchase_amount) AS l12m_net_sales,
        ROUND((CURRENT_DATE - MAX(hdr_tran_datetime)) / 30, 2) AS Recency,
        COUNT(DISTINCT hdr_tran_datetime) AS Frequency,
    FROM 
        NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
    WHERE 
        brand = '{brand}'
        AND hdr_tran_datetime BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND itm_item_status IN ('S', 'R')
    GROUP BY 
        curr_cmd_id
),
vintage AS (
    SELECT 
        curr_cmd_id,
        DATEDIFF(day, MIN(hdr_tran_datetime), CURRENT_DATE) AS vintage_days
    FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
    WHERE 
        brand = '{brand}'
        AND curr_cmd_id IN (SELECT curr_cmd_id FROM l12m_metrics)
        AND itm_item_status = 'S'
    GROUP BY
        curr_cmd_id
    )
SELECT 
    A.*,
    B.vintage_days
FROM 
    l12m_metrics A
    JOIN vintage B
    ON A.curr_cmd_id = B.curr_cmd_id
ORDER BY 
    A.curr_cmd_id);"""
    return query

# Function to create the Trigger Feature Engg SQL query string
def get_query_trigger_feature_engg_data (brand):
    '''
    Script to extract trigger message type and perform feature engineering
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary
    
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_trigger AS
(WITH 
    blu_data AS (
        SELECT 
            curr_customer_id,
            blu_email_received,
            blu_email_clicked
        FROM
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_data
        WHERE
            message_type = 'Trigger'
    ),

    send_data AS (
        SELECT
            curr_customer_id,
            send_email_received,
            send_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_data
        WHERE 
            message_type = 'Trigger'
    ),

    blu_send_trigger AS (
        SELECT 
            COALESCE(b.curr_customer_id, s.curr_customer_id) AS curr_customer_id,
            COALESCE(b.blu_email_received, 0) AS blu_email_received,
            COALESCE(b.blu_email_clicked, 0) AS blu_email_clicked,
            COALESCE(s.send_email_received, 0) AS send_email_received,
            COALESCE(s.send_email_clicked, 0) AS send_email_clicked
        FROM 
            blu_data b
        FULL OUTER JOIN 
            send_data s 
        ON 
            b.curr_customer_id = s.curr_customer_id
    ),

    blu_send_att AS (
        SELECT 
            bst.curr_customer_id,
            bst.blu_email_received,
            bst.blu_email_clicked,
            bst.send_email_received,
            bst.send_email_clicked,
            COALESCE(a.sms_received, 0) AS sms_received,
            COALESCE(a.sms_clicked, 0) AS sms_clicked
        FROM 
            blu_send_trigger bst
        INNER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_data a 
        ON 
            bst.curr_customer_id = a.curr_customer_id
        WHERE
            a.message_type = 'Trigger'
    ),

    transformed AS (
        SELECT 
            curr_customer_id,
            blu_email_received + send_email_received AS email_received_count,
            blu_email_clicked + send_email_clicked AS email_clicked_count,
            sms_received,
            sms_clicked,
            CASE 
                WHEN (blu_email_received + send_email_received) = 0 THEN 0 
                ELSE (blu_email_clicked + send_email_clicked) / (blu_email_received + send_email_received)
            END AS email_engagement_ratio,
            CASE 
                WHEN sms_received = 0 THEN 0 
                ELSE sms_clicked / sms_received 
            END AS sms_engagement_ratio
        FROM 
            blu_send_att
    ),

    feature_eng AS (
        SELECT 
            t.curr_customer_id,
            t.email_received_count,
            t.email_clicked_count,
            t.sms_received,
            t.sms_clicked,
            t.email_engagement_ratio,
            t.sms_engagement_ratio,
            l.*
        FROM 
            transformed t
        INNER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_data l 
        ON 
            t.curr_customer_id = l.CURR_CMD_ID
    ),

    with_channel AS (
        SELECT 
            *,
            CASE 
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio = 0 THEN 'neither'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio = 0 THEN 'email'
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio > 0 THEN 'sms'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio > 0 THEN 'both'
                ELSE 'Unknown'
            END AS preferred_channel
        FROM 
            feature_eng
    )

SELECT 
    curr_customer_id,
    email_received_count,
    email_clicked_count,
    sms_received,
    sms_clicked,
    email_engagement_ratio,
    sms_engagement_ratio,
    TOTAL_ITEMS,
    RETURNED_ITEMS,
    RETURN_PERCENTAGE,
    ONLINE_ITEMS,
    ONLINE_PERCENTAGE,
    DISCOUNTED_ITEMS,
    DISCOUNTED_PERCENTAGE,
    AVG_ITEM_PRICE,
    L12M_NET_SALES,
    RECENCY,
    FREQUENCY,
    VINTAGE_DAYS,
    preferred_channel,
    CASE
        WHEN preferred_channel = 'neither' THEN 0
        WHEN preferred_channel = 'email' THEN 1
        WHEN preferred_channel = 'sms' THEN 2
        WHEN preferred_channel = 'both' THEN 3
    END AS preferred_channel_encoded
FROM with_channel);
    """
    return query

# Function to create the promotional feature engg SQL query string
def get_query_promotional_feature_engg_data (brand):
    '''
    Script to extract Promotional message type and perform feature engineering
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_Promotional AS
(WITH 
    blu_data AS (
        SELECT 
            curr_customer_id,
            blu_email_received,
            blu_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_data
        WHERE 
            message_type = 'Promotional'
    ),

    send_data AS (
        SELECT 
            curr_customer_id,
            send_email_received,
            send_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_data
        WHERE 
            message_type = 'Promotional'
    ),

    eps_data AS (
        SELECT 
            curr_customer_id,
            eps_email_received,
            eps_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_epsilon_data
    ),

    blu_send_trigger AS (
        SELECT 
            COALESCE(b.curr_customer_id, s.curr_customer_id, e.curr_customer_id) AS curr_customer_id,
            COALESCE(b.blu_email_received, 0) AS blu_email_received,
            COALESCE(b.blu_email_clicked, 0) AS blu_email_clicked,
            COALESCE(s.send_email_received, 0) AS send_email_received,
            COALESCE(s.send_email_clicked, 0) AS send_email_clicked,
            COALESCE(e.eps_email_received, 0) AS eps_email_received,
            COALESCE(e.eps_email_clicked, 0) AS eps_email_clicked
        FROM 
            blu_data b
        FULL OUTER JOIN 
            send_data s ON b.curr_customer_id = s.curr_customer_id
        FULL OUTER JOIN 
            eps_data e ON COALESCE(b.curr_customer_id, s.curr_customer_id) = e.curr_customer_id
    ),

    blu_send_att AS (
        SELECT 
            bst.curr_customer_id,
            bst.blu_email_received,
            bst.blu_email_clicked,
            bst.send_email_received,
            bst.send_email_clicked,
            bst.eps_email_received,
            bst.eps_email_clicked,
            COALESCE(a.sms_received, 0) AS sms_received,
            COALESCE(a.sms_clicked, 0) AS sms_clicked
        FROM 
            blu_send_trigger bst
        INNER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_data a 
        ON 
            bst.curr_customer_id = a.curr_customer_id
        WHERE
            a.message_type = 'Promotional'
    ),

    transformed AS (
        SELECT 
            curr_customer_id,
            blu_email_received + send_email_received + eps_email_received AS email_received_count,
            blu_email_clicked + send_email_clicked + eps_email_clicked AS email_clicked_count,
            sms_received,
            sms_clicked,
            CASE 
                WHEN (blu_email_received + send_email_received + eps_email_received) = 0 THEN 0 
                ELSE (blu_email_clicked + send_email_clicked + eps_email_clicked) / 
                     (blu_email_received + send_email_received + eps_email_received)
            END AS email_engagement_ratio,
            CASE 
                WHEN sms_received = 0 THEN 0 
                ELSE sms_clicked / sms_received 
            END AS sms_engagement_ratio
        FROM 
            blu_send_att
    ),

    feature_eng AS (
        SELECT 
            t.curr_customer_id,
            t.email_received_count,
            t.email_clicked_count,
            t.sms_received,
            t.sms_clicked,
            t.email_engagement_ratio,
            t.sms_engagement_ratio,
            l.*
        FROM 
            transformed t
        INNER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_data l 
        ON 
            t.curr_customer_id = l.CURR_CMD_ID
    ),

    with_channel AS (
        SELECT 
            *,
            CASE 
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio = 0 THEN 'neither'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio = 0 THEN 'email'
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio > 0 THEN 'sms'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio > 0 THEN 'both'
                ELSE 'Unknown'
            END AS preferred_channel
        FROM 
            feature_eng
    )

SELECT 
    curr_customer_id,
    email_received_count,
    email_clicked_count,
    sms_received,
    sms_clicked,
    email_engagement_ratio,
    sms_engagement_ratio,
    TOTAL_ITEMS,
    RETURNED_ITEMS,
    RETURN_PERCENTAGE,
    ONLINE_ITEMS,
    ONLINE_PERCENTAGE,
    DISCOUNTED_ITEMS,
    DISCOUNTED_PERCENTAGE,
    AVG_ITEM_PRICE,
    L12M_NET_SALES,
    RECENCY,
    FREQUENCY,
    VINTAGE_DAYS,
    preferred_channel,
    CASE 
        WHEN preferred_channel = 'neither' THEN 0
        WHEN preferred_channel = 'email' THEN 1
        WHEN preferred_channel = 'sms' THEN 2
        WHEN preferred_channel = 'both' THEN 3
    END AS preferred_channel_encoded
FROM 
    with_channel);
    """
    return query

# Function to promotional split SQL query string
def promo_train_test_split(brand):
    '''
    Script to extract Promotional message type and perform modeling
    '''
    query = f"""SELECT * FROM NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_Promotional;
    """
    return query

# Function to trigger split SQL query string
def trigger_train_test_split(brand):
    '''
    Script to extract Trigger message type and perform modeling
    '''
    query = f"""select * from NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_trigger;
    """
    return query

"""
==================================================================================================
==================================================================================================

SECTION 2: Prediction Functions

==================================================================================================
==================================================================================================
"""



def get_query_attentive_pred_data(brand):
    '''
    Script to extract past one year data from attentive for predction
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_pred_data AS
(WITH conversion_ranked AS (
    SELECT
        b.email,
        b.timestamp,
        b.message_id,
        ROW_NUMBER() OVER (PARTITION BY b.email, b.message_id ORDER BY b.timestamp ASC) AS row_num
    FROM
        NMEDWPRD_DB.EDW.ATT_CONVERSION_T b
    WHERE
        b.timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND b.brand = '{brand}'
),
classified_messages AS (
    SELECT
        a.email AS messaging_email,
        a.timestamp AS messaging_timestamp,
        c.timestamp AS conversion_timestamp,
        CASE
            WHEN     a.message_name ILIKE '%new to sale%' OR
                     a.message_name ILIKE '%price decrease%' OR
                     a.message_name ILIKE '%new arrivals%' OR
                     a.message_name ILIKE '%best sellers%' OR
                     a.message_name ILIKE '%weekly picks%' OR
                     a.message_name ILIKE '%vip%' OR
                     a.message_name ILIKE '%sweep up promo%' OR
                     a.message_name ILIKE '%purchase anniversary%' OR
                     a.message_name ILIKE '%next best category%' OR
                     a.message_name ILIKE '%birthday%' OR
                     a.message_name ILIKE '%low inventory%' OR
                     a.message_name ILIKE '%at-risk audience expansion%' OR
                     a.message_name ILIKE '%post purchase repeat buyer%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                     a.message_name ILIKE '%beauty replenishment%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                     a.message_name ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                     a.message_name ILIKE '%post purchase 1x buyer%' OR
                     a.message_name ILIKE '%my favorites%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                     a.message_name ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                     a.message_name ILIKE '%sale%' OR
                     a.message_name ILIKE '%launch%' OR
                     a.message_name ILIKE '%new offer%' OR
                     a.message_name ILIKE '%discount%' OR
                     a.message_name ILIKE '%offer%' OR
                     a.message_name ILIKE '%special%' OR
                     a.message_name ILIKE '%promo%' OR
                     a.message_name ILIKE '%deal%' OR
                     a.message_name ILIKE '%limited time%' OR
                     a.message_name ILIKE '%coupon%' OR
                     a.message_name ILIKE '%free%' OR
                     a.message_name ILIKE '%exclusive%' OR
                     a.message_name ILIKE '%buy now%' OR
                     a.message_name ILIKE '%clearance%' OR
                     a.message_name ILIKE '%flash sale%' OR
                     a.message_name ILIKE '%holiday sale%' OR
                     a.message_name ILIKE '%seasonal offer%' OR
                     a.message_name ILIKE '%member exclusive%' OR
                     a.message_name ILIKE '%early access%' OR
                     a.message_name ILIKE '%limited edition%' OR
                     a.message_name ILIKE '%two-for-one%' OR
                     a.message_name ILIKE '%bundle offer%' OR
                     a.message_name ILIKE '%friends and family%' OR
                     a.message_name ILIKE '%special event%' OR
                     a.message_name ILIKE '%flash promo%' OR
                     a.message_name ILIKE '%pre-sale%' OR
                     a.message_name ILIKE '%limited quantities%' OR
                     a.message_name ILIKE '%doorbuster%' OR
                     a.message_name ILIKE '%cyber monday%' OR
                     a.message_name ILIKE '%new%' OR
                     a.message_name ILIKE '%black friday%' OR
                     a.message_name ILIKE '%gift with purchase%' OR
                     a.message_name ILIKE '%loyalty rewards%' OR
                     a.message_name ILIKE '%bonus points%' OR
                     a.message_name ILIKE '%free shipping%' OR
                     a.message_name ILIKE '%buy more save more%' OR
                     a.message_name ILIKE '%pre-order%' OR
                     a.message_name ILIKE '%kick%'
                     THEN 'Promotional'
                WHEN a.message_name ILIKE '%abandonment%' OR
                     a.message_name ILIKE '%cart abandonment%' OR
                     a.message_name ILIKE '%search abandonment%' OR
                     a.message_name ILIKE '%reminder%' OR
                     a.message_name ILIKE '%follow-up%' OR
                     a.message_name ILIKE '%thank you%' OR
                     a.message_name ILIKE '%survey%' OR
                     a.message_name ILIKE '%feedback%' OR
                     a.message_name ILIKE '%abandoned cart%' OR
                     a.message_name ILIKE '%renewal%' OR
                     a.message_name ILIKE '%welcome%' OR
                     a.message_name ILIKE '%personalized%' OR
                     a.message_name ILIKE '%reactivate%' OR
                     a.message_name ILIKE '%engagement%' OR
                     a.message_name ILIKE '%re-engagement%' OR
                     a.message_name ILIKE '%lost customer%' OR
                     a.message_name ILIKE '%inactivity%' OR
                     a.message_name ILIKE '%churn prevention%' OR
                     a.message_name ILIKE '%win-back%' OR
                     a.message_name ILIKE '%first purchase%' OR
                     a.message_name ILIKE '%order confirmation%' OR
                     a.message_name ILIKE '%delivery confirmation%' OR
                     a.message_name ILIKE '%replenishment reminder%' OR
                     a.message_name ILIKE '%subscription reminder%' OR
                     a.message_name ILIKE '%trial expiration%' OR
                     a.message_name ILIKE '%last chance%' OR
                     a.message_name ILIKE '%return reminder%' OR
                     a.message_name ILIKE '%missed opportunity%'
                     THEN 'Trigger'
                ELSE NULL
            END AS message_type
    FROM
        NMEDWPRD_DB.EDW.ATT_MESSAGING_T a
    LEFT JOIN
        conversion_ranked c
    ON
        a.message_id = c.message_id AND a.email = c.email AND c.row_num = 1
    WHERE
        a.timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND a.brand = '{brand}'
),
CUST_WITH_L12NS AS (
        SELECT 
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE 
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING 
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.messaging_timestamp IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.conversion_timestamp IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(B.EMAIL_ADDRESS)) = LOWER(TRIM(A.messaging_email))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS sms_received,
    SUM(click_date) AS sms_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query


def get_query_bluecore_pred_data(brand):
    '''
    Script to extract past one year data from bluecore for prediction
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_pred_data AS 
(WITH classified_messages AS (
    SELECT
        d.EMAIL,
        d.EVENT_TIME,
        c.EVENT_TIME AS click_event_time,
        CASE
            WHEN d.SUBJECT_LINE ILIKE '%new to sale%' OR
                 d.SUBJECT_LINE ILIKE '%price decrease%' OR
                 d.SUBJECT_LINE ILIKE '%new arrivals%' OR
                 d.SUBJECT_LINE ILIKE '%best sellers%' OR
                 d.SUBJECT_LINE ILIKE '%weekly picks%' OR
                 d.SUBJECT_LINE ILIKE '%vip%' OR
                 d.SUBJECT_LINE ILIKE '%sweep up promo%' OR
                 d.SUBJECT_LINE ILIKE '%purchase anniversary%' OR
                 d.SUBJECT_LINE ILIKE '%next best category%' OR
                 d.SUBJECT_LINE ILIKE '%birthday%' OR
                 d.SUBJECT_LINE ILIKE '%low inventory%' OR
                 d.SUBJECT_LINE ILIKE '%at-risk audience expansion%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%beauty replenishment%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                 d.SUBJECT_LINE ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%my favorites%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                 d.SUBJECT_LINE ILIKE '%sale%' OR
                 d.SUBJECT_LINE ILIKE '%launch%' OR
                 d.SUBJECT_LINE ILIKE '%new offer%' OR
                 d.SUBJECT_LINE ILIKE '%discount%' OR
                 d.SUBJECT_LINE ILIKE '%offer%' OR
                 d.SUBJECT_LINE ILIKE '%special%' OR
                 d.SUBJECT_LINE ILIKE '%promo%' OR
                 d.SUBJECT_LINE ILIKE '%deal%' OR
                 d.SUBJECT_LINE ILIKE '%limited time%' OR
                 d.SUBJECT_LINE ILIKE '%coupon%' OR
                 d.SUBJECT_LINE ILIKE '%free%' OR
                 d.SUBJECT_LINE ILIKE '%exclusive%' OR
                 d.SUBJECT_LINE ILIKE '%buy now%' OR
                 d.SUBJECT_LINE ILIKE '%clearance%' OR
                 d.SUBJECT_LINE ILIKE '%flash sale%' OR
                 d.SUBJECT_LINE ILIKE '%holiday sale%' OR
                 d.SUBJECT_LINE ILIKE '%seasonal offer%' OR
                 d.SUBJECT_LINE ILIKE '%member exclusive%' OR
                 d.SUBJECT_LINE ILIKE '%early access%' OR
                 d.SUBJECT_LINE ILIKE '%limited edition%' OR
                 d.SUBJECT_LINE ILIKE '%two-for-one%' OR
                 d.SUBJECT_LINE ILIKE '%bundle offer%' OR
                 d.SUBJECT_LINE ILIKE '%friends and family%' OR
                 d.SUBJECT_LINE ILIKE '%special event%' OR
                 d.SUBJECT_LINE ILIKE '%flash promo%' OR
                 d.SUBJECT_LINE ILIKE '%pre-sale%' OR
                 d.SUBJECT_LINE ILIKE '%limited quantities%' OR
                 d.SUBJECT_LINE ILIKE '%doorbuster%' OR
                 d.SUBJECT_LINE ILIKE '%cyber monday%' OR
                 d.SUBJECT_LINE ILIKE '%new%' OR
                 d.SUBJECT_LINE ILIKE '%black friday%' OR
                 d.SUBJECT_LINE ILIKE '%gift with purchase%' OR
                 d.SUBJECT_LINE ILIKE '%loyalty rewards%' OR
                 d.SUBJECT_LINE ILIKE '%bonus points%' OR
                 d.SUBJECT_LINE ILIKE '%free shipping%' OR
                 d.SUBJECT_LINE ILIKE '%buy more save more%' OR
                 d.SUBJECT_LINE ILIKE '%pre-order%' OR
                 d.SUBJECT_LINE ILIKE '%kick%'
                 THEN 'Promotional'
            WHEN d.SUBJECT_LINE ILIKE '%abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%cart abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%search abandonment%' OR
                 d.SUBJECT_LINE ILIKE '%reminder%' OR
                 d.SUBJECT_LINE ILIKE '%follow-up%' OR
                 d.SUBJECT_LINE ILIKE '%thank you%' OR
                 d.SUBJECT_LINE ILIKE '%survey%' OR
                 d.SUBJECT_LINE ILIKE '%feedback%' OR
                 d.SUBJECT_LINE ILIKE '%abandoned cart%' OR
                 d.SUBJECT_LINE ILIKE '%renewal%' OR
                 d.SUBJECT_LINE ILIKE '%welcome%' OR
                 d.SUBJECT_LINE ILIKE '%personalized%' OR
                 d.SUBJECT_LINE ILIKE '%reactivate%' OR
                 d.SUBJECT_LINE ILIKE '%engagement%' OR
                 d.SUBJECT_LINE ILIKE '%re-engagement%' OR
                 d.SUBJECT_LINE ILIKE '%lost customer%' OR
                 d.SUBJECT_LINE ILIKE '%inactivity%' OR
                 d.SUBJECT_LINE ILIKE '%churn prevention%' OR
                 d.SUBJECT_LINE ILIKE '%win-back%' OR
                 d.SUBJECT_LINE ILIKE '%first purchase%' OR
                 d.SUBJECT_LINE ILIKE '%order confirmation%' OR
                 d.SUBJECT_LINE ILIKE '%delivery confirmation%' OR
                 d.SUBJECT_LINE ILIKE '%replenishment reminder%' OR
                 d.SUBJECT_LINE ILIKE '%subscription reminder%' OR
                 d.SUBJECT_LINE ILIKE '%trial expiration%' OR
                 d.SUBJECT_LINE ILIKE '%last chance%' OR
                 d.SUBJECT_LINE ILIKE '%return reminder%' OR
                 d.SUBJECT_LINE ILIKE '%missed opportunity%'
                 THEN 'Trigger'
            ELSE NULL
        END AS message_type
    FROM
        NMEDWPRD_DB.EDW.BLUECORE_DELIVER_T d
    LEFT JOIN
        NMEDWPRD_DB.EDW.BLUECORE_CLICK_T c
        ON d.NQE = c.NQE
    WHERE
        d.EVENT_TIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE  
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id, 
        CASE WHEN A.EVENT_TIME IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_event_time IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.EMAIL)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS blu_email_received,
    SUM(click_date) AS blu_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query

def get_query_sendgrid_pred_data(brand):
    '''
    Script to extract past one year data from sendgrid for prediction
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_pred_data AS 
(WITH classified_messages AS (
    SELECT
        external_address,
        delivered_timestamp,
        click_timestamp,
        CASE
            WHEN message_subject ILIKE '%new to sale%' OR
                 message_subject ILIKE '%price decrease%' OR
                 message_subject ILIKE '%new arrivals%' OR
                 message_subject ILIKE '%best sellers%' OR
                 message_subject ILIKE '%weekly picks%' OR
                 message_subject ILIKE '%vip%' OR
                 message_subject ILIKE '%sweep up promo%' OR
                 message_subject ILIKE '%purchase anniversary%' OR
                 message_subject ILIKE '%next best category%' OR
                 message_subject ILIKE '%birthday%' OR
                 message_subject ILIKE '%low inventory%' OR
                 message_subject ILIKE '%at-risk audience expansion%' OR
                 message_subject ILIKE '%post purchase repeat buyer t1_a - 72h%' OR
                 message_subject ILIKE '%post purchase repeat buyer t2_a - 72h%' OR
                 message_subject ILIKE '%beauty replenishment%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - launch%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - reminder%' OR
                 message_subject ILIKE '%7. nmo $100-off private promo (june) - final day%' OR
                 message_subject ILIKE '%post purchase 1x buyer t2_a - 72h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t2_b - 24h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t1_a - 72h%' OR
                 message_subject ILIKE '%post purchase 1x buyer t1_b - 24h%' OR
                 message_subject ILIKE '%my favorites%' OR
                 message_subject ILIKE '%post purchase repeat buyer t2_b - 24h%' OR
                 message_subject ILIKE '%post purchase repeat buyer t1_b - 24h%' OR
                 message_subject ILIKE '%sale%' OR
                 message_subject ILIKE '%launch%' OR
                 message_subject ILIKE '%new offer%' OR
                 message_subject ILIKE '%discount%' OR
                 message_subject ILIKE '%offer%' OR
                 message_subject ILIKE '%special%' OR
                 message_subject ILIKE '%promo%' OR
                 message_subject ILIKE '%deal%' OR
                 message_subject ILIKE '%limited time%' OR
                 message_subject ILIKE '%coupon%' OR
                 message_subject ILIKE '%free%' OR
                 message_subject ILIKE '%exclusive%' OR
                 message_subject ILIKE '%buy now%' OR
                 message_subject ILIKE '%clearance%' OR
                 message_subject ILIKE '%flash sale%' OR
                 message_subject ILIKE '%holiday sale%' OR
                 message_subject ILIKE '%seasonal offer%' OR
                 message_subject ILIKE '%member exclusive%' OR
                 message_subject ILIKE '%early access%' OR
                 message_subject ILIKE '%limited edition%' OR
                 message_subject ILIKE '%two-for-one%' OR
                 message_subject ILIKE '%bundle offer%' OR
                 message_subject ILIKE '%friends and family%' OR
                 message_subject ILIKE '%special event%' OR
                 message_subject ILIKE '%flash promo%' OR
                 message_subject ILIKE '%pre-sale%' OR
                 message_subject ILIKE '%limited quantities%' OR
                 message_subject ILIKE '%doorbuster%' OR
                 message_subject ILIKE '%cyber monday%' OR
                 message_subject ILIKE '%new%' OR
                 message_subject ILIKE '%black friday%' OR
                 message_subject ILIKE '%gift with purchase%' OR
                 message_subject ILIKE '%loyalty rewards%' OR
                 message_subject ILIKE '%bonus points%' OR
                 message_subject ILIKE '%free shipping%' OR
                 message_subject ILIKE '%buy more save more%' OR
                 message_subject ILIKE '%pre-order%' OR
                 message_subject ILIKE '%kick%'
                 THEN 'Promotional'
            WHEN message_subject ILIKE '%abandonment%' OR
                 message_subject ILIKE '%cart abandonment%' OR
                 message_subject ILIKE '%search abandonment%' OR
                 message_subject ILIKE '%reminder%' OR
                 message_subject ILIKE '%follow-up%' OR
                 message_subject ILIKE '%thank you%' OR
                 message_subject ILIKE '%survey%' OR
                 message_subject ILIKE '%feedback%' OR
                 message_subject ILIKE '%abandoned cart%' OR
                 message_subject ILIKE '%renewal%' OR
                 message_subject ILIKE '%welcome%' OR
                 message_subject ILIKE '%personalized%' OR
                 message_subject ILIKE '%reactivate%' OR
                 message_subject ILIKE '%engagement%' OR
                 message_subject ILIKE '%re-engagement%' OR
                 message_subject ILIKE '%lost customer%' OR
                 message_subject ILIKE '%inactivity%' OR
                 message_subject ILIKE '%churn prevention%' OR
                 message_subject ILIKE '%win-back%' OR
                 message_subject ILIKE '%first purchase%' OR
                 message_subject ILIKE '%order confirmation%' OR
                 message_subject ILIKE '%delivery confirmation%' OR
                 message_subject ILIKE '%replenishment reminder%' OR
                 message_subject ILIKE '%subscription reminder%' OR
                 message_subject ILIKE '%trial expiration%' OR
                 message_subject ILIKE '%last chance%' OR
                 message_subject ILIKE '%return reminder%' OR
                 message_subject ILIKE '%missed opportunity%'
                 THEN 'Trigger'
            ELSE NULL
        END AS message_type
    FROM
        NMEDWPRD_DB.COMHUB.CONVERSATION_T
    WHERE
        source_id <> 'CCC'
        AND date_created BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND conversation_direction <> 'Inbound'
        AND message_type = 'EMAIL'
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id,
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.delivered_timestamp IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_timestamp IS NULL THEN 0 ELSE 1 END AS click_date,
        A.message_type
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.external_address)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        A.message_type IN ('Promotional', 'Trigger')
        AND
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    message_type,
    SUM(send_date) AS send_email_received,
    SUM(click_date) AS send_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id,
    message_type);
    """
    return query

def get_query_epsilon_pred_data(brand):
    '''
    Script to extract past one year data from Epsilon for prediction
    '''
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_epsilon_pred_data AS 
    (WITH classified_messages AS (SELECT 
        a.email,
        a.send_date,
        a.click_date
    FROM
        PDWDM.CHEETAH_EMAIL_CONSOLIDATED_T a 
    JOIN
        NMEDWPRD_DB.EPSILON.EPS_ACTIVITY_T b ON a.message_id = b.service_communication_id
    WHERE 
        b.brand = '{brand}'
        AND a.send_date BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE 
        AND b.original_event_timestamp BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE 
        AND b.email_email_type = 'Promo'
),
CUST_WITH_L12NS AS (
        SELECT
            CURR_CMD_ID AS curr_customer_id, 
            SUM(total_itm_purchase_amount) AS L12M_NS
        FROM
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
        WHERE
            BRAND = '{brand}'
            AND ITM_ITEM_STATUS IN ('S', 'R')
            AND HDR_TRAN_DATETIME BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        GROUP BY
            CURR_CMD_ID
        HAVING
            SUM(total_itm_purchase_amount) > 0
),
MERGED AS (
    SELECT
        B.curr_customer_id,
        CASE WHEN A.send_date IS NULL THEN 0 ELSE 1 END AS send_date,
        CASE WHEN A.click_date IS NULL THEN 0 ELSE 1 END AS click_date,
    FROM
        classified_messages A
    INNER JOIN
        PDWDM.cmd_customer_email_pick_best_t B
    ON LOWER(TRIM(A.email)) = LOWER(TRIM(B.EMAIL_ADDRESS))
    WHERE
        B.curr_customer_id IN (SELECT curr_customer_id FROM CUST_WITH_L12NS)
)
SELECT
    curr_customer_id,
    SUM(send_date) AS eps_email_received,
    SUM(click_date) AS eps_email_clicked
FROM
    MERGED
GROUP BY
    curr_customer_id);
    """
    return query

def get_query_customer_transactional_features_pred_data (brand):
    '''
    Script to extract customer transactional features i.e. profile, rfm, vintage all combined for prediction
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary

    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_pred_data AS
(WITH l12m_metrics AS (
    SELECT
        curr_cmd_id,
        COUNT(*) AS total_items,
        SUM(CASE WHEN itm_item_status = 'R' THEN 1 ELSE 0 END) AS returned_items,
        (returned_items/total_items) AS return_percentage,
        SUM(CASE WHEN CHANNEL = 'Online' THEN 1 ELSE 0 END) AS online_items,
        (online_items/total_items) AS online_percentage,
        SUM(CASE WHEN ((itm_item_status = 'S') AND (dsc_unit_discount_amt > 0)) THEN 1 ELSE 0 END) AS discounted_items,
        (discounted_items/total_items) AS discounted_percentage,
        ROUND(AVG(CASE WHEN itm_item_status = 'S' THEN total_itm_purchase_amount ELSE NULL END)) AS avg_item_price,
        SUM(total_itm_purchase_amount) AS l12m_net_sales,
        ROUND((CURRENT_DATE - MAX(hdr_tran_datetime)) / 30, 2) AS Recency,
        COUNT(DISTINCT hdr_tran_datetime) AS Frequency,
    FROM 
        NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
    WHERE 
        brand = '{brand}'
        AND hdr_tran_datetime BETWEEN DATEADD(YEAR, -1, CURRENT_DATE) AND CURRENT_DATE
        AND itm_item_status IN ('S', 'R')
    GROUP BY 
        curr_cmd_id
),
vintage AS (
    SELECT 
        curr_cmd_id,
        DATEDIFF(day, MIN(hdr_tran_datetime), CURRENT_DATE) AS vintage_days
    FROM 
            NMEDWPRD_DB.PDWDM.ALL_SALES_AND_OPEN_ORDERS_MV
    WHERE 
        brand = '{brand}'
        AND curr_cmd_id IN (SELECT curr_cmd_id FROM l12m_metrics)
        AND itm_item_status = 'S'
    GROUP BY
        curr_cmd_id
    )
SELECT 
    A.*,
    B.vintage_days
FROM 
    l12m_metrics A
    JOIN vintage B
    ON A.curr_cmd_id = B.curr_cmd_id
ORDER BY 
    A.curr_cmd_id);"""
    return query

def get_query_trigger_feature_engg_pred_data (brand):
    '''
    Script to extract trigger message type and perform feature engineering
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary
    
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_trigger_pred AS
(WITH 
    blu_data AS (
        SELECT 
            curr_customer_id,
            blu_email_received,
            blu_email_clicked
        FROM
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_pred_data
        WHERE
            message_type = 'Trigger'
    ),
    send_data AS (
        SELECT
            curr_customer_id,
            send_email_received,
            send_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_pred_data
        WHERE 
            message_type = 'Trigger'
    ),
    blu_send_trigger AS (
        SELECT 
            COALESCE(b.curr_customer_id, s.curr_customer_id) AS curr_customer_id,
            COALESCE(b.blu_email_received, 0) AS blu_email_received,
            COALESCE(b.blu_email_clicked, 0) AS blu_email_clicked,
            COALESCE(s.send_email_received, 0) AS send_email_received,
            COALESCE(s.send_email_clicked, 0) AS send_email_clicked
        FROM 
            blu_data b
        FULL OUTER JOIN 
            send_data s 
        ON 
            b.curr_customer_id = s.curr_customer_id
    ),
    blu_send_att AS (
        SELECT 
            bst.curr_customer_id,
            bst.blu_email_received,
            bst.blu_email_clicked,
            bst.send_email_received,
            bst.send_email_clicked,
            COALESCE(a.sms_received, 0) AS sms_received,
            COALESCE(a.sms_clicked, 0) AS sms_clicked
        FROM 
            blu_send_trigger bst
        FULL OUTER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_pred_data a 
        ON 
            bst.curr_customer_id = a.curr_customer_id
        WHERE
            a.message_type = 'Trigger'
    ),
    transformed AS (
        SELECT 
            curr_customer_id,
            blu_email_received + send_email_received AS email_received_count,
            blu_email_clicked + send_email_clicked AS email_clicked_count,
            sms_received,
            sms_clicked,
            CASE 
                WHEN (blu_email_received + send_email_received) = 0 THEN 0 
                ELSE (blu_email_clicked + send_email_clicked) / (blu_email_received + send_email_received)
            END AS email_engagement_ratio,
            CASE 
                WHEN sms_received = 0 THEN 0 
                ELSE sms_clicked / sms_received 
            END AS sms_engagement_ratio
        FROM 
            blu_send_att
    ),
    feature_eng AS (
        SELECT 
            t.curr_customer_id,
            t.email_received_count,
            t.email_clicked_count,
            t.sms_received,
            t.sms_clicked,
            t.email_engagement_ratio,
            t.sms_engagement_ratio,
            l.*
        FROM 
            transformed t
        INNER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_pred_data l 
        ON 
            t.curr_customer_id = l.CURR_CMD_ID
    ),
    with_channel AS (
        SELECT 
            *,
            CASE 
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio = 0 THEN 'neither'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio = 0 THEN 'email'
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio > 0 THEN 'sms'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio > 0 THEN 'both'
                ELSE 'Unknown'
            END AS preferred_channel
        FROM 
            feature_eng
    )
SELECT 
    curr_customer_id,
    email_received_count,
    email_clicked_count,
    sms_received,
    sms_clicked,
    email_engagement_ratio,
    sms_engagement_ratio,
    TOTAL_ITEMS,
    RETURNED_ITEMS,
    RETURN_PERCENTAGE,
    ONLINE_ITEMS,
    ONLINE_PERCENTAGE,
    DISCOUNTED_ITEMS,
    DISCOUNTED_PERCENTAGE,
    AVG_ITEM_PRICE,
    L12M_NET_SALES,
    RECENCY,
    FREQUENCY,
    VINTAGE_DAYS,
    preferred_channel,
    CASE
        WHEN preferred_channel = 'neither' THEN 0
        WHEN preferred_channel = 'email' THEN 1
        WHEN preferred_channel = 'sms' THEN 2
        WHEN preferred_channel = 'both' THEN 3
    END AS preferred_channel_encoded
FROM with_channel);
    """
    return query

def get_query_promotional_feature_engg_pred_data (brand):
    '''
    Script to extract Promotional message type and perform feature engineering for prediction
    '''
    brand = brand.strip().replace("'", "''")  # Escape single quotes if necessary
    
    query = f"""CREATE OR REPLACE TABLE NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_promotional_pred AS
(WITH 
    blu_data AS (
        SELECT 
            curr_customer_id,
            blu_email_received,
            blu_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_bluecore_pred_data
        WHERE 
            message_type = 'Promotional'
    ),

    send_data AS (
        SELECT 
            curr_customer_id,
            send_email_received,
            send_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_sendgrid_pred_data
        WHERE 
            message_type = 'Promotional'
    ),

    eps_data AS (
        SELECT 
            curr_customer_id,
            eps_email_received,
            eps_email_clicked
        FROM 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_epsilon_pred_data
    ),

    blu_send_trigger AS (
        SELECT 
            COALESCE(b.curr_customer_id, s.curr_customer_id, e.curr_customer_id) AS curr_customer_id,
            COALESCE(b.blu_email_received, 0) AS blu_email_received,
            COALESCE(b.blu_email_clicked, 0) AS blu_email_clicked,
            COALESCE(s.send_email_received, 0) AS send_email_received,
            COALESCE(s.send_email_clicked, 0) AS send_email_clicked,
            COALESCE(e.eps_email_received, 0) AS eps_email_received,
            COALESCE(e.eps_email_clicked, 0) AS eps_email_clicked
        FROM 
            blu_data b
        FULL OUTER JOIN 
            send_data s ON b.curr_customer_id = s.curr_customer_id
        FULL OUTER JOIN 
            eps_data e ON COALESCE(b.curr_customer_id, s.curr_customer_id) = e.curr_customer_id
    ),

    blu_send_att AS (
        SELECT 
            bst.curr_customer_id,
            bst.blu_email_received,
            bst.blu_email_clicked,
            bst.send_email_received,
            bst.send_email_clicked,
            bst.eps_email_received,
            bst.eps_email_clicked,
            COALESCE(a.sms_received, 0) AS sms_received,
            COALESCE(a.sms_clicked, 0) AS sms_clicked
        FROM 
            blu_send_trigger bst
        FULL OUTER JOIN 
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_attentive_pred_data a 
        ON 
            bst.curr_customer_id = a.curr_customer_id
        WHERE
            a.message_type = 'Promotional'
    ),

    transformed AS (
        SELECT 
            curr_customer_id,
            blu_email_received + send_email_received + eps_email_received AS email_received_count,
            blu_email_clicked + send_email_clicked + eps_email_clicked AS email_clicked_count,
            sms_received,
            sms_clicked,
            CASE 
                WHEN (blu_email_received + send_email_received + eps_email_received) = 0 THEN 0 
                ELSE (blu_email_clicked + send_email_clicked + eps_email_clicked) / 
                     (blu_email_received + send_email_received + eps_email_received)
            END AS email_engagement_ratio,
            CASE 
                WHEN sms_received = 0 THEN 0 
                ELSE sms_clicked / sms_received 
            END AS sms_engagement_ratio
        FROM 
            blu_send_att
    ),

    feature_eng AS (
        SELECT 
            t.curr_customer_id,
            t.email_received_count,
            t.email_clicked_count,
            t.sms_received,
            t.sms_clicked,
            t.email_engagement_ratio,
            t.sms_engagement_ratio,
            l.*
        FROM 
            transformed t
        INNER JOIN  
            NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_transactional_features_pred_data l 
        ON 
            t.curr_customer_id = l.CURR_CMD_ID
    ),

    with_channel AS (
        SELECT 
            *,
            CASE 
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio = 0 THEN 'neither'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio = 0 THEN 'email'
                WHEN email_engagement_ratio = 0 AND sms_engagement_ratio > 0 THEN 'sms'
                WHEN email_engagement_ratio > 0 AND sms_engagement_ratio > 0 THEN 'both'
                ELSE 'Unknown'
            END AS preferred_channel
        FROM 
            feature_eng
    )

SELECT 
    curr_customer_id,
    email_received_count,
    email_clicked_count,
    sms_received,
    sms_clicked,
    email_engagement_ratio,
    sms_engagement_ratio,
    TOTAL_ITEMS,
    RETURNED_ITEMS,
    RETURN_PERCENTAGE,
    ONLINE_ITEMS,
    ONLINE_PERCENTAGE,
    DISCOUNTED_ITEMS,
    DISCOUNTED_PERCENTAGE,
    AVG_ITEM_PRICE,
    L12M_NET_SALES,
    RECENCY,
    FREQUENCY,
    VINTAGE_DAYS,
    preferred_channel,
    CASE 
        WHEN preferred_channel = 'neither' THEN 0
        WHEN preferred_channel = 'email' THEN 1
        WHEN preferred_channel = 'sms' THEN 2
        WHEN preferred_channel = 'both' THEN 3
    END AS preferred_channel_encoded
FROM with_channel);
    """
    return query

def promo_model_data_pred(brand):
    '''
    Script to extract Promotional message type and perform modeling for prediction
    '''
    query = f"""select * from NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_promotional_pred;
    """
    return query

def trigger_model_data_pred(brand):
    '''
    Script to extract Trigger message type and perform modeling for prediction
    '''
    query = f"""SELECT * FROM NMEDWPRD_DB.MKTSAND.{brand}_channel_preference_feature_engg_trigger_pred;
    """
    return query
















