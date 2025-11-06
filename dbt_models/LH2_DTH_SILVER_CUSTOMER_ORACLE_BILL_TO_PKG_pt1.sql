
{{ config(
    materialized='table',
    transient=false,
    alias='TABLE'
) }}

SELECT *
                            FROM DEV.LH2_BRONZE_DEV.STEPH_APPS_HZ_CUST_SITE_USES_ALL
WHERE SITE_USE_CODE = "BILL_TO"

