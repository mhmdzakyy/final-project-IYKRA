{{ config(materialized='table') }}

SELECT * FROM {{ source('staging','bank_2022-10-01') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-02') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-03') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-04') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-05') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-06') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-07') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-08') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-09') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-10') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-11') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-12') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-13') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-14') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-15') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-16') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-17') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-18') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-19') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-20') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-21') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-22') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-23') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-24') }}
UNION DISTINCT
SELECT * FROM {{ source('staging','bank_2022-10-25') }}
