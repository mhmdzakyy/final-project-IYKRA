{{ config(materialized='table') }}

SELECT id, age, job, marital, education, `default`, housing, loan FROM {{ ref('source_table') }}