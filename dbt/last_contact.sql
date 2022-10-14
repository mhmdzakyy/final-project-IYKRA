{{ config(materialized='table') }}

SELECT contact, month, day_of_week, duration FROM {{ ref('source_table') }}
