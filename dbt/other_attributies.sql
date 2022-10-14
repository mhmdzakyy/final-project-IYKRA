{{ config(materialized='table') }}

SELECT campaign, pdays, previous, poutcome FROM {{ ref('source_table') }}