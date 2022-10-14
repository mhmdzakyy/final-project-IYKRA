{{ config(materialized='table') }}

SELECT emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed FROM {{ ref('source_table') }}
