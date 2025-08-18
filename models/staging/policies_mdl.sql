{{ 
  config( 
    materialized='incremental', 
    unique_key='policy_id',
    incremental_strategy='merge'
  ) 
}}

SELECT
   src.*,
   {{ audit_columns('policy_id') }}
FROM PR_DATABASE.PR_SCHEMA.policies AS src
left join {{ this }} as tgt
    on src.policy_id = tgt.policy_id