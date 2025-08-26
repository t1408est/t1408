{#
  -------------------------------------------------------------------------
  Macro: load_with_audit_columns
  -------------------------------------------------------------------------
  Purpose:
    Orchestrates controlled ingestion of data from source tables into 
    staging target tables. Standard audit metadata columns 
    (CREATED_TS, UPDATED_TS, LOAD_ID, etc.) are applied to every load, 
    and execution details are logged into the ingestion run-control table.

  High-Level Flow:
    1. Discover list of source-to-target mappings from the config table.
       - If load_type = 'FULL_RUN':
         → Fetch all active mappings (optionally filtered by SOURCE_NAME).
       - If load_type = 'RERUN':
         → Fetch only mappings that failed in the last run (based on
           INGESTION_RUN_CONTROL) and are still active.
    2. Generate LOAD_ID from the run-control table:
       - FULL_RUN → assign a new load_id (max + 1).
       - RERUN    → reuse the latest load_id (max).
    3. Run-Control setup:
       - FULL_RUN → insert NOT_STARTED rows for each mapping.
       - RERUN    → do not insert; instead reuse and update existing rows.
    4. For each mapping:
       - Immediately set run-control status = FAILED (pessimistic).
       - Validate target table existence.
       - Insert data from source → target, appending audit columns.
       - On success, update run-control entry with SUCCESS, row count, etc.
    5. Capture all relevant operational metadata (timestamps, run user, 
       record counts, error messages).

  Parameters:
    - table_name   : Name of the ingestion configuration table.
    - schema_name  : Schema where the ingestion configuration table resides.
    - source_name  : (Optional) Limit processing to a specific source system.
    - load_type    : Must be either 'FULL_RUN' or 'RERUN'.
                     - FULL_RUN → load all active mappings
                     - RERUN    → only retry failed mappings from the latest run

  Environment Variables:
    - DBT_SOURCE_DATABASE : Database name for transactional/raw source tables.
    - DBT_SOURCE_SCHEMA   : Schema name for transactional/raw source tables.
    - DBT_TARGET_SCHEMA   : Schema name for staging/target tables.

  Audit / Logging:
    - All attempts logged in [schema_name].INGESTION_RUN_CONTROL.
    - Captures: run timestamps, source/target tables, source system,
      load ID, status (NOT_STARTED/FAILED/SUCCESS), error message, 
      row counts, run user, and macro name.

  Notes:
    - load_type accepts only 'FULL_RUN' or 'RERUN'.
    - In RERUN mode, existing run-control rows are updated (no new inserts).
    - A LIMIT 10 is applied temporarily to inserts (remove after validation).
  -------------------------------------------------------------------------
#}




{# -------------------------------------------------------------------------
   Helper Macro: _q
   Purpose:
     Safely quote a string literal for SQL (escapes single quotes).
   Usage:
     {{ _q("O'Reilly") }} → 'O''Reilly'
   ------------------------------------------------------------------------- #}
{% macro _q(val) -%}
  '{{ (val | string) | replace("'", "''") }}'
{%- endmacro %}


{% macro load_with_audit_columns(table_name=None, schema_name=None, source_name=None, load_type=None) %}

  {# --- Resolve source/target schema details from env vars --- #}
  {% set src_db   = env_var('DBT_SOURCE_DATABASE') %}
  {% set src_sch  = env_var('DBT_SOURCE_SCHEMA') %}
  {% set tgt_sch  = env_var('DBT_TARGET_SCHEMA') %}

  {# --- Validate load_type --- #}
  {% if load_type not in ['FULL_RUN', 'RERUN'] %}
    {% do exceptions.raise_compiler_error(
        "Invalid load_type: '" ~ load_type ~ "'. Allowed values are: FULL_RUN or RERUN."
    ) %}
  {% endif %}

  {# --- Build query to fetch active ingestion mappings from config table --- #}
  {# --- Build query based on LOAD_TYPE --- #}
  {% if load_type == 'RERUN' %}
    {% set query %}
      select 
        TRAN_TABLE_NAME as source_table,
        RAW_TABLE_NAME  as target_table,
        SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
        and UPPER(TRAN_TABLE_NAME) IN 
                    (select UPPER(SOURCE_TABLE) from {{ schema_name }}.INGESTION_RUN_CONTROL
                        WHERE LOAD_ID = (SELECT MAX(LOAD_ID) from {{ schema_name }}.INGESTION_RUN_CONTROL)
                        and upper(STATUS) != 'SUCCESS')
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}

  {% elif load_type == 'FULL_RUN' %}
    {% set query %}
      select 
        TRAN_TABLE_NAME as source_table,
        RAW_TABLE_NAME  as target_table,
        SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}
  {% endif %}

  {# --- Execute config query --- #}
  {% set results = run_query(query) %}
  {% if execute %}
    {% set tbls = results.rows %}
  {% else %}
    {% set tbls = [] %}
  {% endif %}

  {# --- Fail early if config table has no active mappings --- #}
  {% if tbls | length == 0 %}
    {% do exceptions.raise_compiler_error("No active ingestion mappings found in config table.") %}
  {% endif %}

  {# -----------------------------------------------------------------------
     STEP 1: Generate Load Identifier
     - FULL_RUN → assign a new load_id (max + 1)
     - RERUN    → reuse the latest load_id (max)
     ----------------------------------------------------------------------- #}
  {% if execute %}
    {% if load_type == 'FULL_RUN' %}
      {% set load_id_res = run_query(
          "select coalesce(max(LOAD_ID)+1,1) as load_id 
           from " ~ schema_name ~ ".INGESTION_RUN_CONTROL"
      ) %}
    {% elif load_type == 'RERUN' %}
      {% set load_id_res = run_query(
          "select coalesce(max(LOAD_ID),1) as load_id 
           from " ~ schema_name ~ ".INGESTION_RUN_CONTROL"
      ) %}
    {% endif %}
    {% set load_id = load_id_res.columns[0].values()[0] %}
  {% else %}
      {% set load_id = 1 %}
  {% endif %}

  {# -----------------------------------------------------------------------
     STEP 2: Run-Control Setup
     - FULL_RUN → Insert NOT_STARTED row for each mapping.
     - RERUN    → Do not insert new rows (reuse existing load_id).
     ----------------------------------------------------------------------- #}
  {% if load_type == 'FULL_RUN' %}
    {% for row in tbls %}
      {% set src_tbl  = row[0] %}
      {% set tgt_tbl  = row[1] %}
      {% set src_name = row[2] %}

      {% do run_query(
        "insert into " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
        "(RUN_TS, COMPLETED_TS, SOURCE_TABLE, TARGET_TABLE, SOURCE_NAME, LOAD_ID, STATUS, RUN_BY, MACRO_NAME) " ~
        "values (" ~
            "current_timestamp, " ~
            "NULL, " ~
            "'" ~ src_tbl ~ "', " ~
            "'" ~ tgt_tbl ~ "', " ~
            "'" ~ src_name ~ "', " ~
            load_id|string ~ ", " ~
            "'NOT_STARTED', " ~
            "current_user, " ~
            "'load_with_audit_columns')"
      ) %}
    {% endfor %}
  {% elif load_type == 'RERUN' %}
    {% do log("RERUN mode: reusing existing load_id=" ~ load_id|string ~ 
              " → no new inserts in INGESTION_RUN_CONTROL", info=True) %}
  {% endif %}

  {# -----------------------------------------------------------------------
     STEP 3: Process Each Mapping
     - Update status to FAILED, then attempt insert with audit cols.
     - On success, mark SUCCESS and record row count.
     ----------------------------------------------------------------------- #}
  {% for row in tbls %}
    {% set src_tbl  = row[0] %}
    {% set tgt_tbl  = row[1] %}
    {% set src_name = row[2] %}

    {% set src_rel = adapter.get_relation(database=src_db, schema=src_sch, identifier=src_tbl) %}
    {% set tgt_rel = adapter.get_relation(database=src_db, schema=tgt_sch, identifier=tgt_tbl) %}

    {% do log("Processing mapping: " ~ src_tbl ~ " → " ~ tgt_tbl, info=True) %}

    {# --- Mark run-control row as FAILED (pessimistic) --- #}
    {% do run_query(
      "update " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
      "set STATUS='FAILED', ERROR_MESSAGE='Insert started but not completed', COMPLETED_TS=current_timestamp " ~
      "where SOURCE_TABLE='" ~ src_tbl ~ "' and TARGET_TABLE='" ~ tgt_tbl ~ "' and LOAD_ID=" ~ load_id|string
    ) %}

    {# --- STEP 3a: Validate Target Table Existence --- #}
    {% if not tgt_rel %}
      {% do log("Target table " ~ tgt_tbl ~ " not found. Skipping load.", info=True) %}
      {% do run_query(
        "update " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
        "set STATUS='FAILED', ERROR_MESSAGE='Target table not found', COMPLETED_TS=current_timestamp " ~
        "where SOURCE_TABLE='" ~ src_tbl ~ "' and TARGET_TABLE='" ~ tgt_tbl ~ "' and LOAD_ID=" ~ load_id|string
      ) %}
      {% continue %}
    {% endif %}

    {# ---------------------------------------------------------------------
       STEP 3b: Build Insert Statement
       - Inserts all source columns into target.
       - Appends audit metadata (timestamps, user, source, load id).
       --------------------------------------------------------------------- #}
    {% set insert_sql %}
      insert into {{ adapter.quote(tgt_sch) }}.{{ adapter.quote(tgt_tbl) }}
      (
        {% for c in adapter.get_columns_in_relation(src_rel) %}
          {{ adapter.quote(c.name) }}{% if not loop.last %},{% endif %}
        {% endfor %},
        CREATED_TS, UPDATED_TS, CREATED_BY, UPDATED_BY, SOURCE_NAME, LOAD_ID
      )
      select
        {% for c in adapter.get_columns_in_relation(src_rel) %}
          {{ adapter.quote(c.name) }}{% if not loop.last %},{% endif %}
        {% endfor %},
        current_timestamp,
        current_timestamp,
        current_user,
        current_user,
        '{{ src_name }}',
        {{ load_id }}
      from {{ src_rel }} 
      limit 10   -- TEMP: remove hardcoded limit after validation
    {% endset %}

    {# --- STEP 3c: Execute Insert --- #}
    {% call statement('insert_' ~ src_tbl, fetch_result=False, auto_begin=True) %}
      {{ insert_sql }}
    {% endcall %}

    {% set res = load_result('insert_' ~ src_tbl) %}
    {% do log("Insert result: " ~ res , info=True) %}

    {# ---------------------------------------------------------------------
       STEP 3d: Mark Run as SUCCESS on completion
       - Update run-control with status, row counts, and end timestamp.
       --------------------------------------------------------------------- #}
    {% if res and res["response"].code == 'SUCCESS' %}
      {% do run_query(
        "update " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
        "set STATUS='SUCCESS', ERROR_MESSAGE=NULL, COMPLETED_TS=current_timestamp, RECORDS_INSERTED=" ~ res["response"].rows_affected ~
        " where SOURCE_TABLE='" ~ src_tbl ~ "' and TARGET_TABLE='" ~ tgt_tbl ~ "' and LOAD_ID=" ~ load_id|string
      ) %}
    {% endif %}

  {% endfor %}
{% endmacro %}
