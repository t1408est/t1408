{# =======================================================================
   Helper Macro: _fetch_mappings
   -----------------------------------------------------------------------
   Purpose:
     Pulls the list of source→target ingestion mappings that need to run.
   Logic:
     - FULL_RUN → fetch all active mappings (optionally filter by SOURCE_NAME).
     - RERUN    → fetch only mappings that failed in the last run.
   Returns:
     List of (source_table, target_table, source_name) rows.
   ======================================================================= #}
{% macro _fetch_mappings(schema_name, table_name, source_name, load_type, audit_table) %}
  {% if load_type == 'RERUN' %}
    {% set query %}
      select TRAN_TABLE_NAME as source_table,
             RAW_TABLE_NAME  as target_table,
             SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
        and upper(TRAN_TABLE_NAME) in (
            select upper(SOURCE_TABLE)
            from {{ schema_name }}.INGESTION_RUN_CONTROL
            where LOAD_ID = (select max(LOAD_ID) from {{ schema_name }}.INGESTION_RUN_CONTROL)
              and upper(STATUS) != 'SUCCESS'
        )
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}
  {% else %}
    {% set query %}
      select TRAN_TABLE_NAME as source_table,
             RAW_TABLE_NAME  as target_table,
             SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}
  {% endif %}

  {% set results = run_query(query) %}
  {% if execute %}{{ return(results.rows) }}{% else %}{{ return([]) }}{% endif %}
{% endmacro %}


{# =======================================================================
   Helper Macro: _get_load_id
   -----------------------------------------------------------------------
   Purpose:
     Assigns or retrieves the LOAD_ID for this run.
   Logic:
     - FULL_RUN → new load_id (max + 1).
     - RERUN    → reuse the latest load_id.
   ======================================================================= #}
{% macro _get_load_id(schema_name, load_type) %}
  {% set sql = (
      "select coalesce(" ~
      ("max(LOAD_ID)+1" if load_type == 'FULL_RUN' else "max(LOAD_ID)") ~
      ",1) from " ~ schema_name ~ ".INGESTION_RUN_CONTROL"
  ) %}
  {% set res = run_query(sql) %}
  {{ return(res.columns[0].values()[0] if execute else 1) }}
{% endmacro %}


{# =======================================================================
   Helper Macro: _insert_run_control
   -----------------------------------------------------------------------
   Purpose:
     For FULL_RUN only, insert NOT_STARTED entries for every mapping
     into the INGESTION_RUN_CONTROL table.
   ======================================================================= #}
{% macro _insert_run_control(schema_name, tbls, load_id) %}
  {% for row in tbls %}
    {% do run_query(
      "insert into " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
      "(RUN_TS, SOURCE_TABLE, TARGET_TABLE, SOURCE_NAME, LOAD_ID, STATUS, RUN_BY, MACRO_NAME) " ~
      "values (current_timestamp,'" ~ row[0] ~ "','" ~ row[1] ~ "','" ~ row[2] ~ "'," ~ load_id|string ~
      ",'NOT_STARTED', current_user,'load_with_audit_columns')"
    ) %}
  {% endfor %}
{% endmacro %}


{# =======================================================================
   Helper Macro: _mark_failed
   -----------------------------------------------------------------------
   Purpose:
     Marks the given mapping as FAILED in run-control with an error message.
   ======================================================================= #}
  {% macro _mark_failed(schema_name, src_tbl, tgt_tbl, load_id, msg, run_ts="no") %}
    {% set ts_clause = "" %}
    {% do log("run_ts: " ~ run_ts , info=True) %}

    {% if run_ts | lower == "yes" %}
      {% set ts_clause = ", RUN_TS=CURRENT_TIMESTAMP" %}
    {% endif %}

    {% do run_query(
      "update " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
      "set STATUS='FAILED', COMPLETED_TS=current_timestamp, ERROR_MESSAGE='" ~ msg ~ "'" ~ ts_clause ~ " " ~
      "where SOURCE_TABLE='" ~ src_tbl ~ "' and TARGET_TABLE='" ~ tgt_tbl ~ "' and LOAD_ID=" ~ load_id|string
    ) %}
  {% endmacro %}



{# =======================================================================
   Helper Macro: _mark_success
   -----------------------------------------------------------------------
   Purpose:
     Marks the given mapping as SUCCESS and records number of rows inserted.
   ======================================================================= #}
{% macro _mark_success(schema_name, src_tbl, tgt_tbl, load_id, count) %}
  {% do run_query(
    "update " ~ schema_name ~ ".INGESTION_RUN_CONTROL " ~
    "set STATUS='SUCCESS', ERROR_MESSAGE=NULL, COMPLETED_TS=current_timestamp, RECORDS_INSERTED=" ~ count|string ~
    " where SOURCE_TABLE='" ~ src_tbl ~ "' and TARGET_TABLE='" ~ tgt_tbl ~ "' and LOAD_ID=" ~ load_id|string
  ) %}
{% endmacro %}


{# =======================================================================
   Helper Macro: _do_insert_with_audit
   -----------------------------------------------------------------------
   Purpose:
     Builds and executes the INSERT INTO staging target with:
       - all source columns
       - audit metadata (timestamps, user, source_name, load_id)
     If successful, marks run-control entry as SUCCESS.
   Notes:
     A LIMIT 10 is included temporarily for validation.
   ======================================================================= #}
{% macro _do_insert_with_audit(src_rel, tgt_rel, src_name, tgt_tbl, load_id, tgt_sch, schema_name, src_tbl) %}
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
      current_timestamp, current_timestamp, current_user, current_user,
      '{{ src_name }}', {{ load_id }}
    from {{ src_rel }}
    limit 10  -- TEMP: remove after validation
  {% endset %}

  {% call statement('insert_' ~ src_tbl, fetch_result=True) %}{{ insert_sql }}{% endcall %}
  {% set res = load_result('insert_' ~ src_tbl) %}
  {% if res and res["response"].code == 'SUCCESS' %}
    {{ _mark_success(schema_name, src_tbl, tgt_tbl, load_id, res["response"].rows_affected) }}
  {% endif %}
{% endmacro %}