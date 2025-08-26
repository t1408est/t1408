{# =======================================================================
   Main Macro: load_with_audit_columns
   -----------------------------------------------------------------------
   Purpose:
     Orchestrates controlled ingestion of data from source tables
     into staging targets with audit metadata and logging.
   High-Level Flow:
     1. Fetch mappings to process (via _fetch_mappings).
     2. Determine load_id (via _get_load_id).
     3. Run-control setup (NOT_STARTED for FULL_RUN).
     4. For each mapping:
        - pessimistically mark FAILED
        - check target table existence
        - perform insert with audit cols
        - mark SUCCESS if completed
   Parameters:
     - table_name   : config table name
     - schema_name  : schema for config + run-control
     - source_name  : optional filter on source
     - load_type    : FULL_RUN or RERUN
   ======================================================================= #}
{% macro load_with_audit_columns_m(table_name=None, schema_name=None, source_name=None, load_type=None) %}

  {# --- Set database and schema values from DBT environment variables --- #}
  {% set src_db   = env_var('DBT_SOURCE_DATABASE') %}
  {% set src_sch  = env_var('DBT_SOURCE_SCHEMA') %}
  {% set tgt_sch  = env_var('DBT_TARGET_SCHEMA') %}

  {# --- Check load_type input and raise error if invalid --- #}
  {% if load_type not in ['FULL_RUN','RERUN'] %}
    {% do exceptions.raise_compiler_error("Invalid load_type: " ~ load_type) %}
  {% endif %}

  {# --- STEP 1: Discover mappings --- #}
  {% set tbls = _fetch_mappings(schema_name, table_name, source_name, load_type) %}
--   {% if tbls | length == 0 %}  
    {% do exceptions.raise_compiler_error("No active ingestion mappings found.") %}
  {% endif %}

  {# --- STEP 2: Generate load_id --- #}
  {% set load_id = _get_load_id(schema_name, load_type) %}

  {# --- STEP 3: Insert run-control entries for FULL_RUN --- #}
  {% if load_type == 'FULL_RUN' %}
    {{ _insert_run_control(schema_name, tbls, load_id) }}
  {% endif %}

  {# --- STEP 4: Process each mapping --- #}
  {% for row in tbls %}
    {% set src_tbl, tgt_tbl, src_name = row[0], row[1], row[2] %}
    {% set src_rel = adapter.get_relation(database=src_db, schema=src_sch, identifier=src_tbl) %}
    {% set tgt_rel = adapter.get_relation(database=src_db, schema=tgt_sch, identifier=tgt_tbl) %}

    {# pessimistic failure mark before attempting insert #}
    {{ _mark_failed(schema_name, src_tbl, tgt_tbl, load_id, "Insert started but not completed", "yes") }}

    {% if not tgt_rel %}
      {{ _mark_failed(schema_name, src_tbl, tgt_tbl, load_id, "Target table not found", "yes") }}
      {% continue %}
    {% endif %}

    {# perform insert with audit columns #}
    {{ _do_insert_with_audit(src_rel, tgt_rel, src_name, tgt_tbl, load_id, tgt_sch, schema_name, src_tbl) }}
  {% endfor %}
{% endmacro %}
