{% macro build_audited_tables() %}
  {# Vars you pass at runtime:
     source_database: RAW_DB
     source_schema: RAW_SCHEMA
     target_schema: CURATED
     audit_tables: ["orders","customers","products"]
  #}
  {% set src_db   = var('source_database') %}
  {% set src_sch  = var('source_schema') %}
  {% set tgt_sch  = var('target_schema', target.schema) %}
  {% set tbls     = var('audit_tables', []) %}

  {% if tbls | length == 0 %}
    {% do exceptions.raise_compiler_error("No tables provided in var('audit_tables').") %}
  {% endif %}

  {% for t in tbls %}
    {% set rel = adapter.get_relation(database=src_db, schema=src_sch, identifier=t) %}
    {% if rel is none %}
      {% do log("SKIP: " ~ src_db ~ "." ~ src_sch ~ "." ~ t ~ " (not found)", info=True) %}
      {% continue %}
    {% endif %}

    {% set cols = adapter.get_columns_in_relation(rel) %}
    {% set keep = [] %}
    {% for c in cols %}
      {% if c.name | lower not in ['created_at','updated_at'] %}
        {% do keep.append(adapter.quote(c.name)) %}
      {% endif %}
    {% endfor %}

    {% set sql %}
      create or replace table {{ adapter.quote(tgt_sch) }}.{{ adapter.quote(t) }} as
      select
        {{ keep | join(',\n        ') }}{% if keep | length > 0 %},{% endif %}
        current_timestamp() as created_at,
        current_timestamp() as updated_at
      from {{ rel }}
    {% endset %}

    {% do log("Building " ~ tgt_sch ~ "." ~ t, info=True) %}
    {% call statement('build_' ~ t, fetch_result=False) %}
      {{ sql }}
    {% endcall %}
  {% endfor %}
{% endmacro %}
