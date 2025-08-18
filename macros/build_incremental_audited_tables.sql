{% macro build_incremental_audited_tables() %}
  {% set src_db   = var('source_database') %}
  {% set src_sch  = var('source_schema') %}
  {% set tgt_sch  = var('target_schema', target.schema) %}
  {% set tbls     = var('audit_table_configs', []) %}

  {% if tbls | length == 0 %}
    {% do exceptions.raise_compiler_error("No tables provided in var('audit_table_configs').") %}
  {% endif %}

  {% for t in tbls %}
    {% set src_tbl = t['source_table'] %}
    {% set pk      = t['pk'] %}
    {% set tgt_tbl = src_tbl %}

    {% set src_rel = adapter.get_relation(database=src_db, schema=src_sch, identifier=src_tbl) %}
    {% if src_rel is none %}
      {% do log("SKIP: " ~ src_db ~ "." ~ src_sch ~ "." ~ src_tbl ~ " (not found)", info=True) %}
      {% continue %}
    {% endif %}

    {% set sql %}
    merge into {{ adapter.quote(tgt_sch) }}.{{ adapter.quote(tgt_tbl) }} as tgt
    using (
    select *, current_timestamp() as _dbt_update_ts
    from {{ src_rel }}
    ) as src
    on src.{{ pk }} = tgt.{{ pk }}
    when matched and (
    {% for c in adapter.get_columns_in_relation(src_rel) if c.name | lower not in ['created_at','updated_at'] %}
        (tgt.{{ adapter.quote(c.name) }} is distinct from src.{{ adapter.quote(c.name) }})
        {% if not loop.last %} or {% endif %}
    {% endfor %}
    ) then update set
    {% for c in adapter.get_columns_in_relation(src_rel) if c.name | lower not in ['created_at','updated_at'] %}
        {{ adapter.quote(c.name) }} = src.{{ adapter.quote(c.name) }},
    {% endfor %}
    updated_at = src._dbt_update_ts
    when not matched then insert (
    {% for c in adapter.get_columns_in_relation(src_rel) if c.name | lower not in ['created_at','updated_at'] %}
        {{ adapter.quote(c.name) }},
    {% endfor %}
    created_at, updated_at
    )
    values (
    {% for c in adapter.get_columns_in_relation(src_rel) if c.name | lower not in ['created_at','updated_at'] %}
        src.{{ adapter.quote(c.name) }},
    {% endfor %}
    src._dbt_update_ts, src._dbt_update_ts
    )
    {% endset %}

    {% do log("MERGE into " ~ tgt_sch ~ "." ~ tgt_tbl ~ " using PK " ~ pk, info=True) %}
    {% call statement('merge_' ~ src_tbl, fetch_result=False) %}
      {{ sql }}
    {% endcall %}
  {% endfor %}
{% endmacro %}
