{% macro _fetch_mappings(schema_name, table_name, source_name, load_type, audit_table , RAW_TABLE_NAME, STG_TABLE_NAME ) %}
  {% if load_type == 'RERUN' %}
    {% set query %}
      select RAW_TABLE_NAME, as source_table,
             STG_TABLE_NAME  as target_table,
             SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
        and upper(RAW_TABLE_NAME) in (
            select upper(SOURCE_TABLE)
            from {{ schema_name }}. {{ audit_table }}
            where LOAD_ID = (select max(LOAD_ID) from {{ schema_name }}. {{ audit_table }} 
              and upper(STATUS) != 'SUCCESS'
        )
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}
  {% else %}
    {% set query %}
      select RAW_TABLE_NAME as source_table,
             STG_TABLE_NAME  as target_table,
             SOURCE_NAME
      from {{ schema_name }}.{{ table_name }}
      where ISACTIVE = 'Y'
      {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
    {% endset %}
  {% endif %}

  {% set results = run_query(query) %}
  {% if execute %}{{ return(results.rows) }}{% else %}{{ return([]) }}{% endif %}
{% endmacro %}

