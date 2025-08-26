
{% macro load_with_audit_columns_2(table_name=None, schema_name=None, source_name=None) %}
  {% set src_db   = env_var('DBT_SOURCE_DATABASE') %}
  {% set src_sch  = env_var('DBT_SOURCE_SCHEMA') %}
  {% set tgt_sch  = env_var('DBT_TARGET_SCHEMA') %}

  {% set query %}
    select TRAN_TABLE_NAME as source_table,
           RAW_TABLE_NAME  as target_table,
           SOURCE_NAME
    from {{ schema_name }}.{{ table_name }}
    where ISACTIVE = 'Y'
    {% if source_name %} and SOURCE_NAME = '{{ source_name }}' {% endif %}
  {% endset %}

  {% set results = run_query(query) %}
  {% if execute %}
    {% set tbls = results.rows %}
  {% else %}
    {% set tbls = [] %}
  {% endif %}

  {% if tbls | length == 0 %}
    {% do exceptions.raise_compiler_error("No tables found in ingestion config.") %}
  {% endif %}

  {% for row in tbls %}
    {% set src_tbl  = row[0] %}
    {% set tgt_tbl  = row[1] %}
    {% set src_name = row[2] %}
    {% set src_rel = adapter.get_relation(database=src_db, schema=src_sch, identifier=src_tbl) %}
    {% set tgt_rel = adapter.get_relation(database=src_db, schema=tgt_sch, identifier=tgt_tbl) %}

	{% set pl_block %}
	execute immediate $$
	declare
		v_start_ts timestamp;
		v_load_id number;
		v_msg string;
	begin
		select current_timestamp into v_start_ts;

		begin
            -- get next load id
            SET v_load_id = (SELECT COALESCE(MAX(load_id) + 1, 1) FROM {{ tgt_rel }});

			-- main insert
			insert into {{ tgt_rel }}
			(
			  {% for c in adapter.get_columns_in_relation(src_rel) %}
				{{ adapter.quote(c.name) }}{% if not loop.last %},{% endif %}
			  {% endfor %},
			  created_ts, updated_ts, created_by, updated_by, source_name, load_id
			)
			select
			  {% for c in adapter.get_columns_in_relation(src_rel) %}
				{{ adapter.quote(c.name) }}{% if not loop.last %},{% endif %}
			  {% endfor %},
			  current_timestamp, current_timestamp,
			  current_user, current_user,
			  '{{ src_name }}', :v_load_id
			from {{ src_rel }};

			-- success logging
			insert into {{ schema_name }}.INGESTION_RUN_CONTROL
			(run_ts, completed_ts, source_table, target_table, source_name,
			 load_id, status, error_message, run_by, macro_name)
			values
			(:v_start_ts, current_timestamp, '{{ src_tbl }}', '{{ tgt_tbl }}',
			 '{{ src_name }}', :v_load_id, 'SUCCESS', null, current_user,
			 'load_with_audit_columns');

		exception
			when statement_error then
				let v_msg := sqlerrm;

				insert into {{ schema_name }}.INGESTION_RUN_CONTROL
				(run_ts, completed_ts, source_table, target_table, source_name,
				 load_id, status, error_message, run_by, macro_name)
				values
				(:v_start_ts, current_timestamp, '{{ src_tbl }}', '{{ tgt_tbl }}',
				 '{{ src_name }}', :v_load_id, 'FAILED', :v_msg, current_user,
				 'load_with_audit_columns');
		end;
	end;
	$$;
	{% endset %}
    {% do run_query(pl_block) %}
  {% endfor %}
{% endmacro %}
