{% macro audit_columns(unique_key='id') %}
    {% if is_incremental() %}
        case
            when tgt.{{ unique_key }} is null then current_timestamp()
            else tgt.created_at
        end as created_at
      {% else %}
        current_timestamp() AS created_at
      {% endif %}
    , current_timestamp() AS updated_at
{% endmacro %}
