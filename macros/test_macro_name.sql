{% macro my_macro() %}
    {% set macro_name = (adapter.dispatch).func_name %}
    {{ log("Macro name: " ~ macro_name, info=True) }}
{% endmacro %}
