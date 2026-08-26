{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- set dataset_env_var = 'DBT_BIGQUERY_' ~ (custom_schema_name | upper) ~ '_DATASET' -%}
        {{ env_var(dataset_env_var, target.schema ~ '_' ~ custom_schema_name) }}
    {%- endif -%}
{%- endmacro %}
