/*
    Custom schema name macro.
    When a custom schema is defined in dbt_project.yml, use it directly
    (without the default target schema prefix).
    This gives us clean schema names: raw, staging, intermediate, analytics
    instead of public_raw, public_staging, etc.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
