{% macro stable_key(columns) -%}
    to_hex(
        sha256(
            concat(
                {%- for column in columns -%}
                coalesce(cast({{ column }} as string), '<null>')
                {%- if not loop.last %}, '||', {% endif -%}
                {%- endfor -%}
            )
        )
    )
{%- endmacro %}

{% macro ingestion_key(source_system, source_record_id, source_updated_at) -%}
    {{ stable_key([source_system, source_record_id, source_updated_at]) }}
{%- endmacro %}
