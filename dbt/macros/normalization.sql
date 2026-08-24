{% macro normalize_email(column_name) -%}
    case
        when {{ column_name }} is null then null
        when regexp_contains(
            lower(trim(cast({{ column_name }} as string))),
            r'^[a-z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$'
        ) then lower(trim(cast({{ column_name }} as string)))
        else null
    end
{%- endmacro %}

{% macro normalize_au_phone(column_name) -%}
    case
        when regexp_contains(
            regexp_replace(cast({{ column_name }} as string), r'[^0-9]', ''),
            r'^(?:61|0)?[23478][0-9]{8}$'
        ) then concat(
            '+61',
            regexp_extract(
                regexp_replace(cast({{ column_name }} as string), r'[^0-9]', ''),
                r'(?:61|0)?([23478][0-9]{8})$'
            )
        )
        else null
    end
{%- endmacro %}

{% macro normalize_name(column_name) -%}
    nullif(
        trim(regexp_replace(lower(normalize(cast({{ column_name }} as string), NFKD)), r'[^a-z0-9]+', ' ')),
        ''
    )
{%- endmacro %}

{% macro normalize_postcode(column_name) -%}
    case
        when regexp_contains(trim(cast({{ column_name }} as string)), r'^[0-9]{4}$')
            then trim(cast({{ column_name }} as string))
        else null
    end
{%- endmacro %}
