{% macro hash_string(input_string) -%}
    to_hex(sha512(cast({{ input_string }} as varbinary)))
{%- endmacro %}
