{% macro generate_surrogate_key(column_list) %}
    md5(
        {% for col in column_list %}
            coalesce(cast({{ col }} as varchar), '_null_')
            {%- if not loop.last %} || '||' || {% endif %}
        {% endfor %}
    )
{% endmacro %}