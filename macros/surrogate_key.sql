-- macros/surrogate_key.sql
{% macro surrogate_key(fields) %}
  md5(
    {% for f in fields %}
      md5(coalesce({{ f }}::text, '')){% if not loop.last %} || '|' || {% endif %}
    {% endfor %}
  )
{% endmacro %}

