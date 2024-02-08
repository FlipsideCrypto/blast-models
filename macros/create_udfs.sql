{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}