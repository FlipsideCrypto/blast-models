{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;

        {{ create_udtf_get_base_table(
            schema = "streamline"
        ) }}
        {{ create_udf_rest_api() }}
        {{ create_aws_blast_api() }}
        {{ create_udf_bulk_decode_logs() }}

        {% endset %}
        {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}
