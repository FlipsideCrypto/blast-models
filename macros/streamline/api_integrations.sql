{% macro create_aws_blast_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}
    {% if target.name == "prod" %}
    {% set sql %}
    CREATE api integration IF NOT EXISTS aws_blast_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/blast-api-prod-rolesnowflakeudfsAF733095-DY5ob2RfHqOI' api_allowed_prefixes = (
        'https://42gzudc5si.execute-api.us-east-1.amazonaws.com/prod/'
    ) enabled = TRUE;
{% endset %}
    {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_blast_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/blast-api-stg-rolesnowflakeudfsAF733095-p8GqKotrBniw' api_allowed_prefixes = (
            'https://y9d0tuavh6.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
