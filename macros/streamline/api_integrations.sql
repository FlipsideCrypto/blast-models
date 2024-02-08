{% macro create_aws_base_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}
    {# {% if target.name == "prod" %}
    {% set sql %}
    CREATE api integration IF NOT EXISTS aws_blast_api api_provider = aws_api_gateway api_aws_role_arn = 'insert-prod-arn-here' api_allowed_prefixes = (
        'insert-prod-url-here'
    ) enabled = TRUE;
{% endset %}
    {% do run_query(sql) %}
    #}
    {% if target.name == "dev" %}
        --replace if with elif after prod is deployed
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_blast_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/blast-api-dev-rolesnowflakeudfsAF733095-Wtkj0DGJ7lOQ' api_allowed_prefixes = (
            'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
