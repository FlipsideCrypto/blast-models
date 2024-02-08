{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration =
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/get_chainhead'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = 
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/udf_bulk_json_rpc'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_get_traces() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_traces(
        json variant
    ) returns text api_integration = 
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/udf_bulk_get_traces'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_get_traces'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_string() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode(
        abi ARRAY,
        DATA STRING
    ) returns ARRAY api_integration = 
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/decode_function'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/decode_function'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_object() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode(
        abi ARRAY,
        DATA OBJECT
    ) returns ARRAY api_integration = 
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/decode_log'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/decode_log'
    {%- endif %};
{% endmacro %}


{% macro create_udf_bulk_decode_logs() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_logs(
        json OBJECT
    ) returns ARRAY api_integration = 
    {# {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here/bulk_decode_logs'
    {% else %} #}
    {% if target.name == "dev" %} --remove if and replace with else after prod is deployed
        aws_blast_api_dev AS'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_logs'
    {%- endif %};
{% endmacro %}
