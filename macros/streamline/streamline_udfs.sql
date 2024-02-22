{% macro create_udf_rest_api() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_rest_api(
        json OBJECT
    ) returns ARRAY api_integration = 
    {% if target.name == "prod" %}
        aws_blast_api AS 'insert-prod-url-here'
    {% else %}
        aws_blast_api_dev AS 'https://05340o05al.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}