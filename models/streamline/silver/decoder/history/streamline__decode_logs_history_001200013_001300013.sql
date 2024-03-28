{{ config (
    materialized = "view",
    post_hook = [if_data_call_function(
        func = "{{model.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{model.alias}}', 'external_table', 'DECODED_LOGS', 'sql_limit', {{var('sql_limit','7500000')}}, 'producer_batch_size', {{var('producer_batch_size','20000000')}}, 'worker_batch_size', {{var('worker_batch_size','10000000')}}))",
        target = "{{model.schema}}.{{model.alias}}"
    ),
    if_data_call_wait()],
    tags = ['streamline_decoded_logs_history']
) }}

{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}
{{ fsc_utils.decode_logs_history(
    start,
    stop
) }}
