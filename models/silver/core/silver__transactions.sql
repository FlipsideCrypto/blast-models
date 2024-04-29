-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['non_realtime'],
    full_refresh = false
) }}

WITH base AS (

    SELECT
        IFNULL(
            VALUE :BLOCK_NUMBER :: INT,
            metadata :request :"data" :id :: INT
        ) AS block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
base_tx AS (
    SELECT
        block_number,
        DATA :blockHash :: STRING AS block_hash,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :blockNumber :: STRING
            )
        ) AS blockNumber,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA: depositReceiptVersion :: STRING
            )
        ) AS deposit_receipt_version,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :chainId :: STRING
            )
        ) AS chain_id,
        DATA :from :: STRING AS from_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :gas :: STRING
            )
        ) AS gas,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    DATA :gasPrice :: STRING
                )
            ) / pow(
                10,
                9
            ),
            0
        ) AS gas_price,
        DATA :hash :: STRING AS tx_hash,
        DATA :input :: STRING AS input_data,
        SUBSTR(
            input_data,
            1,
            10
        ) AS origin_function_signature,
        utils.udf_hex_to_int(
            DATA :mint :: STRING
        ) AS mint_precise_raw,
        utils.udf_decimal_adjust(
            mint_precise_raw,
            18
        ) AS mint_precise,
        mint_precise :: FLOAT AS mint,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :maxFeePerGas :: STRING
            )
        ) / pow(
            10,
            9
        ) AS max_fee_per_gas,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :maxPriorityFeePerGas :: STRING
            )
        ) / pow(
            10,
            9
        ) AS max_priority_fee_per_gas,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :nonce :: STRING
            )
        ) AS nonce,
        DATA :r :: STRING AS r,
        DATA :s :: STRING AS s,
        DATA :sourceHash :: STRING AS source_hash,
        DATA :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                DATA :transactionIndex :: STRING
            )
        ) AS POSITION,
        DATA :type :: STRING AS TYPE,
        DATA :v :: STRING AS v,
        utils.udf_hex_to_int(
            DATA :value :: STRING
        ) AS value_precise_raw,
        utils.udf_decimal_adjust(
            value_precise_raw,
            18
        ) AS value_precise,
        value_precise :: FLOAT AS VALUE,
        DATA :yParity :: STRING AS y_parity,
        _INSERTED_TIMESTAMP,
        DATA
    FROM
        base
),
new_records AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.mint_precise_raw,
        t.mint_precise,
        t.mint,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.source_hash,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        t.y_parity,
        block_timestamp,
        CASE
            WHEN block_timestamp IS NULL
            OR tx_status IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        r.gas_used,
        r.l1_fee,
        r.l1_fee_scalar,
        r.l1_gas_used,
        r.l1_gas_price,
        utils.udf_decimal_adjust(
            (
                r.gas_used * utils.udf_hex_to_int(
                    t.data :gasPrice :: STRING
                ) :: bigint
            ) + FLOOR(
                r.l1_gas_price * r.l1_gas_used * r.l1_fee_scalar
            ) + IFF(
                r.l1_fee_scalar = 0,
                r.l1_fee,
                0
            ),
            18
        ) AS tx_fee_precise,
        COALESCE(
            tx_fee_precise :: FLOAT,
            0
        ) AS tx_fee,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        r.type AS tx_type,
        t._inserted_timestamp,
        t.data,
        r.deposit_nonce,
        r.deposit_receipt_version
    FROM
        base_tx t
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        b
        ON t.block_number = b.block_number
        LEFT OUTER JOIN {{ ref('silver__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash

{% if is_incremental() %}
AND r._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.mint_precise_raw,
        t.mint_precise,
        t.mint,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.source_hash,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        t.y_parity,
        b.block_timestamp,
        FALSE AS is_pending,
        r.gas_used,
        r.tx_success,
        r.tx_status,
        r.cumulative_gas_used,
        r.effective_gas_price,
        r.l1_fee,
        r.l1_fee_scalar,
        r.l1_gas_used,
        r.l1_gas_price,
        utils.udf_decimal_adjust(
            (
                r.gas_used * utils.udf_hex_to_int(
                    t.data :gasPrice :: STRING
                ) :: bigint
            ) + FLOOR(
                r.l1_gas_price * r.l1_gas_used * r.l1_fee_scalar
            ) + IFF(
                r.l1_fee_scalar = 0,
                r.l1_fee,
                0
            ),
            18
        ) AS tx_fee_precise_heal,
        COALESCE(
            tx_fee_precise_heal :: FLOAT,
            0
        ) AS tx_fee,
        r.type AS tx_type,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp,
        t.data,
        r.deposit_nonce,
        r.deposit_receipt_version
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__blocks') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('silver__receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_hash,
        chain_id,
        from_address,
        gas,
        gas_price,
        tx_hash,
        input_data,
        origin_function_signature,
        mint_precise_raw,
        mint_precise,
        mint,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        nonce,
        r,
        s,
        source_hash,
        to_address,
        POSITION,
        TYPE,
        v,
        y_parity,
        VALUE,
        value_precise_raw,
        value_precise,
        block_timestamp,
        is_pending,
        gas_used,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        l1_fee,
        l1_fee_scalar,
        l1_gas_used,
        l1_gas_price,
        tx_fee,
        tx_fee_precise,
        tx_type,
        _inserted_timestamp,
        DATA,
        deposit_nonce,
        deposit_receipt_version
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price,
    tx_hash,
    input_data,
    origin_function_signature,
    mint_precise_raw,
    mint_precise,
    mint,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    nonce,
    r,
    s,
    source_hash,
    to_address,
    POSITION,
    TYPE,
    v,
    y_parity,
    VALUE,
    value_precise_raw,
    value_precise,
    block_timestamp,
    is_pending,
    gas_used,
    tx_success,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    l1_fee,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    tx_fee,
    tx_fee_precise_heal AS tx_fee_precise,
    tx_type,
    _inserted_timestamp,
    DATA,
    deposit_nonce,
    deposit_receipt_version
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price,
    tx_hash,
    input_data,
    origin_function_signature,
    mint_precise_raw,
    mint_precise,
    mint,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    nonce,
    r,
    s,
    source_hash,
    to_address,
    POSITION,
    TYPE,
    v,
    y_parity,
    VALUE,
    value_precise_raw,
    value_precise,
    block_timestamp,
    is_pending,
    gas_used,
    tx_success,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    utils.udf_decimal_adjust(
        l1_fee,
        18
    ) AS l1_fee_precise,
    l1_fee_precise :: FLOAT AS l1_fee,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price / pow(
        10,
        9
    ) AS l1_gas_price,
    tx_fee,
    tx_fee_precise,
    tx_type,
    _inserted_timestamp,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    deposit_nonce,
    deposit_receipt_version
FROM
    FINAL
WHERE
    block_hash IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
