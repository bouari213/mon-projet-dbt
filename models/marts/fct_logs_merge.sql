{{ config(
    materialized='incremental',
    unique_key='id_log',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "updated_at",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_logs_connexion_v2') }}
),

final AS (
    SELECT
        id_log,
        date_connexion,
        utilisateur,
        action,
        duree_secondes,
        updated_at
    FROM source

    {% if is_incremental() %}
        WHERE updated_at >= DATEADD(day, -3, CURRENT_TIMESTAMP())
    {% endif %}
)

SELECT * FROM final