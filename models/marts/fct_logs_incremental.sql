{{ config(
    materialized='incremental',
    unique_key='sk_log'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_logs_connexion') }}
),

final AS (
    SELECT
        sk_log,
        date_connexion,
        utilisateur,
        action,
        duree_secondes
    FROM source

    {% if is_incremental() %}
        WHERE sk_log NOT IN (SELECT sk_log FROM {{ this }})
    {% endif %}
)

SELECT * FROM final