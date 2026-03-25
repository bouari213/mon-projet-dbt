WITH source AS (
    SELECT * FROM {{ source('raw', 'logs_connexion_v2') }}
)

SELECT
    id_log,
    date_connexion::TIMESTAMP   AS date_connexion,
    utilisateur,
    action,
    duree_secondes,
    updated_at::TIMESTAMP       AS updated_at
FROM source