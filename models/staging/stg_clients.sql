WITH source AS (
    SELECT * FROM {{ source('raw', 'clients') }}
)

SELECT
    id_client,
    UPPER(nom)                          AS nom,
    INITCAP(prenom)                     AS prenom,
    LOWER(email)                        AS email,
    date_inscription::DATE              AS date_inscription
FROM source