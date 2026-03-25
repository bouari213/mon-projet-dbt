WITH source AS (
    SELECT * FROM {{ source('raw', 'commandes') }}
)

SELECT
    id_commande,
    id_client,
    date_commande::DATE                 AS date_commande,
    montant::DECIMAL(10,2)              AS montant,
    LOWER(statut)                       AS statut
FROM source