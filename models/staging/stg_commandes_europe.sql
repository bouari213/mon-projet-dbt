WITH source AS (
    SELECT * FROM {{ source('raw', 'commandes_europe') }}
)

SELECT
    id_commande,
    date_commande::DATE     AS date_commande,
    montant::DECIMAL(10,2)  AS montant,
    LOWER(statut)           AS statut,
    'Europe'                AS region
FROM source