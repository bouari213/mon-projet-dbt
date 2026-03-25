WITH source AS (
    SELECT * FROM {{ source('raw', 'commandes_usa') }}
)

SELECT
    id_commande,
    date_commande::DATE     AS date_commande,
    montant::DECIMAL(10,2)  AS montant,
    LOWER(statut)           AS statut,
    'USA'                   AS region
FROM source