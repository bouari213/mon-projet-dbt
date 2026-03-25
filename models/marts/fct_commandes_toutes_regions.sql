WITH commandes_europe AS (
    SELECT * FROM {{ ref('stg_commandes_europe') }}
),

commandes_usa AS (
    SELECT * FROM {{ ref('stg_commandes_usa') }}
),

toutes_commandes AS (
    SELECT * FROM commandes_europe
    UNION ALL
    SELECT * FROM commandes_usa
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_commande', 'region']) }} AS sk_commande,
        id_commande,
        region,
        date_commande,
        montant,
        statut
    FROM toutes_commandes
)

SELECT * FROM final