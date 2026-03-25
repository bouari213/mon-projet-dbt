WITH clients AS (
    SELECT * FROM {{ ref('stg_clients') }}
),

commandes AS (
    SELECT * FROM {{ ref('stg_commandes') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['c.id_commande', 'c.id_client']) }} AS sk_commande_client,
        c.id_commande,
        c.date_commande,
        c.montant,
        c.statut,
        cl.id_client,
        cl.nom,
        cl.prenom,
        cl.email
    FROM commandes c
    LEFT JOIN clients cl
        ON c.id_client = cl.id_client
)

SELECT * FROM final