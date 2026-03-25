WITH source AS (
    SELECT * FROM {{ source('raw', 'logs_connexion') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'date_connexion',
            'utilisateur',
            'action',
            'duree_secondes'
        ]) }}               AS sk_log,
        date_connexion,
        utilisateur,
        action,
        duree_secondes
    FROM source
)

SELECT * FROM final