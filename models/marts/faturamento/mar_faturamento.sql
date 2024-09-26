{{ config(
    materialized='incremental',
    unique_key='id'
) }}
with mart_faturamento as (
    select
       *
    from
        {{ref("int_faturamento")}}
 -- Filtro global aplicado a todos os registros, independentemente de ser incremental ou não
    WHERE data_saida >= '2000-01-01' -- Aqui você pode definir um limite inicial fixo para filtrar dados
    
    {% if is_incremental() %}
        -- Condição adicional para processamento incremental
        AND data_saida > (
            SELECT COALESCE(MAX(data_saida), '2000-01-01'::date) 
            FROM {{ this }}
            )
    {% endif %}
)
SELECT * FROM mart_faturamento