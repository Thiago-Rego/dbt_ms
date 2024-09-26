{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH mart_faturamento_filial AS (
    SELECT
        *
    FROM
        {{ ref("int_faturamento_filial") }}
    
    -- Filtro global aplicado a todos os registros, independentemente de ser incremental ou não
    WHERE mes >= '2000-01-01' -- Aqui você pode definir um limite inicial fixo para filtrar dados
    
    {% if is_incremental() %}
        -- Condição adicional para processamento incremental
        AND mes > (
            SELECT COALESCE(MAX(mes), '2000-01-01'::date) 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    *
FROM mart_faturamento_filial