with int_fat_filial as (
SELECT
    cod_filial,
    date_trunc('month', dt_saida) AS mes,
    SUM(vl_venda) AS total_venda
FROM
    {{ ref("stg_resumo_faturamento") }}
GROUP BY
    cod_filial,
    mes
ORDER BY
    mes
)
select 
    ROW_NUMBER() OVER (ORDER BY mes) AS id, -- Adiciona uma coluna `id` com valores únicos
    cod_filial,
    mes,
    total_venda
 from int_fat_filial