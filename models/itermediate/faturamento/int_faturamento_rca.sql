with int_faturamento_rca as (
    select
        rca,
        date_trunc('month', dt_saida) AS mes,
        SUM(vl_venda) AS total_venda
    from
        {{ref("stg_resumo_faturamento")}}
    group by
        rca,
        mes
ORDER BY mes
)
SELECT
    ROW_NUMBER() OVER (ORDER BY mes) AS id, -- Adiciona uma coluna `id` com valores únicos
    rca,
    mes,
    total_venda
FROM
    int_faturamento_rca