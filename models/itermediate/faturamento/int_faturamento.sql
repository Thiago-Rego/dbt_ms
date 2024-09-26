with int_faturamento as (
    select
        cod_filial,  
        date_trunc('day', dt_saida) AS data_saida,
        rca, 
        codcli,  
        cliente, 
        SUM(vl_tabela) AS vl_tabela,
        SUM(vl_desconto) AS vl_desconto,
        SUM(vl_atend)  AS vl_atendido,
        num_ped,
        SUM(vl_devol_cli) AS vl_devol_cli, 
        cod_supervisor
    from
        {{ref("stg_resumo_faturamento")}}
    group BY
        cod_filial,
        data_saida,
        rca,
        codcli,
        cliente,
        num_ped,
        cod_supervisor
)
SELECT
    ROW_NUMBER() OVER (ORDER BY data_saida) AS id,
    *
 FROM int_faturamento