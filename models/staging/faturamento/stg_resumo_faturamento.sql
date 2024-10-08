with stg_resumo_faturamento as (
	select
    ROW_NUMBER() OVER (ORDER BY 'DTSAIDA' ASC) AS id,
	("_airbyte_data" ->> 'CODFILIAL')::int AS cod_filial,
    ("_airbyte_data" ->> 'NUMTRANSVENDA')::numeric AS num_trans_venda,
    ("_airbyte_data" ->> 'DTSAIDA')::date AS dt_saida,
    ("_airbyte_data" ->> 'ROTA')::text AS rota,
    ("_airbyte_data" ->> 'CODUSUR')::int AS rca,
    ("_airbyte_data" ->> 'CODCLI')::int AS codcli,
    ("_airbyte_data" ->> 'CLIENTE')::text AS cliente,
    ("_airbyte_data" ->> 'CODPROD')::int AS cod_prod,
    ("_airbyte_data" ->> 'DESCRICAO')::text AS descricao,
    ("_airbyte_data" ->> 'QTVENDA')::numeric AS qt_venda,
    ("_airbyte_data" ->> 'EMBALAGEM')::text AS embalagem,
    ("_airbyte_data" ->> 'PUNIT')::float AS punit,
    ("_airbyte_data" ->> 'VLTABELA')::float AS vl_tabela,
    ("_airbyte_data" ->> 'VLDESCONTO')::float AS vl_desconto,
    ("_airbyte_data" ->> 'VLATEND')::float AS vl_atend,
    ("_airbyte_data" ->> 'VLVENDA')::float AS vl_venda,
    ("_airbyte_data" ->> 'COMISSAO')::float AS comissao,
    ("_airbyte_data" ->> 'NUMNOTA')::int AS num_nota,
    ("_airbyte_data" ->> 'PESOLIQ')::float AS peso_liq,
    ("_airbyte_data" ->> 'PESOBRUTO')::float AS peso_bruto,
    ("_airbyte_data" ->> 'QTENT')::numeric AS qt_ent,
    ("_airbyte_data" ->> 'SERIE')::text AS serie,
    ("_airbyte_data" ->> 'UFMOV')::text AS uf_mov,
    ("_airbyte_data" ->> 'VLIPI')::numeric AS vl_ipi,    
    ("_airbyte_data" ->> 'CODCOB')::text AS cod_cob,
    ("_airbyte_data" ->> 'CAIXA')::numeric AS caixa,
    ("_airbyte_data" ->> 'CODSEC')::text AS cod_sec,
    ("_airbyte_data" ->> 'NUMPED')::numeric AS num_ped,
    ("_airbyte_data" ->> 'QTCONT')::numeric AS qt_cont,
    ("_airbyte_data" ->> 'BONIFIC')::text AS bonific,
    ("_airbyte_data" ->> 'CODEPTO')::int AS cod_depto,
    ("_airbyte_data" ->> 'CODOPER')::text AS cod_oper,    
    ("_airbyte_data" ->> 'VLFRETE')::numeric AS vl_frete,
    ("_airbyte_data" ->> 'CODPLPAG')::numeric AS cod_pl_pag,
    ("_airbyte_data" ->> 'CUSTOFIN')::numeric AS custo_fin,
    ("_airbyte_data" ->> 'CUSTOREAL')::numeric AS custo_real,    
    ("_airbyte_data" ->> 'CODPRACA')::numeric AS cod_praca,
    ("_airbyte_data" ->> 'DTCANCEL')::date AS dt_cancel,
    ("_airbyte_data" ->> 'MUNICENT')::text AS muni_cent,
    ("_airbyte_data" ->> 'NUMITENS')::numeric AS num_itens,
    ("_airbyte_data" ->> 'VLOUTROS')::numeric AS vl_outras,
    ("_airbyte_data" ->> 'CODFORNEC')::numeric AS cod_fornec,
    ("_airbyte_data" ->> 'CONDVENDA')::text AS cond_venda,
    ("_airbyte_data" ->> 'NUMREGIAO')::numeric AS num_regiao,
    ("_airbyte_data" ->> 'ORIGEMPED')::text AS orig_emp_ped,
    ("_airbyte_data" ->> 'PUNITCONT')::numeric AS punit_cont,
    ("_airbyte_data" ->> 'QTBONIFIC')::numeric AS qt_bonific,
    ("_airbyte_data" ->> 'QTCLIENTE')::text AS qt_cliente,
    ("_airbyte_data" ->> 'VLBONIFIC')::numeric AS vl_bonific,
    ("_airbyte_data" ->> 'VLESTDISP')::numeric AS vl_est_disp,
    ("_airbyte_data" ->> 'VLREPASSE')::numeric AS vl_repasse,
    ("_airbyte_data" ->> 'CODEPTOMOV')::numeric AS cod_depto_mov,
    ("_airbyte_data" ->> 'CODPRODMOV')::numeric AS cod_prod_mov,
    ("_airbyte_data" ->> 'CODUSURMOV')::numeric AS cod_usur_mov,    
    ("_airbyte_data" ->> 'PRAZOMEDIO')::text AS prazo_medio,
    ("_airbyte_data" ->> 'QTDEVOLCLI')::numeric AS qt_devol_cli,    
    ("_airbyte_data" ->> 'VLCUSTOFIN')::numeric AS vl_custo_fin,
    ("_airbyte_data" ->> 'VLCUSTOREP')::numeric AS vl_custo_rep,
    ("_airbyte_data" ->> 'VLDEVOLCLI')::numeric AS vl_devol_cli,
    ("_airbyte_data" ->> 'CODEMITENTE')::numeric AS cod_emitente,
    ("_airbyte_data" ->> 'CODFILIALNF')::text AS cod_filial_nf,
    ("_airbyte_data" ->> 'CODPLPAGMOV')::numeric AS cod_pl_pag_mov,
    ("_airbyte_data" ->> 'CODPRACAMOV')::numeric AS cod_praca_mov,
    ("_airbyte_data" ->> 'CUSTOFINEST')::numeric AS custo_fin_est,
    ("_airbyte_data" ->> 'FORNECPRINC')::text AS fornec_princ,
    ("_airbyte_data" ->> 'NUMORIGINAL')::text AS num_original,
    ("_airbyte_data" ->> 'PERCDESCFIN')::numeric AS perc_desc_fin,
    ("_airbyte_data" ->> 'VLCUSTOCONT')::numeric AS vl_custo_cont,
    ("_airbyte_data" ->> 'VLCUSTOFINB')::numeric AS vl_custo_finb,
    ("_airbyte_data" ->> 'VLCUSTOREAL')::numeric AS vl_custo_real,
    ("_airbyte_data" ->> 'CODCOMPRADOR')::numeric AS cod_comprador,
    ("_airbyte_data" ->> 'CODFORNECMOV')::numeric AS cod_fornec_mov,
    ("_airbyte_data" ->> 'CODSUPERVMOV')::numeric AS cod_superv_mov,
    ("_airbyte_data" ->> 'NUMREGIAOMOV')::numeric AS num_regiao_mov,
    ("_airbyte_data" ->> 'PESOBRUTOMOV')::numeric AS peso_bruto_mov,
    ("_airbyte_data" ->> 'VLIPIBONIFIC')::numeric AS vl_ipi_bonific,
    ("_airbyte_data" ->> 'VLOUTRASDESP')::numeric AS vl_outras_desp,
    ("_airbyte_data" ->> 'VLSUBTOTITEM')::numeric AS vl_subtot_item,
    ("_airbyte_data" ->> 'CODSUPERVISOR')::numeric AS cod_supervisor,
    ("_airbyte_data" ->> 'CODFORNECPRINC')::numeric AS cod_fornec_princ
    
from dbapi.airbyte_internal.api_raw__stream_view_vendas_resumo_faturamento
)
select * from stg_resumo_faturamento