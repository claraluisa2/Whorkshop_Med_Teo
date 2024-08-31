WITH tb_transacoes AS (

    SELECT *,
           date(dtTransacao) AS diaTransacao

    FROM silver.upsell.transacoes

    WHERE dtTransacao < '{data}'
    AND dtTransacao >= '{data}' - INTERVAL 28 DAY

),

tb_agrupada AS (

    SELECT 
          idCliente,

          COUNT(DISTINCT dtTransacao) AS nrQtdeTransacoes,
          COUNT(DISTINCT date(dtTransacao) ) AS nrQtdeDias, -- Frequência Dias 
          min(datediff('{data}' , date(dtTransacao))) AS nrRecenciaDias,
          count(CASE WHEN dayofweek(dtTransacao) = 2 then idTransacao end) AS nrQtdeTransacaoDay2,
          count(CASE WHEN dayofweek(dtTransacao) = 3 then idTransacao end) AS nrQtdeTransacaoDay3,
          count(CASE WHEN dayofweek(dtTransacao) = 4 then idTransacao end) AS nrQtdeTransacaoDay4,
          count(CASE WHEN dayofweek(dtTransacao) = 5 then idTransacao end) AS nrQtdeTransacaoDay5,
          count(CASE WHEN dayofweek(dtTransacao) = 6 then idTransacao end) AS nrQtdeTransacaoDay6,

          count(DISTINCT CASE WHEN dayofweek(dtTransacao) = 2 then date(dtTransacao) end) AS nrQtdeDay2,
          count(DISTINCT CASE WHEN dayofweek(dtTransacao) = 3 then date(dtTransacao) end) AS nrQtdeDay3,
          count(DISTINCT CASE WHEN dayofweek(dtTransacao) = 4 then date(dtTransacao) end) AS nrQtdeDay4,
          count(DISTINCT CASE WHEN dayofweek(dtTransacao) = 5 then date(dtTransacao) end) AS nrQtdeDay5,
          count(DISTINCT CASE WHEN dayofweek(dtTransacao) = 6 then date(dtTransacao) end) AS nrQtdeDay6,

          MAX(nrPontosTransacao) AS maiorTransacao

    FROM tb_transacoes

    GROUP BY ALL

),

tb_cliente_dia AS (

    SELECT DISTINCT
          idCliente,
          diaTransacao

    FROM tb_transacoes
    ORDER BY idCliente, diaTransacao

),

tb_cliente_lag AS (

    SELECT *,
          lag(diaTransacao) OVER (PARTITION BY idCliente ORDER BY diaTransacao) AS lagDia

    FROM tb_cliente_dia

),

tb_recorrencia AS (

    select idCliente,
          avg(datediff(diaTransacao, lagDia)) AS nrAvgRecorrencia

    from tb_cliente_lag

    GROUP BY ALL

),

tb_primeira_transacao AS (
    SELECT
        idCliente,
        MIN(dtTransacao) AS primeiraTransacao
    FROM silver.upsell.transacoes
    GROUP BY idCliente
), 

tb_email AS ( -- Se o cliente tem email vinculado
    SELECT
        idCliente,
        flEmailCliente
    FROM silver.upsell.cliente
)

SELECT 
       '{data}' AS dtRef,
       t1.*,
       t2.nrAvgRecorrencia,
       DATEDIFF('{data}', pt.primeiraTransacao) AS qtdeDiasBase, -- Quantidade de dias do cliente na base
       flEmailCliente AS flEmail
FROM tb_agrupada AS t1

LEFT JOIN tb_recorrencia AS t2 ON t1.idCliente = t2.idCliente
LEFT JOIN tb_primeira_transacao AS pt ON t1.idCliente = pt.idCliente
LEFT JOIN tb_email AS t4 ON t1.idCliente = t4.idCliente