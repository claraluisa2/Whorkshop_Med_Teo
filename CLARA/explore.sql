SELECT idCliente,
       MAX(nrPontosTransacao) AS mairTransacao
FROM silver.upsell.transacoes
GROUP BY idCliente