SELECT '{data}' AS dtRef,
      idCliente,
       sum(nrPontosTransacao) AS nrSomaPontos, -- Total de Pontos
       sum(CASE WHEN nrPontosTransacao > 0 THEN nrPontosTransacao ELSE 0 END) AS nrSomaPontosPos, -- Pontos ganhos
       sum(CASE WHEN nrPontosTransacao < 0 THEN nrPontosTransacao ELSE 0 END) AS nrSomaPontosNeg, -- Pontos gastos
       sum(nrPontosTransacao) / count(distinct idTransacao) AS nrTicketMedio, -- Media de Pontos
       
       coalesce(sum(CASE WHEN nrPontosTransacao > 0 THEN nrPontosTransacao ELSE 0 END) / count( distinct CASE WHEN nrPontosTransacao > 0 THEN idTransacao END),0) AS nrTicketMedioPos, -- Media de Pontos Ganhos
       
       coalesce(sum(CASE WHEN nrPontosTransacao < 0 THEN nrPontosTransacao ELSE 0 END) / count( distinct CASE WHEN nrPontosTransacao < 0 THEN idTransacao END), 0) AS nrTicketMedioNeg, -- Media de Pontos Gasto

       sum(nrPontosTransacao) / count(distinct date(dtTransacao)) AS nrPontosDia 

FROM silver.upsell.transacoes


WHERE dtTransacao < '{data}'                   -- DATA DA SAFRA
AND dtTransacao >= '{data}' - INTERVAL 28 DAY  -- JANELA DE OBSERVACAO

GROUP BY ALL