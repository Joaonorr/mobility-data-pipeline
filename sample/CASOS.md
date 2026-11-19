# Casos da amostra sintética

`yellow_sample_input.csv` tem 23 registros (linhas de dados, numeradas de L01 a L23).
Os valores são **inventados** para cobrir casos de regra; não são copiados de corridas reais.
O esquema é o do Parquet Yellow Taxi de 2025-01 (20 colunas, mesmos tipos).

Os resultados esperados ficam em `sample/expected/`, escritos a partir desta tabela e **não** a partir do código.

## Registros válidos (16)

| Linha | Caso | Por que deve ser mantido |
|---|---|---|
| L01 | Corrida comum às 08:05 na zona 161 | Caso base da agregação |
| L02 | Segunda corrida na mesma hora e zona | Soma de viagens |
| L03 | Embarque às 08:59:59 | Fronteira: hora 8, não 9 |
| L04 | Embarque às 09:00:00 | Fronteira: hora 9 |
| L05 | Embarque no JFK (zona 132) às 00:00:00, 2 passageiros | Hora 0; zona aeroportuária |
| L06 | Embarque às 23:59:59 com desembarque no dia seguinte | Hora 23; data pela hora de embarque |
| L07 | Cópia exata de L01 | Duplicata sem ID não é removida (ver `docs/REGRAS_QUALIDADE.md`) |
| L08 | Pagamento `payment_type = 0` (Flex Fare), `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` e `Airport_fee` nulos | Padrão observado no Parquet real; código 0 documentado |
| L09 | `RatecodeID = 99` (null/unknown) | Código documentado pela TLC |
| L10 | `passenger_count = 0`, distância 0 e tarifa 0 | Valor incomum, mas não impossível; só contado como observação |
| L11 | Tarifa negativa (`fare_amount = -5.00`), `payment_type = 4` (disputa) | Estornos e ajustes existem na fonte |
| L12 | Embarque e desembarque no mesmo instante | Duração zero é possível |
| L13 | Zona 265 ("Outside of NYC") | Zona oficial da tabela de referência |
| L14 | Zona 264 ("N/A", borough "Unknown") | Zona oficial da tabela de referência |
| L15 | `tpep_dropoff_datetime` nulo | Desembarque não é usado para a demanda |
| L16 | Aeroporto (zona 132) com `Airport_fee` | Tarifa aeroportuária comum |

## Registros rejeitados (7)

| Linha | Motivo esperado | Caso |
|---|---|---|
| L17 | `missing_pickup_datetime` | Embarque nulo |
| L18 | `missing_pickup_location` | `PULocationID` nulo |
| L19 | `unknown_pickup_zone` | Zona 999 não existe na tabela oficial |
| L20 | `dropoff_before_pickup` | Desembarque anterior ao embarque |
| L21 | `missing_pickup_datetime` e `unknown_pickup_zone` | Dois motivos no mesmo registro |
| L22 | `missing_pickup_datetime` e `missing_pickup_location` | Dois motivos no mesmo registro |
| L23 | `unknown_pickup_zone` | Zona 0 (fora do intervalo 1–265) |

Total esperado: 16 válidos + 7 rejeitados = 23 (sem perda nem duplicação de registros).
Contagem de motivos: `missing_pickup_datetime` 3, `missing_pickup_location` 2, `unknown_pickup_zone` 3, `dropoff_before_pickup` 1 (total 9, pois L21 e L22 têm dois motivos).
