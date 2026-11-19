# Regras de qualidade

## Princípio

Um registro só é rejeitado quando não serve para a pergunta do projeto: **demanda por dia, hora e zona de embarque**.
Valores incomuns são mantidos e contados. A fonte pode conter exceções reais (estornos, corridas sem distância, códigos de pagamento especiais), e apagá-las mudaria a análise sem aviso.

Não existe chave única de corrida no dicionário Yellow Taxi. Por isso **não há deduplicação**: registros com conteúdo idêntico podem ser corridas diferentes. A amostra inclui uma cópia exata (L01 e L07) para deixar essa decisão testada.

Todo registro de entrada termina em exatamente uma saída: `valid` ou `rejected`. A reconciliação do pipeline confere que `válidos + rejeitados = lidos`.

## Rejeições

Um registro rejeitado vai para `rejected`, com a lista de motivos em `reject_reasons`. Um registro pode ter mais de um motivo.

| Motivo | Regra | Justificativa | Evidência no mês real (2025-01, 3.475.226 registros) |
|---|---|---|---|
| `missing_pickup_datetime` | `tpep_pickup_datetime` é nulo | Sem embarque não há dia nem hora | 0 casos (regra defensiva para outros meses) |
| `missing_pickup_location` | `PULocationID` é nulo | Sem zona não há região de embarque | 0 casos (regra defensiva) |
| `unknown_pickup_zone` | `PULocationID` não existe em `reference/taxi_zone_lookup.csv` | Zona fora da referência oficial não pode ser agregada por região | 0 casos (todos os valores estão entre 1 e 265) |
| `dropoff_before_pickup` | `tpep_dropoff_datetime < tpep_pickup_datetime` | Intervalo impossível: ou o embarque ou o desembarque está errado, e a fonte não diz qual. A hora de embarque fica sob dúvida. | 124 casos |
| `outside_partition_month` | `tpep_pickup_datetime` cai em outro mês que não o da partição (`--month`) | Cada corrida entra em uma única partição, pelo mês do embarque. Assim nenhuma corrida é contada em dois meses. Registros de borda de arquivos mensais vizinhos aparecem como contagem consultável. | 2025-01: 22 casos (21 em 31/12/2024, 1 em 01/02/2025). 2024-12: 34 casos (26 em 30/11/2024, 3 com ano 2008/2009, 5 com data de 2025) |

Decisões ligadas às rejeições:

- `dropoff_before_pickup` é a regra mais discutível. A alternativa seria mantê-la e só contar. Escolhi rejeitar porque o embarque depende do mesmo registro. Se a análise pedir outra política, basta trocar uma linha em `transform.py` e a contagem continua visível.
- Desembarque **igual** ao embarque é válido (corrida de duração zero; caso L12 da amostra).
- Desembarque **nulo** não é rejeição, porque a demanda não usa esse campo (caso L15).
- Null em uma condição não dispara a regra. Se o embarque é nulo, a comparação com o desembarque não é avaliada, e o motivo fica só `missing_pickup_datetime`.

## Observações (mantidas e contadas, nunca removidas)

O resumo de cada execução (`run_summary.json`) mostra essas contagens. Os números do mês real são medidos sobre o arquivo de 2025-01.

| Observação | Motivo para manter | Ocorrências em 2025-01 |
|---|---|---|
| `payment_type = 0` (Flex Fare, código oficial) | Código documentado. São exatamente as mesmas linhas em que `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` e `Airport_fee` são nulos (verificado linha a linha) | 540.149 |
| `passenger_count` nulo | Mesmo padrão do Flex Fare. A fonte não define o preenchimento | 540.149 |
| `passenger_count = 0` | Dado declarado pelo motorista; o dicionário não define domínio | 24.656 |
| `trip_distance = 0` | Corridas sem deslocamento ou taxímetro sem registro. Não afeta a contagem de demanda | 90.893 |
| `fare_amount < 0` | Estornos e ajustes existem na fonte | 144.118 |
| `total_amount < 0` | Idem | 63.037 |
| `tpep_dropoff_datetime` nulo | Campo não usado pela demanda | 0 |

Valores extremos também são mantidos. Em 2025-01 há `trip_distance` máximo de 276.423,57 milhas. É um valor claramente errado, mas não afeta a contagem de demanda, e removê-lo sem uma regra de distância seria arbitrário. Uma regra de distância pode ser adicionada depois, com justificativa e evidência.

## Decisão da etapa 2: embarque fora do mês

Cada mês é uma partição (`<saida>/month=AAAA-MM/`). Um registro cujo embarque cai em outro mês é rejeitado com `outside_partition_month`. Escolhi isso em vez de aceitar a corrida no mês vizinho pelo motivo abaixo.

**Lacuna medida, não resolvida:** a corrida do arquivo de janeiro com embarque em 31/12/2024 não aparece no arquivo de dezembro.

- Dezembro tem 88.681 embarques em 31/12/2024. O join por embarque, desembarque e zona não encontrou nenhuma das 21 corridas de janeiro nesse conjunto.
- Aceitar essas 21 corridas em janeiro quebraria a análise por dia: teriam `pickup_date` de 31/12 dentro da partição de janeiro.
- Resultado: 21 corridas de 31/12/2024 ficam sem partição quando só dezembro e janeiro são processados. A corrida de 01/02/2025 (rejeitada em janeiro) entra na partição de fevereiro, quando ela for processada.
- Medição feita com `query` sobre as partições publicadas em 2026-11-19. Recalcular se a fonte mudar.

Os 3 registros de dezembro com ano de embarque 2008 ou 2009 e os 5 com data de 2025 são erros da fonte. A regra os separa como fora do mês e não os remove sem contagem.
- **Esquema pode mudar entre anos.** `cbd_congestion_fee` aparece a partir de 2025 (o próprio TLC menciona a mudança). A TLC também diz que pode padronizar o esquema Parquet no futuro. O pipeline só exige as três colunas usadas; as demais são preservadas quando existem.

## Validação de esquema (falha a execução; nada é publicado)

| Condição | Erro |
|---|---|
| Caminho inexistente, ou pasta sem `.parquet` | `InputNotFoundError` |
| Arquivo truncado ou que não é Parquet | `InputCorruptedError` |
| Coluna exigida ausente (`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`) | `IncompatibleInputError` |
| Tipo errado, ou timestamp **com fuso** (o pipeline exige `timestamp` sem fuso, como na fonte, para preservar a hora de embarque) | `IncompatibleInputError` |
| Entrada sem registros | `EmptyInputError` |
| Tabela de zonas com cabeçalho inesperado, IDs duplicados ou ausente | `ZoneReferenceError` / `InputNotFoundError` |

Erros de processamento Spark (ex.: página de dados corrompida no meio do arquivo) viram `ExecutionError`. Nesses casos a publicação anterior continua válida.
