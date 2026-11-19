-- Quantos registros foram rejeitados por cada motivo.
-- Um registro pode ter mais de um motivo; por isso a soma pode ser maior que o total de rejeitados.
SELECT
  reason,
  COUNT(*) AS registros
FROM (
  SELECT EXPLODE(reject_reasons) AS reason
  FROM rejected
)
GROUP BY reason
ORDER BY registros DESC, reason
