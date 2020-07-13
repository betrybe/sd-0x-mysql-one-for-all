USE SpotifyClone;

CREATE VIEW faturamento_atual AS
SELECT 
  MIN(p.valor) AS faturamento_minimo,
  MAX(p.valor) AS faturamento_maximo,
  AVG(p.valor) AS faturamento_medio,
  SUM(p.valor) AS faturamento_total
FROM usuarios AS u
INNER JOIN planos AS p ON p.id = u.plano_id;
