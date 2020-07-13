USE SpotifyClone;

CREATE VIEW CancoesPremium AS
SELECT 
  c.nome_cancao AS nome,
  COUNT(hr.cancao_id) AS reproducoes
FROM historico_reproducao AS hr
INNER JOIN cancoes AS c ON c.id = hr.cancao_id
INNER JOIN usuarios AS u ON u.id = hr.usuario_id
WHERE u.plano_id IN (2, 3)
GROUP BY c.id
ORDER BY nome;
