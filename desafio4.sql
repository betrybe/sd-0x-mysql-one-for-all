USE SpotifyClone;

CREATE VIEW top_3_artistas AS
SELECT
  a.id AS artista_id,
  a.nome_artista AS artista,
  COUNT(usa.artista_id) AS seguidores
FROM artistas AS a
INNER JOIN usuario_seguindo_artista AS usa ON a.id = usa.artista_id
GROUP BY a.id
ORDER BY seguidores DESC
LIMIT 3;
