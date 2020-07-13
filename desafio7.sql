USE SpotifyClone;

CREATE VIEW perfil_artistas AS
SELECT 
  ar.id AS artista_id,
  ar.nome_artista AS artista,
  al.id AS album_id,
  al.nome_album AS album,
  (SELECT COUNT(usa.artista_id)
    FROM usuario_seguindo_artista AS usa
    WHERE usa.artista_id = ar.id) AS seguidores
FROM artistas_por_albuns AS apa
INNER JOIN artistas AS ar ON apa.artista_id = ar.id
INNER JOIN albuns AS al ON apa.album_id = al.id
ORDER BY seguidores DESC;
