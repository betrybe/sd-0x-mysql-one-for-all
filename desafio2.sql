USE SpotifyClone;

CREATE VIEW estatisticas_musicais AS
SELECT 
  COUNT(DISTINCT ac.cancao_id) AS cancoes,
  COUNT(DISTINCT aa.artista_id) AS artistas,
  COUNT(DISTINCT aa.album_id) as albuns
FROM artistas_por_albuns AS aa, albuns_por_cancoes AS ac;
