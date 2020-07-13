USE SpotifyClone;

DELIMITER $$

CREATE PROCEDURE albuns_do_artista(IN id INT)
BEGIN
  SELECT
    ar.id AS artista_id,
    ar.nome_artista AS artista,
    al.id AS album_id,
    al.nome_album AS album
  FROM artistas_por_albuns apa
  INNER JOIN albuns AS al ON al.id = apa.album_id
  INNER JOIN artistas AS ar ON ar.id = apa.artista_id
  WHERE ar.id = id;
END $$

DELIMITER ;

CALL albuns_do_artista(1);
