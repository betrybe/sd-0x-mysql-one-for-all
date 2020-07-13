CREATE DATABASE IF NOT EXISTS SpotifyClone;

USE SpotifyClone;

CREATE TABLE planos(
  id INT AUTO_INCREMENT,
  nome VARCHAR(50) NOT NULL,
  valor DECIMAL(9,2) NOT NULL,
  PRIMARY KEY (id)
) engine = InnoDB;

CREATE TABLE usuarios(
  id INT AUTO_INCREMENT,
  nome VARCHAR(100) NOT NULL,
  idade INT NOT NULL,
  plano_id INT NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (plano_id) REFERENCES planos(id)
) engine = InnoDB;

CREATE TABLE artistas(
  id INT AUTO_INCREMENT,
  nome_artista VARCHAR(100) NOT NULL,
  PRIMARY KEY (id)
) engine = InnoDB;

CREATE TABLE albuns(
  id INT AUTO_INCREMENT,
  nome_album VARCHAR(100) NOT NULL,
  PRIMARY KEY (id)
) engine = InnoDB;

CREATE TABLE cancoes(
  id INT AUTO_INCREMENT,
  nome_cancao VARCHAR(100) NOT NULL,
  PRIMARY KEY (id)
) engine = InnoDB;

CREATE TABLE artistas_por_albuns(
  artista_id INT NOT NULL,
  album_id INT NOT NULL,
  PRIMARY KEY (artista_id, album_id),
  FOREIGN KEY (artista_id) REFERENCES artistas(id),
  FOREIGN KEY (album_id) REFERENCES albuns(id)
) engine = InnoDB;

CREATE TABLE albuns_por_cancoes(
  album_id INT NOT NULL,
  cancao_id INT NOT NULL,
  PRIMARY KEY (album_id, cancao_id),
  FOREIGN KEY (album_id) REFERENCES albuns(id),
  FOREIGN KEY (cancao_id) REFERENCES cancoes(id)
) engine = InnoDB;

CREATE TABLE usuario_seguindo_artista(
  usuario_id INT NOT NULL,
  artista_id INT NOT NULL,
  PRIMARY KEY (usuario_id, artista_id),
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
  FOREIGN KEY (artista_id) REFERENCES artistas(id)
) engine = InnoDB;

CREATE TABLE historico_reproducao(
  usuario_id INT NOT NULL,
  cancao_id INT NOT NULL,
  PRIMARY KEY (usuario_id, cancao_id),
  FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
  FOREIGN KEY (cancao_id) REFERENCES cancoes(id)
) engine = InnoDB;

DELIMITER $$

CREATE TRIGGER trigger_usuario_delete
  BEFORE DELETE ON usuarios
  FOR EACH ROW
BEGIN
  DELETE FROM historico_reproducao
  WHERE usuario_id = OLD.id;
  DELETE FROM usuario_seguindo_artista
  WHERE usuario_id = OLD.id;
END $$
DELIMITER ;

INSERT INTO planos (id, nome, valor) VALUES
  (1, 'gratuito', 0),
  (2, 'famíliar', 7.99),
  (3, 'universitário', 5.99);

INSERT INTO usuarios (id, nome, idade, plano_id) VALUES
  (15, 'Thati', 23, 1),
  (16, 'Cintia', 35, 2),
  (17, 'Bill', 20, 3),
  (18, 'Roger', 45, 1);

INSERT INTO artistas (id, nome_artista) VALUES
  (1, 'Walter Phoenix'),
  (2, 'Peter Strong'),
  (3, 'Lance Day'),
  (4, 'Freedie Shannon');

INSERT INTO albuns (id, nome_album) VALUES
  (1, 'Envious'),
  (2, 'Exuberant'),
  (3, 'Hallowed Steam'),
  (4, 'Incandescent'),
  (5, 'Temporary Culture');

INSERT INTO cancoes (id, nome_cancao) VALUES
  (1, 'Soul For Us'),
  (2, 'Reflections Of Magic'),
  (3, 'Dance With Her Own'),
  (4, 'Troubles Of My Inner Fire'),
  (5, 'Time Fireworks'),
  (6, 'Magic Circus'),
  (7, 'Honey, So Do I'),
  (8, 'Sweetie, Let\'s Go Wild'),
  (9, 'She Knows'),
  (10, 'Fantasy For Me'),
  (11, 'Celebration Of More'),
  (12, 'Rock His Everything'),
  (13, 'Home Forever'),
  (14, 'Diamond Power'),
  (15, 'Honey, Let\'s Be Silly'),
  (16, 'Thang Of Thunder'),
  (17, 'Words Of Her Life'),
  (18, 'Without My Streets');

INSERT INTO artistas_por_albuns(artista_id, album_id) VALUES
  (1, 1),
  (1, 2),
  (2, 3),
  (3, 4),
  (4, 5);

INSERT INTO albuns_por_cancoes(album_id, cancao_id) VALUES
  (1, 1),
  (1, 2),
  (1, 3),
  (2, 4),
  (2, 5),
  (3, 6),
  (3, 7),
  (3, 8),
  (3, 9),
  (4, 10),
  (4, 11),
  (4, 12),
  (4, 13),
  (4, 14),
  (4, 15),
  (5, 16),
  (5, 17),
  (5, 18);

INSERT INTO usuario_seguindo_artista(usuario_id, artista_id) VALUES
  (15, 1),
  (15, 4),
  (15, 3),
  (16, 1),
  (16, 3),
  (17, 2),
  (17, 1),
  (18, 4);

INSERT INTO historico_reproducao(usuario_id, cancao_id) VALUES
  (15, 1),
  (15, 6),
  (15, 14),
  (15, 16),
  (16, 13),
  (16, 17),
  (16, 2),
  (16, 15),
  (17, 4),
  (17, 16),
  (17, 6),
  (18, 3),
  (18, 18),
  (18, 11);
