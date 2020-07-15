const { readFileSync } = require('fs');
const { Sequelize } = require('sequelize');
const Importer = require('mysql-import');

describe('Queries de seleção', () => {
  let sequelize;

  beforeAll(async () => {
    const importer = new Importer(
      { user: process.env.MYSQL_USER, password: process.env.MYSQL_PASSWORD, host: process.env.HOSTNAME }
    );

    await importer.import('./desafio1.sql');

    importer.disconnect();

    sequelize = new Sequelize(
      `mysql://${process.env.MYSQL_USER}:${process.env.MYSQL_PASSWORD}@${process.env.HOSTNAME}:3306/SpotifyClone`
    );
  });

  afterAll(async () => {
    await sequelize.query('DROP DATABASE SpotifyClone;', { type: 'RAW' });
    sequelize.close();
  });

  describe('Normalize as tabelas para a 3ª Forma Normal', () => {
    const hasForeignKey = async (table, referencedTable) => {
      const [{ REFERENCE_COUNT: referenceCount }] = await sequelize.query(
        `SELECT COUNT(COLUMN_NAME) AS REFERENCE_COUNT
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE
          TABLE_NAME = '${table}'
            AND REFERENCED_TABLE_NAME = '${referencedTable}'
            AND REFERENCED_COLUMN_NAME = (
            SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = '${referencedTable}' AND CONSTRAINT_NAME = 'PRIMARY'
            );`,
        { type: 'SELECT' }
      );

      return (referenceCount === 1);
    };

    const composedPrimaryKey = async (table) => {
      const [{ PK_COUNT: pkCount }] = await sequelize.query(
        `SELECT COUNT(COLUMN_NAME) AS PK_COUNT
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_NAME = '${table}' AND CONSTRAINT_NAME = 'PRIMARY';`,
        { type: 'SELECT' }
      );

      return (pkCount > 1);
    }

    it('Verifica os planos', async () => {
      const {
        tabela_que_contem_plano: planTable,
        tabela_que_contem_usuario: userTable,
      } = JSON.parse(readFileSync('desafio1.json', 'utf8'));

      expect(planTable).not.toBe(userTable);

      const plansCount = await sequelize.query(
        `SELECT COUNT(*) FROM ${planTable};`, { type: 'SELECT' }
      );

      expect(plansCount).toEqual([{ 'COUNT(*)': 3 }]);

      expect(await hasForeignKey(userTable, planTable)).toBeTruthy();
    });

    it('Verifica o histórico de reprodução', async () => {
      const {
        tabela_que_contem_historico_de_reproducoes: reproductionHistoryTable,
        tabela_que_contem_usuario: userTable,
        tabela_que_contem_cancoes: songTable,
      } = JSON.parse(readFileSync('desafio1.json', 'utf8'));

      expect(reproductionHistoryTable).not.toBe(userTable);
      expect(reproductionHistoryTable).not.toBe(songTable);

      const reproductionHistoryCount = await sequelize.query(
        `SELECT COUNT(*) FROM ${reproductionHistoryTable};`, { type: 'SELECT' }
      );

      expect(reproductionHistoryCount).toEqual([{ 'COUNT(*)': 14 }]);

      expect(await hasForeignKey(reproductionHistoryTable, songTable)).toBeTruthy();
      expect(await hasForeignKey(reproductionHistoryTable, userTable)).toBeTruthy();
      expect(await composedPrimaryKey(reproductionHistoryTable)).toBeTruthy();
    });

    it('Verifica pessoas seguindo artistas', async () => {
      const {
        tabela_que_contem_seguindo_artistas: followingTable,
        tabela_que_contem_usuario: userTable,
        tabela_que_contem_artista: artistTable,
      } = JSON.parse(readFileSync('desafio1.json', 'utf8'));

      expect(followingTable).not.toBe(userTable);
      expect(followingTable).not.toBe(artistTable);

      const reproductionHistoryCount = await sequelize.query(
        `SELECT COUNT(*) FROM ${followingTable};`, { type: 'SELECT' }
      );

      expect(reproductionHistoryCount).toEqual([{ 'COUNT(*)': 8 }]);

      expect(await hasForeignKey(followingTable, artistTable)).toBeTruthy();
      expect(await hasForeignKey(followingTable, userTable)).toBeTruthy();
      expect(await composedPrimaryKey(followingTable)).toBeTruthy();
    });

    it('Verifica os álbuns', async () => {
      const {
        tabela_que_contem_album: albumTable,
        tabela_que_contem_artista: artistTable,
      } = JSON.parse(readFileSync('desafio1.json', 'utf8'));

      expect(albumTable).not.toBe(artistTable);

      const plansCount = await sequelize.query(
        `SELECT COUNT(*) FROM ${albumTable};`, { type: 'SELECT' }
      );

      expect(plansCount).toEqual([{ 'COUNT(*)': 5 }]);

      expect(await hasForeignKey(albumTable, artistTable)).toBeTruthy();
    });

    it('Verifica as canções', async () => {
      const {
        tabela_que_contem_cancoes: songTable,
        tabela_que_contem_album: albumTable,
      } = JSON.parse(readFileSync('desafio1.json', 'utf8'));

      expect(songTable).not.toBe(albumTable);

      const songsCount = await sequelize.query(
        `SELECT COUNT(*) FROM ${songTable};`, { type: 'SELECT' }
      );

      expect(songsCount).toEqual([{ 'COUNT(*)': 18 }]);

      expect(await hasForeignKey(songTable, albumTable)).toBeTruthy();
    });
  });

  describe('Exibe as estatísticas musicais', () => {
    it('Verifica o desafio 2', async () => {
      const challengeQuery = readFileSync('desafio2.sql', 'utf8');

      await sequelize.query(challengeQuery, { type: 'RAW' });

      const result = await sequelize.query('SELECT * FROM estatisticas_musicais;', { type: 'SELECT' });

      expect(result).toEqual([{ cancoes: 18, artistas: 4, albuns: 5 }]);
    });
  });

  describe('Exibe o histórico de reprodução para cada pessoa usuária', () => {
    it('Verifica o desafio 3', async () => {
      const challengeQuery = readFileSync('desafio3.sql', 'utf8');

      await sequelize.query(challengeQuery, { type: 'RAW' });

      const result = await sequelize.query('SELECT * FROM historico_reproducao_usuarios;', { type: 'SELECT' });
      const expectedResult = [
        { nome: 'Magic Circus', usuario: 'Bill' },
        { nome: 'Thang Of Thunder', usuario: 'Bill' },
        { nome: 'Troubles Of My Inner Fire', usuario: 'Bill' },
        { nome: 'Home Forever', usuario: 'Cintia' },
        { nome: 'Honey, Let\'s Be Silly', usuario: 'Cintia' },
        { nome: 'Reflections Of Magic', usuario: 'Cintia' },
        { nome: 'Words Of Her Life', usuario: 'Cintia' },
        { nome: 'Celebration Of More', usuario: 'Roger' },
        { nome: 'Dance With Her Own', usuario: 'Roger' },
        { nome: 'Without My Streets', usuario: 'Roger' },
        { nome: 'Diamond Power', usuario: 'Thati' },
        { nome: 'Magic Circus', usuario: 'Thati' },
        { nome: 'Soul For Us', usuario: 'Thati' },
        { nome: 'Thang Of Thunder', usuario: 'Thati' },
      ];

      expect(result).toEqual(expectedResult);
    });
  });

  describe('Exibe top 3 artistas com maior quantidade de pessoas seguidoras ', () => {
    it('Verifica o desafio 4', async () => {
      const challengeQuery = readFileSync('desafio4.sql', 'utf8');

      await sequelize.query(challengeQuery, { type: 'RAW' });

      const result = await sequelize.query('SELECT * FROM top_3_artistas;', { type: 'SELECT' });
      const expectedResult = [
        { artista: 'Walter Phoenix', seguidores: 3 },
        { artista: 'Freedie Shannon', seguidores: 2 },
        { artista: 'Lance Day', seguidores: 2 },
      ];

      expect(result).toEqual(expectedResult);
    });
  });

  describe('Exibe top 2 hits mais tocados no momento', () => {
    it('Verifica o desafio 5', async () => {
      const challengeQuery = readFileSync('desafio5.sql', 'utf8');

      await sequelize.query(challengeQuery, { type: 'RAW' });

      const result = await sequelize.query('SELECT * FROM top_2_hits_do_momento;', { type: 'SELECT' });
      const expectedResult = [
        { cancao: 'Magic Circus', reproducoes: 2 },
        { cancao: 'Thang Of Thunder', reproducoes: 2 },
      ];

      expect(result).toEqual(expectedResult);
    });
  });
});
