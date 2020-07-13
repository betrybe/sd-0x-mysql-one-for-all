const { readFileSync } = require('fs');
const { Sequelize } = require('sequelize');
const Importer = require('mysql-import');

describe('Desafios obrigatórios', () => {
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
        tabela_que_contem_historico_reproducao: reproductionHistoryTable,
        tabela_que_contem_usuario: userTable,
        tabela_que_contem_cancao: songTable,
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
        tabela_que_contem_seguindo_artista: followingTable,
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
  });
});
