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
  });
});
