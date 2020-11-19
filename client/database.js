const pg = require('pg');
const envConfig = process.env.pg_CONFIG;

const pgConfig = JSON.parse(envConfig || '{"host":"database-1.csxbhznoei2x.us-east-1.rds.amazonaws.com", "database":"themachineDB", "user":"dinobartolome", "password":"upwork2020"}');

//console.log('Working with pg config: ' + JSON.stringify(pgConfig))
var client = new pg.Client(pgConfig);
client
  .connect()
  .then(() => console.log('connected'))
  .catch(err => console.error('connection error', err.stack))
exports.select = (table, clause = '', sortClause = '', skip = 0, limit = 1) =>
  query(
    `SELECT * FROM ${table} ${clause} ${sortClause} LIMIT ${skip}, ${limit}`,
    {},
    identity => identity
  )

exports.insert = (table, item) =>
  query(`INSERT INTO ${table} SET ?`, item, () => item)

exports.update = (table, item) =>
  query(
    `UPDATE ${table} SET ? WHERE _id = ${client.escape(item._id)}`,
    item,
    () => item
  )

exports.deleteOne = (table, itemId) =>
  query(
    `DELETE FROM ${table} WHERE _id = ${client.escape(itemId)}`,
    {},
    result => result.affectedRows
  )

exports.count = (table, clause) =>
  query(
    `SELECT COUNT(*) FROM ${table} ${clause}`,
    {},
    result => result[0]['COUNT(*)']
  )

exports.describeDatabase = () =>
  query('SHOW TABLES', {}, async result => {
    const tables = result.map(entry => entry[`Tables_in_${pgConfig.database}`])

    return Promise.all(
      tables.map(async table => {
        const columns = await describeTable(table)

        return {
          table,
          columns
        }
      })
    )
  })

const describeTable = table =>
  query(`DESCRIBE ${table}`, {}, result => {
    return result.map(entry => {
      return {
        name: entry['Field'],
        type: entry['Type'],
        isPrimary: entry['Key'] === 'PRI'
      }
    })
  })

const query = (query, values, handler) =>
  new Promise((resolve, reject) => {
    client.query(query, values, (err, results, fields) => {
      if (err) {
        console.log(err);
        reject(err)
      }

      resolve(handler(results, fields))
    })
  })
