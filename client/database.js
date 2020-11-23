const { Pool, Client } = require('pg')

const envConfig = process.env.pg_CONFIG;
const pgConfig = JSON.parse(envConfig || '{"host":"database-1.csxbhznoei2x.us-east-1.rds.amazonaws.com", "database":"themachineDB", "user":"dinobartolome", "password":"upwork2020"}');

// const client = new Client(pgConfig)
// client.connect()
const connectionString = 'postgresql://dinobartolome:upwork2020@database-1.csxbhznoei2x.us-east-1.rds.amazonaws.com:5432/themachineDB'
const pool = new Pool({
  connectionString,
})

//console.log('Working with pg config: ' + JSON.stringify(pgConfig))

exports.select = (table, clause = '', sortClause = '', skip = 0, limit = 1) =>
query(
    `SELECT * FROM ${table} ${clause} ${sortClause} LIMIT ${limit} OFFSET ${skip}`,
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
    result => result.rows[0]['count']
  )

exports.describeDatabase = () =>

query("SELECT * FROM information_schema.tables WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'", {}, async result => {
  var tables = []
  for (i=0;i<result.rows.length;i++)
  {
    tables.push(result.rows[i].table_name);
  }
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
  
  query(`SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '${table}'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;`, {}, result => {
    return (result.rows).map(entry => {
      return {
        name: entry['name'],
        type: entry['type'],
        isPrimary: entry['name'] === 'id'
      }
    })
  })
const query = (query, values, handler) =>
  new Promise((resolve, reject) => {
    pool.query(query, Array.from(values), (err, results, fields) => {
      if (err) {
        console.log(err);
        reject(err)
      }
      resolve(handler(results, fields))
    })
  })
