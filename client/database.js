const { Pool } = require('pg')
const connectionString = 'postgresql://dinobartolome:upwork2020@database-1.csxbhznoei2x.us-east-1.rds.amazonaws.com:5432/test1'
const pool = new Pool({
  connectionString,
})
//console.log('Working with pg config: ' + JSON.stringify(pgConfig))
const schemaName = 'thestorybook';    

exports.select = (table, clause = '', sortClause = '', skip = 0, limit = 1) => {
  query(
    `SELECT * FROM ${schemaName}.${table} ${clause} ${sortClause} LIMIT ${limit} OFFSET ${skip}`,
      {},
      identity => identity
    )
}  

exports.insert = (table, item) => {
  //query(`INSERT INTO ${table} SET ?`, item, () => item)
  let keys_string = '';
  let values_string = '';
  for (var key in item) {
    if(key != '_id') {
      keys_string+=key+',';  
      values_string+='"'+item[key]+'",';
    }
  }
  keys_string = keys_string.slice(0, -1);
  values_string = values_string.slice(0, -1);
  query(`INSERT INTO ${table} (${keys_string}) values (${values_string})`);
}


exports.update = (table, item) => { 
  let values_string = '';
  let key_id = '';
  let update_key = '';
  for (var key in item) {
    if(key != '_id') {
      values_string=item[key];
      update_key+=key+'="'+values_string+'",';
    } else {
      key_id = item[key];
    }
  }
  update_key = update_key.slice(0, -1);
  query(`UPDATE ${schemaName}.${table} SET ${update_key}  WHERE _id = ${key_id}  `,
      item,
      () => item
  );
  /*query(
    `UPDATE ${table} SET ? WHERE _id = ${client.escape(item._id)}`,
    item,
    () => item
  )*/
}  

exports.deleteOne = (table, itemId) => {
  query(`DELETE FROM ${schemaName}.${table} WHERE id = ${itemId}`,
    {},
    result => result.affectedRows
  );
  /*query(
      `DELETE FROM ${table} WHERE _id = ${client.escape(itemId)}`,
      {},
      result => result.affectedRows
    )*/
}  

exports.count = (table, clause) =>
query(
    `SELECT COUNT(*) FROM ${schemaName}.${table} ${clause}`,
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
  
  query(`SELECT table_schema,table_name,column_name AS name,data_type AS type,case when character_maximum_length is not null then character_maximum_length else numeric_precision end as max_length, is_nullable from information_schema.columns WHERE table_schema not in ('information_schema', 'pg_catalog') AND table_name='${table}'`, {}, result => {
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
