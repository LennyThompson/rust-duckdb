use duckdb::{params, Connection, Result};
use duckdb::arrow::record_batch::RecordBatch;

#[derive(Debug)]
struct Jackpot {
    pool: i32,
    name: String
}

fn main() {
    let duckConn = Connection::open_in_memory();
    let duckCreateTable = "CREATE TABLE JL1 AS SELECT * FROM read_csv_auto\
                (\
                './data/01607795.Jl1',\
                 delimiter=',',\
                 header=False, \
                 columns={\
                 'Pool': 'INTEGER', \
                 'Jurisdiction': 'INTEGER', \
                 'Variation': 'INTEGER', \
                 'Name': 'VARCHAR', \
                 'Reset': 'INTEGER', \
                 'Max': 'INTEGER', \
                 'Increment': 'DOUBLE', \
                 'HitRate': 'DOUBLE', \
                 'Param9': 'DOUBLE', \
                 'Param10': 'DOUBLE', \
                 'Param11': 'DOUBLE', \
                 'Param12': 'INTEGER', \
                 'Param13': 'INTEGER', \
                 'AuxReset': 'INTEGER',\
                 'Param15': 'VARCHAR', \
                 'Param16': 'VARCHAR', \
                 'Param17': 'VARCHAR', \
                 'Param18': 'VARCHAR', \
                 'Param19': 'VARCHAR', \
                 'Param20': 'VARCHAR', \
                 'Date': 'BIGINT', \
                 'Time': 'BIGINT', \
                 'Param23': 'VARCHAR'\
                 }\
                )";

    duckConn.execute_batch(duckCreateTable, ).expect("Ducdb failed to create table from csv");

    let mut stmt = duckConn.prepare("SELECT Pool, Name FROM JL1").unwrap();
    let jp_iter = stmt.query_map([], |row| {
        Ok(Jackpot {
            pool: row.get(0)?,
            name: row.get(1)?,
        })
    });

    for jp in jp_iter {
        println!("Found jackpot {:?}", jp.unwrap());
    }

    println!("Hello, world!");
}
