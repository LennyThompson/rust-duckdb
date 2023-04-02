use duckdb::{params, Connection, Result};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::print_batches;


#[derive(Debug)]
struct Jackpot {
    pool: String,
    nameRaw: String
}

impl Jackpot {

    fn poolNumber(&self) -> i32 {
        match i32::from_str_radix(self.pool.trim(), 16) {
            Ok(poolNumFromHex) => return poolNumFromHex,
            Err(e) => return -1,
        }
    }

    fn name(&self) -> &str {
        return self.nameRaw.trim();
    }
}

fn main() -> Result<()> {
    let duckConn = Connection::open_in_memory()?;
    let duckCreateTable = "CREATE TABLE JL1 AS SELECT * FROM read_csv_auto\
                (\
                '/home/lenny/CLionProjects/first-look/data/01113293/01607795.jl1',\
                 delim=',',\
                 header=False, \
                 columns={\
                 'Pool': 'TEXT', \
                 'Jurisdiction': 'INTEGER', \
                 'Variation': 'INTEGER', \
                 'Name': 'TEXT', \
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

    duckConn.execute_batch(duckCreateTable, ).expect("DuckDb failed to create table from csv");

    let mut stmt = duckConn.prepare("SELECT Pool, Name FROM JL1")?;
    let jp_iter = stmt.query_map([], |row| {
        Ok(Jackpot {
            pool: row.get(0)?,
            nameRaw: row.get(1)?,
        })
    })?;

    for jp in jp_iter {
        match jp {
            Ok(ref jpLocal) => {
                println!("Found jackpot pool number: {0:?}, name: {1:?}", jpLocal.poolNumber(), jpLocal.name());

            },
            Err(e) => println!("Error: {:?}", e),
        }
    }

    println!("Hello, world!");
    Ok(())
}

/*

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
         ")?;
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }

    // query table by arrow
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs);
    Ok(())
}*/