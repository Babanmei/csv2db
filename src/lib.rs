#[macro_use]
extern crate anyhow;
extern crate clap;
#[macro_use]
extern crate serde_derive;
extern crate toml;

use std::fs::File;
use std::io::Read;

use anyhow::Result;
use mysql::{Pool, PooledConn, Row};
use mysql::prelude::{Queryable, ToValue};

#[derive(Serialize, Deserialize, Debug)]
pub struct Args {
    pub host: String,
    pub port: String,
    pub user: String,
    pub pwd: String,
    pub db: String,
    pub tb: String,
    pub skip_headers: bool,
}

impl Args {
    pub fn new() -> Args {
        Args {
            host: "".to_string(),
            port: "".to_string(),
            user: "".to_string(),
            pwd: "".to_string(),
            db: "".to_string(),
            tb: "".to_string(),
            skip_headers: false,
        }
    }
}

pub struct App {
    pub args: Args,
    conn: PooledConn,
}

impl App {
    pub fn from_args(args: Args) -> Result<App> {
        let _url = format!("mysql://{}:{}@{}:{}/{}",
                          args.user,
                          args.pwd,
                          args.host,
                          args.port,
                          args.db
        );

        let pool = Pool::new(_url)?;
        let conn = pool.get_conn()?;

        Ok(App { args, conn })
    }

    pub fn parse_statement(&mut self) -> Vec<(String, String)> {
        //let desc = format!("desc {}.{}", self.args.db, self.args.tb);
        let desc = "desc test.msg_mkt_list_20200902";
        let v: Vec<Row> = self.conn.query(desc).unwrap();
        let tupe: Vec<(String, String)> = v.iter().map(|x| {
            let name = x[0].to_value().as_sql(true);
            let mut tpe = x[1].to_value().as_sql(false);
            let s = tpe.as_str();
            if s.contains("(") {
                let end = s.find('(').unwrap();
                tpe = s[1..end].to_string();
            }
            let tpe = tpe.trim_matches('\'').to_string();
            let name = name.replace("'", "`");
            (name, tpe)
        }).collect();
        tupe
    }

    pub fn import_from_stdin(&mut self, stdin: std::io::Stdin) {
        let tupe = self.parse_statement();
        let mut rdr = csv::Reader::from_reader(stdin);
        let skip = if self.args.skip_headers { 1 } else { 0 };
        rdr.records().skip(skip).for_each(|record| {
            match record {
                Ok(record) => {
                    let (mut names, mut values) = (String::new(), String::new());
                    let (mut idx, count) = (0, record.len());
                    record.iter().for_each(|r| {
                        let (name, tpe) = tupe.get(idx).unwrap();
                        names.push_str(name);
                        if is_str(tpe) {
                            values.push_str("\'");
                            values.push_str(r);
                            values.push_str("\'");
                        } else {
                            values.push_str(r);
                        }
                        if idx != count - 1 {
                            values.push_str(",");
                            names.push_str(",");
                        }
                        idx += 1;
                    });
                    let sql = format!(r#"insert into {}.{} ({}) values({});"#, self.args.db, self.args.tb, names, values);
                    println!("{}", sql);
                },
                Err(e) => println!("{}", e),
            }
        });
    }
}

fn is_str(tpe: &str) -> bool {
    match tpe {
        "int" => false,
        "double" => false,
        _ => true,
    }
}

fn load_toml() -> Result<Args> {
    let mut f = File::open("config.toml")?;
    let mut s = vec![];
    let _ = f.read_to_end(&mut s);
    match toml::from_slice(&mut s) {
        Err(_e) => Err(anyhow!("load toml error")),
        Ok(a) => Ok(a),
    }
}

pub fn cli() -> Args {
    let yaml = clap::load_yaml!("cli.yml");
    let matches = clap::App::from(yaml).get_matches();

    let mut args = match load_toml() {
        Err(_e) => Args::new(),
        Ok(a) => a,
    };

    if let Some(host) = matches.value_of("host") {
        args.host = host.to_owned();
    }
    if let Some(port) = matches.value_of("port") {
        args.port = port.to_owned();
    }
    if let Some(user) = matches.value_of("user") {
        args.user = user.to_owned();
    }
    if let Some(pwd) = matches.value_of("password") {
        args.pwd = pwd.to_owned();
    }
    if let Some(db) = matches.value_of("db") {
        args.db = db.to_owned();
    }
    if let Some(tb) = matches.value_of("table") {
        args.tb = tb.to_owned();
    }

    if 1 == matches.occurrences_of("skip-headers"){
        args.skip_headers = true
    }

    args
}

