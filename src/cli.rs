use anyhow::Result;
use std::str::FromStr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "pgtransfer", about = "Postgres table transfer CLI.")]
pub struct Opt {
    /// Source DB host:port
    #[structopt(long = "from", env = "PG_FROM_DATABASE")]
    pub from: String,

    /// Destination DB host:port
    #[structopt(long = "to", env = "PG_TO_DATABASE")]
    pub to: String,

    /// Table mapping in the format source_table:dest_table
    #[structopt(short = "t", long = "table")]
    pub table: StringPair,

    /// Column mappings in the format source_col:dest_col
    #[structopt(short = "c", long = "col")]
    pub columns: Vec<StringPair>,

    /// Static data insertion in the format column_name=data_to_insert
    #[structopt(short = "s", long = "static", parse(try_from_str = parse_kv))]
    pub static_cols: Option<Vec<(String, String)>>,
}

#[derive(Debug)]
pub struct StringPair {
    pub source: String,
    pub dest: String,
}
impl FromStr for StringPair {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "Invalid string pair, expected 'source:destination'"
            ));
        }
        Ok(StringPair {
            source: parts[0].into(),
            dest: parts[1].into(),
        })
    }
}

impl std::fmt::Display for StringPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.source, self.dest)
    }
}

fn parse_kv(s: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!(
            "Invalid key-value pair, expected 'key=value'"
        ));
    }
    Ok((parts[0].into(), parts[1].into()))
}
