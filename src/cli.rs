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

    /// Update data before insertion in the format source_column=find_value:dest_column=new_value
    #[structopt(short = "u", long = "update", parse(try_from_str = parse_update))]
    pub updates: Option<Vec<(String, String, String, String)>>,
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

fn parse_update(s: &str) -> Result<(String, String, String, String)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!(
            "Invalid update mapping, expected 'source_column=value:dest_column=value'"
        ));
    }

    // Parsing the first part (source_column=value)
    let source_parts: Vec<&str> = parts[0].split('=').collect();
    if source_parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid source mapping"));
    }

    // Parsing the second part (dest_column=value)
    let dest_parts: Vec<&str> = parts[1].split('=').collect();
    if dest_parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid destination mapping"));
    }

    Ok((
        source_parts[0].into(),
        source_parts[1].into(),
        dest_parts[0].into(),
        dest_parts[1].into(),
    ))
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
