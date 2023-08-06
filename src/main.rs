use anyhow::Result;
use cli::{Opt, StringPair};
use db::connect;
use log::info;
use structopt::StructOpt;
use tokio_postgres::Row;
use util::any_to_string;

mod cli;
mod db;
mod util;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    transfer_data(
        &opt.from,
        &opt.to,
        &opt.table,
        &opt.columns,
        &opt.static_cols,
    )
    .await
}

async fn transfer_data(
    from: &str,
    to: &str,
    table: &StringPair,
    cols: &[StringPair],
    static_cols: &Option<Vec<(String, String)>>,
) -> Result<()> {
    // Connect to source
    let source_client = connect(from).await?;

    let (from_cols_string, join_clauses) = build_source_cols_and_joins(table, cols, static_cols);

    // Build the query
    let query = format!(
        "SELECT {} FROM {} {}",
        from_cols_string, table.source, join_clauses
    );

    info!("{}", query);
    // Query the source database
    let rows = source_client.query(query.as_str(), &[]).await?;

    // Now connect to destination
    let dest_client = connect(to).await?;

    let to_cols_string = build_dest_cols(cols, static_cols);
    let values_clause = build_values_clause(&rows);
    // Build the SQL query
    let query = format!(
        "INSERT INTO {} ({}) VALUES {}",
        table.dest, to_cols_string, values_clause
    );

    info!("{}", query);
    // Execute the batch insert
    dest_client.execute(query.as_str(), &[]).await?;

    Ok(())
}

fn build_dest_cols(cols: &[StringPair], static_cols: &Option<Vec<(String, String)>>) -> String {
    let to_cols: Vec<String> = cols
        .iter()
        .map(|c| {
            if c.dest.contains('/') {
                let split: Vec<&str> = c.dest.split('/').collect();
                split[1].to_string()
            } else {
                c.dest.clone()
            }
        })
        .collect();

    let mut all_cols = to_cols;
    // Add static columns
    if let Some(static_values) = static_cols {
        let static_cols: Vec<String> = static_values.iter().map(|(col, _)| col.clone()).collect();
        all_cols.extend(static_cols);
    }

    all_cols.join(", ")
}

fn build_values_clause(rows: &[Row]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "({})",
                row.columns()
                    .iter()
                    .map(|column| {
                        let value = any_to_string(row, column.name());
                        format!("'{}'", value.replace('\'', "''"))
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        })
        .collect::<Vec<String>>()
        .join(", ")
}

fn build_source_cols_and_joins(
    table: &StringPair,
    cols: &[StringPair],
    static_cols: &Option<Vec<(String, String)>>,
) -> (String, String) {
    let (from_cols, joins): (Vec<_>, Vec<_>) = cols
        .iter()
        .map(|c| {
            let (table_name, column_name, join_select) = if c.source.contains('/') {
                let split: Vec<&str> = c.source.split('/').collect();
                let table_column: Vec<&str> = split[0].split('.').collect();
                (table_column[0], table_column[1], Some(split[1]))
            } else {
                (&table.source[..], &c.source[..], None)
            };

            let from_col = match join_select {
                Some(select) => format!(
                    "{}.{} AS {}",
                    table_name,
                    select,
                    c.source.replace(['/', '.'], "_")
                ),
                None => format!("{}.{}", table_name, c.source.clone()),
            };

            let join = if c.source.contains('/') {
                Some(format!(
                    "INNER JOIN {} ON {}.{} = {}.{}",
                    table_name, table.source, column_name, table_name, column_name
                ))
            } else {
                None
            };

            (from_col, join)
        })
        .unzip();

    let mut all_cols = from_cols;
    // Handle static columns
    if let Some(static_values) = static_cols {
        let static_cols: Vec<String> = static_values
            .iter()
            .map(|(col, value)| format!("'{}' AS {}", value, col))
            .collect();
        all_cols.extend(static_cols);
    }

    (
        all_cols.join(", "),
        joins.into_iter().flatten().collect::<Vec<_>>().join(" "),
    )
}
