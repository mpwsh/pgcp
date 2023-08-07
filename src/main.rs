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
        &opt.updates,
    )
    .await
}

async fn transfer_data(
    from: &str,
    to: &str,
    table: &StringPair,
    cols: &[StringPair],
    static_cols: &Option<Vec<(String, String)>>,
    updates: &Option<Vec<(String, String, String, String)>>,
) -> Result<()> {
    // Connect to source
    let source_client = connect(from).await?;

    let (from_cols_string, join_clauses) =
        build_source_cols_and_joins(table, cols, static_cols, updates);
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

    let to_cols_string = build_dest_cols(cols, static_cols, updates);

    info!("{to_cols_string}");
    let values_clause = build_values_clause(&rows, updates);

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

fn build_dest_cols(
    cols: &[StringPair],
    static_cols: &Option<Vec<(String, String)>>,
    updates: &Option<Vec<(String, String, String, String)>>,
) -> String {
    let mut dest_cols: Vec<String> = cols
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

    if let Some(static_values) = static_cols {
        dest_cols.extend(static_values.iter().map(|(col, _)| col.clone()));
    }

    if let Some(upd) = updates {
        for (_, _, dest_col, _) in upd {
            if !dest_cols.contains(dest_col) {
                dest_cols.push(dest_col.clone());
            }
        }
    }
    dest_cols.join(", ")
}

fn build_values_clause(
    rows: &[Row],
    updates: &Option<Vec<(String, String, String, String)>>,
) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "({})",
                row.columns()
                    .iter()
                    .map(|column| {
                        let mut value = any_to_string(row, column.name());
                        // Apply updates if necessary
                        if let Some(upd) = updates {
                            for (col, old_val, _, new_val) in upd {
                                if col == column.name() && value == *old_val {
                                    value = new_val.clone();
                                }
                            }
                        }
                        format!("'{}'", value.replace('\'', "''"))
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        })
        .collect::<Vec<String>>()
        .join(", ")
}

fn get_update_source_columns(
    updates: &Option<Vec<(String, String, String, String)>>,
) -> Vec<String> {
    updates
        .as_ref()
        .map(|upd| {
            upd.iter()
                .map(|(col, _, _, _)| col.clone())
                .collect::<std::collections::HashSet<String>>()
                .into_iter()
                .collect::<Vec<String>>()
        })
        .unwrap_or_else(Vec::new)
}

fn build_source_cols_and_joins(
    table: &StringPair,
    cols: &[StringPair],
    static_cols: &Option<Vec<(String, String)>>,
    updates: &Option<Vec<(String, String, String, String)>>,
) -> (String, String) {
    let update_cols = get_update_source_columns(updates);

    let (from_cols, joins): (Vec<_>, Vec<_>) = cols
        .iter()
        .map(|c| {
            let source = &c.source;
            if source.contains('/') {
                let mut split = source.split('/');
                let join_part = split.next().unwrap();
                let select_part = split.next().unwrap();
                let (select_table, select_column) = select_part
                    .split_once('.')
                    .unwrap_or((table.source.as_str(), select_part));
                let from_col = format!(
                    "{}.{} AS {}",
                    select_table,
                    select_column,
                    source.replace(['/', '.'], "_")
                );
                let (join_table, join_column) = join_part
                    .split_once('.')
                    .unwrap_or((table.source.as_str(), join_part));
                let join = format!(
                    "INNER JOIN {} ON {}.{} = {}.{}",
                    select_table, join_table, join_column, select_table, join_column
                );
                (from_col, Some(join))
            } else {
                (format!("{}.{}", table.source, source), None)
            }
        })
        .unzip();

    let all_cols = from_cols
        .into_iter()
        .chain(update_cols)
        .chain(
            static_cols
                .iter()
                .flatten()
                .map(|(col, value)| format!("'{}' AS {}", value, col)),
        )
        .collect::<Vec<_>>()
        .join(", ");

    let all_joins = joins.into_iter().flatten().collect::<Vec<_>>().join(" ");

    (all_cols, all_joins)
}
