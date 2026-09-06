use crate::ClientConfig;

const MAX_TABLE_ID_CHARS: usize = 50;
pub(crate) const MAX_ROW_KEY_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableIdIssue {
    Empty,
    TooLong,
    ContainsSlash,
}

pub(crate) fn validate_table_id(table_id: &str) -> Result<(), TableIdIssue> {
    if table_id.is_empty() {
        return Err(TableIdIssue::Empty);
    }
    if table_id.chars().count() > MAX_TABLE_ID_CHARS {
        return Err(TableIdIssue::TooLong);
    }
    if table_id.contains('/') {
        return Err(TableIdIssue::ContainsSlash);
    }
    Ok(())
}

pub(crate) fn table_name(config: &ClientConfig, table_id: &str) -> String {
    format!(
        "projects/{}/instances/{}/tables/{table_id}",
        config.project_id(),
        config.instance_id()
    )
}
