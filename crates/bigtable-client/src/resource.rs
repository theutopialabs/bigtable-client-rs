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

#[cfg(test)]
mod tests {
    use super::{TableIdIssue, table_name, validate_table_id};
    use crate::ClientConfig;

    #[test]
    fn valid_table_id_builds_resource_name() {
        let config = ClientConfig::new("project", "instance").expect("valid config");

        assert_eq!(
            table_name(&config, "events"),
            "projects/project/instances/instance/tables/events"
        );
    }

    #[test]
    fn table_id_validation_covers_each_failure() {
        assert_eq!(validate_table_id(""), Err(TableIdIssue::Empty));
        assert_eq!(
            validate_table_id(&"a".repeat(51)),
            Err(TableIdIssue::TooLong)
        );
        assert_eq!(
            validate_table_id("tables/events"),
            Err(TableIdIssue::ContainsSlash)
        );
    }
}
