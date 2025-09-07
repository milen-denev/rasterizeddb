use crate::core::row::Row;
use once_cell::sync::Lazy;
use std::io;
use tera::{Context, Tera};

// Initialize Tera template engine
static TEMPLATES: Lazy<Tera> = Lazy::new(|| {
    let mut tera = Tera::default();
    tera.add_raw_template("table.html", include_str!("template.html"))
        .expect("Failed to add template");
    tera
});

/// Renders database query results as HTML using the Tera template engine
pub fn render_rows_to_html(
    rows_result: Result<Option<Vec<Row>>, io::Error>,
    table_name: &str,
) -> Result<String, io::Error> {
    // Set up the template context
    let mut context = Context::new();
    context.insert("table_name", table_name);

    match rows_result {
        Ok(Some(rows)) if !rows.is_empty() => {
            // Extract column headers - use COL(X) format
            let first_row = &rows[0];
            let columns = first_row.columns()?;
            let column_names: Vec<String> = (0..columns.len())
                .map(|i| format!("COL({})", i + 1))
                .collect();

            context.insert("columns", &column_names);

            // Process row data
            let mut row_data = Vec::with_capacity(rows.len());

            for row in &rows {
                let row_columns = row.columns()?;
                let values: Vec<String> = row_columns.iter().map(|col| col.into_value()).collect();
                row_data.push(values);
            }

            context.insert("rows", &row_data);
        }
        Ok(Some(_)) | Ok(None) => {
            // Empty dataset
            context.insert("columns", &Vec::<String>::new());
            context.insert("rows", &Vec::<Vec<String>>::new());
        }
        Err(e) => return Err(e),
    }

    // Render the template
    TEMPLATES.render("table.html", &context).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Template rendering error: {}", e),
        )
    })
}
