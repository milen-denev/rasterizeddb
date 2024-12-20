pub(crate) fn whitespace_spec_splitter(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for c in input.chars() {
        match c {
            '\'' => {
                in_quotes = !in_quotes; // Toggle the in_quotes flag
                current.push(c);       // Include the quote in the current part
            }
            ' ' if !in_quotes => {
                if !current.is_empty() {
                    result.push(current.trim().to_string());
                    current.clear();
                }
            }
            _ => {
                current.push(c);
            }
        }
    }

    // Add the last collected part, if any
    if !current.is_empty() {
        result.push(current.trim().to_string());
    }

    result
}