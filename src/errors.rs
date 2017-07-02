error_chain!{
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    foreign_links {
        Io(::std::io::Error);
        JSON(::serde_json::Error);
    }
    errors {
        NotReady {
            description("Task is not ready")
            display("Task is not ready")
        }
        NotFull {
            description("Task template has not been filled")
            display("Task template has not been filled")
        }
        TokenStreamError {
            description("Failed to receive from token stream")
            display("Failed to receive from token stream")
        }
        DependencyError {
            description("One or more dependencies failed to complete.")
            display("One or more dependencies failed to complete.")
        }
        MissingField {
            description("One or more fields could not be read from dependency output.")
            display("One or more fields could not be read from dependency output.")
        }
        AlreadyComplete {
            description("Task is already complete when the future tried to run.")
            display("Task is already complete when the future tried to run.")
        }
    }
}
