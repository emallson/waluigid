error_chain!{
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    foreign_links {
        Io(::std::io::Error);
    }
    errors {
        NotReady {
            description("Task is not ready")
            display("Task is not ready")
        }
        TokenStreamError {
            description("Failed to receive from token stream")
            display("Failed to receive from token stream")
        }
        DependencyError {
            description("One or more dependencies failed to complete.")
            display("One or more dependencies failed to complete.")
        }
    }
}
