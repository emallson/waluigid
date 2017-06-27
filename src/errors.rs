error_chain!{
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
    }
}
