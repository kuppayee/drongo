package in.drongo.drongodb.exception;

public class DrongoDBFileLockException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DrongoDBFileLockException() {
        super();
    }

    public DrongoDBFileLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public DrongoDBFileLockException(String message) {
        super(message);
    }

    public DrongoDBFileLockException(Throwable cause) {
        super(cause);
    }
    
}
