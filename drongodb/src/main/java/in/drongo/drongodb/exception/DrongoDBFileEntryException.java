package in.drongo.drongodb.exception;

public class DrongoDBFileEntryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DrongoDBFileEntryException() {
        super();
    }

    public DrongoDBFileEntryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DrongoDBFileEntryException(String message) {
        super(message);
    }

    public DrongoDBFileEntryException(Throwable cause) {
        super(cause);
    }

}
