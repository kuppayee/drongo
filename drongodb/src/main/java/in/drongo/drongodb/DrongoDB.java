package in.drongo.drongodb;

import java.io.File;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public abstract class DrongoDB {

    protected DrongoDB drongoDB;
    protected File directory;
    protected DrongoDBOptions drongoDBOptions;
    
    public DrongoDB open(File directory, DrongoDBOptions drongoDBOptions) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        drongoDB = createDrongoDB();
        log.info("DB instance initialized.");
        return drongoDB;
    }

    public byte[] get(byte[] key) {
        return drongoDB.get(key);
    }

    public void put(byte[] key, byte[] value) {
        drongoDB.put(key, value);
    }

    public void delete(byte[] key) {
        drongoDB.delete(key);
    }

    public void close() {
        drongoDB.close();
    }
    
    protected abstract DrongoDB createDrongoDB();

}
