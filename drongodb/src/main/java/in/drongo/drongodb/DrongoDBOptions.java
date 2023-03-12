package in.drongo.drongodb;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public final class DrongoDBOptions  implements Serializable {
    private static final long serialVersionUID = 1L;
    //public int ssTableThresholdSize = 8 * 1024 * 1024; /* 8MB */
    public int ssTableThresholdSize = 1024; /* KB */
    public static final int COMPACT_AND_MERGE_SCHEDULED_INITIAL_DELAY = (4 << 1) + 4;//4 * 3;
    public static final int COMPACT_AND_MERGE_SCHEDULED_DELAY = 4;

    public void validateDrongoDBOptions() {
    }
}
