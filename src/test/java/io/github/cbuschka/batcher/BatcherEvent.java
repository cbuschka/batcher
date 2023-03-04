package io.github.cbuschka.batcher;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@AllArgsConstructor
@Getter
public class BatcherEvent {
    enum Type {
        BATCH_CREATED, KEY_ADDED, BATCH_LOAD_STARTED, BATCH_LOAD_COMPLETED, BATCH_LOAD_FAILED, BACKGROUND_CHECK_TRIGGERED, SHUTDOWN_TRIGGERED, SHUTDOWN_COMPLETED;
    }

    private Type type;
    private long time;
    private Integer batchId;
    private Long batchAgeInMillis;
    private Integer batchSize;
    private Object key;
    private Throwable ex;

}
