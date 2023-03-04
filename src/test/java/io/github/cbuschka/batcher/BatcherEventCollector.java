package io.github.cbuschka.batcher;

import java.time.Clock;
import java.util.*;

public class BatcherEventCollector implements BatcherListener {
    private long startMillis;
    private final List<BatcherEvent> events = Collections.synchronizedList(new ArrayList<>());

    public BatcherEventCollector(Clock clock) {
        this.startMillis = clock.millis();
    }

    @Override
    public void batchCreated(long time, int batchId) {
        events.add(new BatcherEvent(BatcherEvent.Type.BATCH_CREATED,
                time - startMillis, batchId, null, null, null, null));
    }

    @Override
    public void keyAdded(long time, int batchId, long batchAgeMillis, int batchSize, Object key) {
        events.add(new BatcherEvent(BatcherEvent.Type.KEY_ADDED,
                time - startMillis, batchId, batchAgeMillis, batchSize, key, null));
    }

    @Override
    public void batchLoadStarted(long time, int batchId, long batchAgeMillis, int batchSize) {
        events.add(new BatcherEvent(BatcherEvent.Type.BATCH_LOAD_STARTED,
                time - startMillis, batchId, batchAgeMillis, batchSize, null, null));
    }

    @Override
    public void batchLoadCompleted(long time, int batchId) {
        events.add(new BatcherEvent(BatcherEvent.Type.BATCH_LOAD_COMPLETED,
                time - startMillis, batchId, null, null, null, null));
    }

    @Override
    public void batchLoadFailed(long time, int batchId, Throwable ex) {
        events.add(new BatcherEvent(BatcherEvent.Type.BATCH_LOAD_FAILED,
                time - startMillis, batchId, null, null, null, ex));
    }

    @Override
    public void backgroundCheckTriggered(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
        events.add(new BatcherEvent(BatcherEvent.Type.BACKGROUND_CHECK_TRIGGERED,
                time - startMillis, batchId, batchAgeMillis, batchSize, null, null));
    }


    @Override
    public void shutdownStarted(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
        events.add(new BatcherEvent(BatcherEvent.Type.SHUTDOWN_TRIGGERED,
                time - startMillis, batchId, batchAgeMillis, batchSize, null, null));
    }

    @Override
    public void shutdownCompleted(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
        events.add(new BatcherEvent(BatcherEvent.Type.SHUTDOWN_COMPLETED,
                time - startMillis, batchId, batchAgeMillis, batchSize, null, null));
    }

    public List<BatcherEvent> getEvents() {
        return Collections.unmodifiableList(events);
    }
}
