package io.github.cbuschka.batcher;

public interface BatcherListener {
    default void batchCreated(long time, int batchId) {
    }

    default void keyAdded(long time, int batchId, long batchAgeMillis, int batchSize, Object key) {
    }

    default void batchLoadStarted(long time, int batchId, long batchAgeMillis, int batchSize) {
    }

    default void batchLoadCompleted(long time, int batchId) {
    }

    default void batchLoadFailed(long time, int batchId, Throwable ex) {
    }

    default void backgroundCheckTriggered(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
    }

    default void shutdownStarted(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
    }

    default void shutdownCompleted(long time, Integer batchId, Long batchAgeMillis, Integer batchSize) {
    }
}
