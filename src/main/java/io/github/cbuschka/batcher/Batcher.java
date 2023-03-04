package io.github.cbuschka.batcher;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Batcher<Key, Entity, Value> {
    private final Clock clock;
    private final ExecutorService asyncLoadExecutor;
    private final ScheduledExecutorService backgroundExecutor;
    private final Function<List<Key>, List<Entity>> loadFunction;
    private final Function<Entity, Key> keyFunction;
    private final Function<Entity, Value> valueFunction;
    private final int maxBatchSize;
    private final long maxBatchDelayMillis;
    private final Object LOCK = new Object();
    private Batch batch;

    public Batcher(Clock clock, int maxParallelLoadCount, Function<List<Key>, List<Entity>> loadFunction, Function<Entity, Key> keyFunction, Function<Entity, Value> valueFunction, int maxBatchSize, long maxBatchDelayMillis) {
        this.clock = clock;
        this.asyncLoadExecutor = Executors.newFixedThreadPool(maxParallelLoadCount);
        this.backgroundExecutor = Executors.newScheduledThreadPool(1);
        this.loadFunction = loadFunction;
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchDelayMillis = maxBatchDelayMillis;

        scheduleBatchTimeoutChecker();
    }

    private void scheduleBatchTimeoutChecker() {
        backgroundExecutor.schedule(() -> {
            synchronized (LOCK) {
                log.trace("Background check if batch must be loaded.");
                checkIfBatchMustBeLoaded();
            }

            scheduleBatchTimeoutChecker();
        }, 500, TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    public Future<Optional<Value>> get(Key key) {
        return getLoadedBatchWithKey(key)
                .thenApply((batch) -> batch.getValue(key));
    }

    private CompletableFuture<Batch> getLoadedBatchWithKey(Key key) {
        synchronized (LOCK) {
            if (batch == null) {
                batch = new Batch();
                log.trace("New batch={} created.", batch);
            }

            batch.add(key);
            log.trace("Added key={} to batch={}...", key, batch);
            CompletableFuture<Batch> loadFuture = batch.getLoadFuture();
            checkIfBatchMustBeLoaded();

            return loadFuture;
        }
    }

    private void checkIfBatchMustBeLoaded() {
        if (batch == null) {
            log.trace("No batch given.");
            return;
        }

        if (batch.keySize() >= maxBatchSize
                || batch.createdAtMillis < clock.millis() - maxBatchDelayMillis) {
            triggerBatchLoadAsync(batch);
            batch = null;
        } else {
            log.trace("No load neccessary.");
        }
    }

    private void triggerBatchLoadAsync(Batch batch) {
        log.trace("Scheduling async load of batch={}...", batch);
        asyncLoadExecutor.execute(() -> {
            try {
                batch.load();
                log.debug("Batch batch={} loaded. Triggering load future.", batch);
                batch.loadFuture.complete(batch);
            } catch (Exception ex) {
                log.debug("Loading batch batch={} failed. Triggering load future exceptionally.", batch);
                batch.loadFuture.completeExceptionally(ex);
            }
        });
    }

    public void shutdown() {
        try {
            @SuppressWarnings("unused")
            boolean ignored = this.asyncLoadExecutor.awaitTermination(maxBatchDelayMillis, TimeUnit.MILLISECONDS);
            this.asyncLoadExecutor.shutdown();
        } catch (Exception ex) {
            this.asyncLoadExecutor.shutdownNow();
        } finally {
            this.backgroundExecutor.shutdownNow();
        }
    }

    private class Batch {
        private final long createdAtMillis = clock.millis();
        private final CompletableFuture<Batch> loadFuture = new CompletableFuture<>();
        private final Set<Key> keys = new HashSet<>();
        private Map<Key, Value> values = new HashMap<>();

        public long getCreatedAtMillis() {
            return createdAtMillis;
        }

        public int keySize() {
            return keys.size();
        }

        public Optional<Value> getValue(Key key) {
            Value value = values.get(key);
            return Optional.ofNullable(value);
        }

        public void add(Key key) {
            keys.add(key);
        }

        private void load() {
            log.debug("Loading batch={} with keys={}...", this, keys);
            List<Key> keys = new ArrayList<>(this.keys);
            this.values = loadFunction.apply(keys)
                    .stream()
                    .collect(Collectors.toMap(keyFunction, valueFunction, (p, q) -> p));
        }

        public CompletableFuture<Batch> getLoadFuture() {
            return loadFuture;
        }

        @Override
        public String toString() {
            return String.format("Batch@%d{}size=%d}", System.identityHashCode(this), keySize());
        }
    }
}
