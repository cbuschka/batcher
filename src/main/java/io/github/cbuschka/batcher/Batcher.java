package io.github.cbuschka.batcher;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Batcher<Key, Entity, Value> {
    private static final AtomicInteger idSeq = new AtomicInteger(0);
    private final Clock clock;
    private final ExecutorService asyncLoadExecutor;
    private final ScheduledExecutorService backgroundExecutor;
    private final Function<List<Key>, List<Entity>> loadFunction;
    private final Function<Entity, Key> keyFunction;
    private final Function<Entity, Value> valueFunction;
    private final int maxBatchSize;
    private final long maxBatchDelayMillis;
    private final Object LOCK = new Object();
    private final BatcherListener listener;
    private Batch batch;
    private boolean shutdown = false;

    public Batcher(Clock clock, int maxParallelLoadCount, Function<List<Key>, List<Entity>> loadFunction, Function<Entity, Key> keyFunction, Function<Entity, Value> valueFunction, int maxBatchSize, long maxBatchDelayMillis,
                   BatcherListener listener) {
        this.clock = clock;
        this.asyncLoadExecutor = Executors.newFixedThreadPool(maxParallelLoadCount);
        this.backgroundExecutor = Executors.newScheduledThreadPool(1);
        this.loadFunction = loadFunction;
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchDelayMillis = maxBatchDelayMillis;
        this.listener = (listener == null) ? new BatcherListener() {
        } : listener;

        scheduleBatchTimeoutChecker();
    }

    private void scheduleBatchTimeoutChecker() {
        backgroundExecutor.schedule(() -> {
            synchronized (LOCK) {
                fireBackgroundCheckTriggered();
                log.trace("Background check if batch must be loaded.");
                checkIfBatchMustBeLoaded(false);
                if (!shutdown) {
                    scheduleBatchTimeoutChecker();
                }
            }

        }, Math.max(maxBatchDelayMillis / 2, 100), TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    public Optional<Value> waitAndGet(Key key) {
        return get(key).get();
    }

    public Future<Optional<Value>> get(Key key) {
        return getLoadedBatchWithKey(key)
                .thenApply((batch) -> batch.getValue(key));
    }

    private CompletableFuture<Batch> getLoadedBatchWithKey(Key key) {
        synchronized (LOCK) {
            checkNotShutdownYet();

            if (batch == null) {
                batch = new Batch();
                listener.batchCreated(clock.millis(), batch.id);
                log.trace("New batch={} created.", batch);
            }

            batch.add(key);
            fireKeyAdded(key);
            log.trace("Added key={} to batch={}...", key, batch);
            CompletableFuture<Batch> loadFuture = batch.getLoadFuture();
            checkIfBatchMustBeLoaded(false);

            return loadFuture;
        }
    }

    private void checkNotShutdownYet() {
        if (shutdown) {
            throw new IllegalStateException("Batcher already shutdown.");
        }
    }


    private void checkIfBatchMustBeLoaded(boolean force) {
        if (batch == null) {
            log.trace("No batch given.");
            return;
        }

        if (force || batch.keySize() >= maxBatchSize
                || batch.createdAtMillis < clock.millis() - maxBatchDelayMillis) {
            startAsyncBatchLoad(batch);
            batch = null;
        } else {
            log.trace("No load neccessary.");
        }
    }

    private void startAsyncBatchLoad(Batch batch) {
        fireBatchLoadTriggered(batch);
        log.trace("Scheduling async load of batch={}...", batch);
        asyncLoadExecutor.execute(new BatchLoadJob(batch));
    }

    @AllArgsConstructor
    private class BatchLoadJob implements Runnable {
        private Batch batch;

        @Override
        public void run() {
            try {
                batch.load();
                Batcher.this.listener.batchLoadCompleted(clock.millis(), batch.id);
                log.debug("Batch batch={} loaded. Triggering load future.", batch);
                batch.loadFuture.complete(batch);
            } catch (Exception ex) {
                long now = clock.millis();
                Batcher.this.listener.batchLoadFailed(now, batch.id, ex);
                log.debug("Loading batch batch={} failed. Triggering load future exceptionally.", batch);
                batch.loadFuture.completeExceptionally(ex);
            }
        }
    }

    public void shutdown() {
        synchronized (LOCK) {
            if (shutdown) {
                return;
            }

            shutdown = true;
            checkIfBatchMustBeLoaded(true);
            fireShutdownTriggered();
        }

        try {
            @SuppressWarnings("unused")
            boolean ignored = this.asyncLoadExecutor.awaitTermination(maxBatchDelayMillis, TimeUnit.MILLISECONDS);
            this.asyncLoadExecutor.shutdownNow();
        } catch (Exception ex) {
            this.asyncLoadExecutor.shutdownNow();
        } finally {
            this.backgroundExecutor.shutdownNow();
        }

        synchronized (LOCK) {
            fireShutdownCompleted();
        }
    }

    private class Batch {
        private final int id = idSeq.getAndIncrement();
        private final long createdAtMillis = clock.millis();
        private final CompletableFuture<Batch> loadFuture = new CompletableFuture<>();
        private final Set<Key> keys = new HashSet<>();
        private Map<Key, Value> values = new HashMap<>();

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
                    .collect(Collectors.toUnmodifiableMap(keyFunction, valueFunction, (p, q) -> p));
        }

        public CompletableFuture<Batch> getLoadFuture() {
            return loadFuture;
        }

        @Override
        public String toString() {
            return String.format("Batch@%d{}size=%d}", System.identityHashCode(this), keySize());
        }
    }


    private void fireBackgroundCheckTriggered() {
        long now = clock.millis();
        listener.backgroundCheckTriggered(now,
                batch != null ? batch.id : null,
                batch != null ? now - batch.createdAtMillis : null,
                batch != null ? batch.keySize() : null);
    }


    private void fireKeyAdded(Key key) {
        long now = clock.millis();
        listener.keyAdded(now, batch.id, now - batch.createdAtMillis, batch.keySize(), key);
    }

    private void fireBatchLoadTriggered(Batch batch) {
        long now = clock.millis();
        listener.batchLoadStarted(clock.millis(), batch.id, now - batch.createdAtMillis, batch.keySize());
    }

    private void fireShutdownTriggered() {
        long now = clock.millis();
        listener.shutdownStarted(now,
                batch != null ? batch.id : null,
                batch != null ? now - batch.createdAtMillis : null,
                batch != null ? batch.keySize() : null);
    }

    private void fireShutdownCompleted() {
        long now = clock.millis();
        listener.shutdownCompleted(now,
                batch != null ? batch.id : null,
                batch != null ? now - batch.createdAtMillis : null,
                batch != null ? batch.keySize() : null);
    }

}
