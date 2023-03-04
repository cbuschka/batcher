package io.github.cbuschka.batcher;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@Slf4j
class BatcherTest {
    private Clock clock = Clock.systemUTC();
    private Batcher<Long, Item, String> batcher;
    private ItemRepo itemRepo;
    private ExecutorService executorService;

    @Test
    public void test() {
        int itemCount = 1005;
        int maxParallelLoadCount = 10;
        int maxBatchSize = 100;
        int maxBatchDelayMillis = 3000;
        long bulkLoadDelayMillis = 1000L;
        itemRepo = new ItemRepo(itemCount, bulkLoadDelayMillis);
        executorService = Executors.newFixedThreadPool(itemCount);
        batcher = new Batcher<>(clock,
                maxParallelLoadCount, itemRepo::findByIds,
                Item::getId, Item::getName,
                maxBatchSize, maxBatchDelayMillis);

        long startMillis = clock.millis();

        List<CompletableFuture<Timings>> futures = LongStream.range(1, itemCount)
                .mapToObj(this::scheduleItemRequest)
                .collect(Collectors.toList());
        futures.add(scheduleItemRequest(0L));

        List<Timings> timingsList = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        TimingsSummary summary = timingsList.stream()
                .reduce(null, TimingsSummary::combine, (p, q) -> p);
        long durationMillis = clock.millis() - startMillis;
        log.info("Total millis(s): {}", durationMillis);
        log.info("{}", summary);

    }

    private CompletableFuture<Timings> scheduleItemRequest(Long id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long startMillis = clock.millis();
                log.debug("Asking for item id={}...", id);
                Future<Optional<String>> future = batcher.get(id);
                long gotFutureMillis = clock.millis();
                log.debug("Got future for id={}. Waiting for value...", id);
                Optional<String> optItemName = future.get();
                log.debug("Got optItemName={} for id={}.", optItemName, id);
                long gotResultMillis = clock.millis();
                return new Timings(gotFutureMillis - startMillis, gotResultMillis - gotFutureMillis, optItemName.isPresent());
            } catch (InterruptedException | ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }, this.executorService);
    }
}