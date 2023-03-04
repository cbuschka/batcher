package io.github.cbuschka.batcher;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class BatcherTest {
    private Clock clock = Clock.systemUTC();
    private Batcher<Long, Item, String> batcher;
    private ItemRepo itemRepo;
    private ExecutorService executorService;

    @Test
    public void test() {
        int itemCount = 10_000;
        int maxParallelLoadCount = 9;
        int maxBatchSize = 100;
        int maxBatchDelayMillis = 500;
        long bulkLoadDelayMillis = 30L;
        itemRepo = new ItemRepo(itemCount, bulkLoadDelayMillis);
        executorService = Executors.newFixedThreadPool(Math.min(itemCount, 1000));
        BatcherEventCollector collector = new BatcherEventCollector(clock);
        batcher = new Batcher<>(clock,
                maxParallelLoadCount, itemRepo::findByIds,
                Item::getId, Item::getName,
                maxBatchSize, maxBatchDelayMillis, collector, true);

        long startMillis = clock.millis();

        List<CompletableFuture<Timings>> futures = LongStream.range(1, itemCount + 1)
                .mapToObj(this::scheduleItemRequest)
                .collect(Collectors.toList());
        futures.add(scheduleItemRequest(0L));

        List<Timings> timingsList = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        batcher.shutdown();
        executorService.shutdownNow();

        TimingsSummary summary = timingsList.stream()
                .reduce(null, TimingsSummary::combine, (p, q) -> p);
        long durationMillis = clock.millis() - startMillis;
        log.info("Total millis(s): {}", durationMillis);
        log.info("{}", summary);

        assertThat(summary.getFound()).isEqualTo(itemCount);
        assertThat(summary.getNotFound()).isEqualTo(1);

        // collector.getEvents().forEach(System.err::println);
    }

    private CompletableFuture<Timings> scheduleItemRequest(Long id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long startMillis = clock.millis();
                log.debug("Asking for item id={}...", id);
                Optional<String> optItemName = batcher.waitAndGet(id);
                log.debug("Got optItemName={} for id={}.", optItemName, id);
                long gotResultMillis = clock.millis();
                return new Timings(gotResultMillis - startMillis, optItemName.isPresent());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, this.executorService);
    }
}