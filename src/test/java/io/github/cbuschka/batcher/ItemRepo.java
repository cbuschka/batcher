package io.github.cbuschka.batcher;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
class ItemRepo {

    private final Map<Long, Item> testData;
    private final long bulkLoadDelayMillis;

    public ItemRepo(int itemCount, long bulkLoadDelayMillis) {
        this.bulkLoadDelayMillis = bulkLoadDelayMillis;
        this.testData = IntStream.range(1, itemCount + 1)
                .mapToObj((i) -> new Item((long) i, "Item#" + i))
                .collect(Collectors.toMap(Item::getId, p -> p, (p, q) -> p));
    }

    @SneakyThrows
    public List<Item> findByIds(List<Long> ids) {
        log.debug("Finding by ids={}...", ids);
        Thread.sleep(this.bulkLoadDelayMillis);
        return ids
                .stream().map(testData::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
