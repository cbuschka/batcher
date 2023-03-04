package io.github.cbuschka.batcher;

import lombok.AllArgsConstructor;

@AllArgsConstructor
class Timings {
    long gotFutureAfterMillis;
    long gotResultAfterMillis;
    boolean found;
}
