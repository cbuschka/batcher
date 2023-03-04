package io.github.cbuschka.batcher;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@AllArgsConstructor
class TimingsSummary {
    private long minGotFutureAfterMillis;
    private long avgGotFutureAfterMillis;
    private long maxGotFutureAfterMillis;
    private long minGotResultAfterMillis;
    private long avgGotResultAfterMillis;
    private long maxGotResultAfterMillis;
    private int found;
    private int notFound;

    private static long avg(long a, long b) {
        return (int) (((double) a + b) / 2.0d);
    }

    public static TimingsSummary combine(TimingsSummary s, Timings b) {
        if (s == null) {
            return new TimingsSummary(b.gotFutureAfterMillis, b.gotFutureAfterMillis, b.gotFutureAfterMillis,
                    b.gotResultAfterMillis, b.gotResultAfterMillis, b.gotResultAfterMillis,
                    b.found ? 1 : 0, b.found ? 0 : 1);
        }

        return new TimingsSummary(
                Math.min(s.minGotFutureAfterMillis, b.gotFutureAfterMillis),
                avg(s.avgGotFutureAfterMillis, b.gotFutureAfterMillis),
                Math.max(s.maxGotFutureAfterMillis, b.gotFutureAfterMillis),

                Math.min(s.minGotResultAfterMillis, b.gotResultAfterMillis),
                avg(s.avgGotResultAfterMillis, b.gotResultAfterMillis),
                Math.max(s.maxGotResultAfterMillis, b.gotResultAfterMillis),

                b.found ? s.found + 1 : s.found, b.found ? s.notFound : s.notFound + 1);
    }
}
