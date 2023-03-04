package io.github.cbuschka.batcher;

public class Metrics {
    private LongValue batchAgeMillis = new LongValue();
    private IntValue batchSize = new IntValue();
    private LongValue loadDurationMillis = new LongValue();

    public boolean isEmpty() {
        return this.batchSize.isEmpty()
                || this.batchAgeMillis.isEmpty()
                || this.loadDurationMillis.isEmpty();
    }

    public void reset() {
        this.batchSize.reset();
        this.batchAgeMillis.reset();
        this.batchAgeMillis.reset();
    }

    public long getAvgBatchAgeMillis() {
        return batchAgeMillis.avg();
    }

    public int getAvgBatchSize() {
        return batchSize.avg();
    }

    public long getAvgLoadDurationMillis() {
        return loadDurationMillis.avg();
    }

    public void recordBatchSize(int batchSize) {
        this.batchSize.record(batchSize);
    }

    public void recordBatchAgeMillis(long batchAgeMillis) {
        this.batchAgeMillis.record(batchAgeMillis);
    }

    public void recordLoadDurationMillis(long loadDurationMillis) {
        this.loadDurationMillis.record(loadDurationMillis);
    }

    private static class LongValue {
        private long avg;

        public synchronized void record(long x) {
            if (avg == 0) {
                avg = x;
            } else {
                avg = (avg + x) / 2;
            }
        }

        public synchronized long avg() {
            return avg;
        }

        public synchronized void reset() {
            this.avg = 0;
        }

        public synchronized boolean isEmpty() {
            return this.avg == 0L;
        }
    }


    private static class IntValue {
        private int avg;

        public synchronized void record(int x) {
            if (avg == 0) {
                avg = x;
            } else {
                avg = (avg + x) / 2;
            }
        }

        public synchronized int avg() {
            return avg;
        }

        public synchronized void reset() {
            this.avg = 0;
        }

        public synchronized boolean isEmpty() {
            return this.avg == 0;
        }
    }
}
