package local.sd.test;

import local.sd.test.redis.RedisList;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class Main {

    private static final int NUMBER_OF_RECORDS = 10000;
    private static final boolean USE_BATCH = true;
    private static final int BATCH_SIZE = 200; // For writing
    private static final int RANGE_SIZE = 200; // For reading
    private static final int THREADS = 40; // For reading
    private static final String HOST = ""; // FILL IT!!!
    private static final int PORT = 6379;

    public static void main(String[] args) throws IOException {

        String payload = new Payload().getJson();

        testSimpleRedisList(payload);

        testRedisListWithTrim(payload);
    }

    private static void testRedisListWithTrim(String payload) {
        log.info("Test Redis List with Range + Trim");
        try (RedisList redisList = new RedisList(HOST, PORT, BATCH_SIZE)) {
            redisList.clear();
            writeData(payload, redisList);
            log.info("Current list length: {}", redisList.size());
            long start = System.currentTimeMillis();
            int recordsCount = redisList.readRangesWithTrim(NUMBER_OF_RECORDS, RANGE_SIZE);
            long finish = System.currentTimeMillis();
            log.info("Read {} records for {} ms", recordsCount, finish - start);
            log.info("Current list length: {}", redisList.size());
        }
    }

    private static void writeData(String payload, RedisList redisList) {
        long start = System.currentTimeMillis();
        redisList.writeData(payload, NUMBER_OF_RECORDS, USE_BATCH);
        long finish = System.currentTimeMillis();
        log.info("Write {} records for {} ms", NUMBER_OF_RECORDS, finish - start);
    }

    private static void testSimpleRedisList(String payload) {
        log.info("Test Redis List with simple push + pop");
        try (RedisList redisList = new RedisList(HOST, PORT, BATCH_SIZE)) {
            redisList.clear();
            writeData(payload, redisList);
            long start = System.currentTimeMillis();
            int recordsCount = redisList.readData(NUMBER_OF_RECORDS, THREADS);
            long finish = System.currentTimeMillis();
            log.info("Read {} records for {} ms", recordsCount, finish - start);
        }
    }
}
