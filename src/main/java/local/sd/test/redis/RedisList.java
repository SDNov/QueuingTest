package local.sd.test.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RedisList implements Closeable {

    private static final String LIST_NAME = "records";
    private static final String GET_CONNECTION_FROM_REDIS_CLIENT = "Get connection from Redis client";
    private RedisClient redisClient;
    private int batchSize;
    private int loggingAccuracy = 500;

    public RedisList(String host, int port, int batchSize) {
        log.info("Create RedisClient");
        redisClient = new RedisClient(
                RedisURI.create(String.format("redis://%s:%d", host, port)));
        this.batchSize = batchSize;
    }

    public void writeData(String payload, int numberRecords, boolean useBatch) {
        if (useBatch) {
            log.info("Write {} records to '{}' with batch. Batch size: {}", numberRecords, LIST_NAME, batchSize);
            writeWithBatch(payload, numberRecords);
        } else {
            log.info("Write to '{}' without batch", LIST_NAME);
            writeWithoutBatch(payload, numberRecords);
        }
    }

    private void writeWithoutBatch(String payload, int numberRecords) {
        log.info(GET_CONNECTION_FROM_REDIS_CLIENT);
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            log.info("Writing...");
            for (int i = 1; i <= numberRecords; i++) {
                connection.rpush("sd", payload);
                if (i % loggingAccuracy == 0) {
                    log.info("Write {} records", i);
                }
            }
        }
    }

    private void writeWithBatch(String payload, int numberRecords) {
        log.info(GET_CONNECTION_FROM_REDIS_CLIENT);
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            int batchLength = 0;
            connection.multi();
            for (int i = 1; i <= numberRecords; i++) {
                connection.rpush(LIST_NAME, payload);
                batchLength++;
                if (batchLength == batchSize) {
                    connection.exec();
                    log.info("Write {} records", i);
                    batchLength = 0;
                    connection.multi();
                }
            }
            connection.exec();
        }
    }

    public int readData(int size, int threads) {
        log.info("Read data from redis in {} thread(s)", threads);
        AtomicInteger records = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        log.info(GET_CONNECTION_FROM_REDIS_CLIENT);
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            log.info("Reading...");
            for (int thr = 0; thr < threads; thr++) {
                executorService.execute(() -> {
                    do {
                        if (connection.lpop(LIST_NAME) != null) {
                            int currentRecords = records.incrementAndGet();
                            if (currentRecords % loggingAccuracy == 0) {
                                log.info("Read {} records", currentRecords);
                            }
                        }
                    } while (records.get() < size);
                });
            }
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
        return records.get();
    }

    public void clear() {
        log.info(GET_CONNECTION_FROM_REDIS_CLIENT);
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            log.info("Delete '{}' list", LIST_NAME);
            connection.del(LIST_NAME);
        }
    }


    public int readRangesWithTrim(int numberOfRecords, int rangeSize) {
        log.info(GET_CONNECTION_FROM_REDIS_CLIENT);
        int recordCount = 0;
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            log.info("Reading from {} with batch size: {}", LIST_NAME, rangeSize);
            while (recordCount < numberOfRecords) {
                final List<String> records = connection.lrange(LIST_NAME, 0, (long) (rangeSize - 1));
                recordCount += records.size();
                log.info("Read {} records", recordCount);
                connection.ltrim(LIST_NAME, rangeSize, -1);
            }
        }
        return recordCount;
    }

    public long size() {
        Long listLength;
        try (RedisConnection<String, String> connection = redisClient.connect()) {
            listLength = connection.llen(LIST_NAME);
        }
        return listLength;
    }

    @Override
    public void close() {
        redisClient.shutdown();
    }
}
