/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.runCql;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.utils.TestUtils;

public class CommitLogRealTimeParserTest extends AbstractCommitLogProcessorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogRealTimeParserTest.class);
    private CommitLogIdxProcessor commitLogProcessor;

    @Override
    public void assumeTestRuns() {
        assumeNotCassandra3();
    }

    @Before
    public void setUp() throws Throwable {
        super.setUp();
        commitLogProcessor = new CommitLogIdxProcessor(context, metrics,
                commitLogProcessing.getCommitLogSegmentReader(),
                Paths.get("target/data/cassandra/cdc_raw").toAbsolutePath().toFile());
    }

    @Override
    public Configuration getContextConfiguration() throws Throwable {
        Properties properties = TestUtils.generateDefaultConfigMap();
        properties.put(CassandraConnectorConfig.COMMIT_LOG_REAL_TIME_PROCESSING_ENABLED.name(), "true");
        properties.put(CassandraConnectorConfig.COMMIT_LOG_MARKED_COMPLETE_POLL_INTERVAL_IN_MS.name(), "1000");
        return Configuration.from(properties);
    }

    @Override
    public void initialiseData() {
        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, PRIMARY KEY(a)) WITH cdc = true;");
        insertRows(3, 10);
    }

    private void insertRows(int count, int keyInc) {
        for (int i = 0; i < count; i++) {
            runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                    .value("a", literal(i + keyInc))
                    .value("b", literal(i))
                    .build());
        }
        LOGGER.info("Inserted rows: {}", count);
    }

    @Override
    public void verifyEvents() {
        readLogs();
        verify(3, 10);
        insertRows(2, 20);
        verify(2, 20);
    }

    private void verify(int expectedEventsCount, int keyInc) {
        List<Event> events = new ArrayList<>();
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            events.addAll(context.getQueues().get(0).poll());
            return events.size() == expectedEventsCount;
        });

        LOGGER.info("Total events received: {}", events.size());
        Assert.assertEquals("Total number of events received must be " + expectedEventsCount, expectedEventsCount, events.size());

        for (int i = 0; i < expectedEventsCount; i++) {
            Record record = (Record) events.get(i);
            Record.Operation op = record.getOp();
            Assert.assertEquals("Operation type must be insert but it was " + op, Record.Operation.INSERT, op);
            Assert.assertEquals("Inserted key should be " + i + keyInc, record.getRowData().getPrimary().get(0).value, i + keyInc);
        }
    }

    private void readLogs() {
        // check to make sure there are no records in the queue to begin with
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
        File cdcLoc = Paths.get("target/data/cassandra/cdc_raw").toAbsolutePath().toFile();
        LOGGER.info("CDC Location: {}", cdcLoc);
        await().timeout(Duration.ofSeconds(3)).until(() -> CommitLogUtil.getIndexes(cdcLoc).length >= 1);
        File[] commitLogIndexes = CommitLogUtil.getIndexes(cdcLoc);
        Arrays.sort(commitLogIndexes, (file1, file2) -> CommitLogUtil.compareCommitLogsIndexes(file1, file2));
        Assert.assertTrue("At least one idx file must be generated", commitLogIndexes.length >= 1);
        // Submitting the last idx file as that one is generated by current test
        commitLogProcessor.submit(commitLogIndexes[commitLogIndexes.length - 1].toPath());
    }

}