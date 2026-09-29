// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.share;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.share.KafkaShareAckSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KafkaShareToDorisEosITCase {
    private static final String DATABASE = "kafka_share_eos";
    private static final int RECORD_COUNT = 100;

    @Test
    public void testEveryKafkaShareIdentityBecomesOneVisibleDorisRow() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("kafka.share.doris.eos.it.enabled"));

        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String topic = "share-eos-input-" + suffix;
        String groupId = "share-eos-group-" + suffix;
        String table = "identity_audit_" + suffix;
        List<String> expectedIdentities = identities(suffix);

        String bootstrapServers = requiredProperty("kafka.bootstrap.servers");
        String jdbcUrl = requiredProperty("doris.jdbc.url");
        String username = System.getProperty("doris.username", "root");
        String password = System.getProperty("doris.password", "");

        prepareKafka(bootstrapServers, topic, groupId, expectedIdentities);
        prepareDoris(jdbcUrl, username, password, table);

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(
                Long.getLong("kafka.share.doris.eos.checkpoint.ms", 1_000L));

        DorisSink<String> dorisSink = createDorisSink(suffix, table, username, password);
        KafkaShareAckSink shareAckSink = createShareAckSink(bootstrapServers, suffix);
        environment
                .addSource(
                        new BoundedKafkaShareSource(
                                bootstrapServers, groupId, topic, RECORD_COUNT))
                .name("bounded-kafka-share-source")
                .setParallelism(1)
                .sinkTo(new DorisShareAckSink<>(dorisSink, shareAckSink))
                .name("doris-then-kafka-share-ack")
                .setParallelism(1);

        JobClient jobClient = environment.executeAsync("Kafka share group to Doris EOS proof");
        jobClient.getJobExecutionResult().get(5, TimeUnit.MINUTES);
        Assert.assertEquals(JobStatus.FINISHED, jobClient.getJobStatus().get());

        long visibleDeadlineMs =
                System.currentTimeMillis()
                        + Long.getLong("kafka.share.doris.eos.visible.timeout.ms", 120_000L);
        Map<String, Integer> visibleIdentities =
                awaitVisibleIdentities(
                        jdbcUrl, username, password, table, RECORD_COUNT, visibleDeadlineMs);
        assertExactlyOnce(expectedIdentities, visibleIdentities);
    }

    private static DorisSink<String> createDorisSink(
            String suffix, String table, String username, String password) {
        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty("format", "json");
        streamLoadProperties.setProperty("read_json_by_line", "true");

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(requiredProperty("doris.fenodes"))
                        .setBenodes(requiredProperty("doris.benodes"))
                        .setAutoRedirect(false)
                        .setTableIdentifier(DATABASE + "." + table)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("share-eos-" + suffix)
                        .enable2PC()
                        .setCheckInterval(500)
                        .setMaxRetries(5)
                        .setStreamLoadProp(streamLoadProperties)
                        .build();

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions)
                .build();
    }

    private static KafkaShareAckSink createShareAckSink(
            String bootstrapServers, String suffix) {
        return KafkaShareAckSink.builder()
                .setBootstrapServers(bootstrapServers)
                .setShareExactlyOnceId("doris-share-eos-" + suffix)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setProperty(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, "true")
                .build();
    }

    private static void prepareKafka(
            String bootstrapServers,
            String topic,
            String groupId,
            Collection<String> identities)
            throws Exception {
        Properties adminProperties = new Properties();
        adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(adminProperties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, groupId);
            AlterConfigOp resetToEarliest =
                    new AlterConfigOp(
                            new ConfigEntry("share.auto.offset.reset", "earliest"),
                            AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(group, List.of(resetToEarliest))).all().get();
        }

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties)) {
            long producedAtMs = System.currentTimeMillis();
            for (String identity : identities) {
                String value =
                        "{\"source_id\":\""
                                + identity
                                + "\",\"payload\":\"value-"
                                + identity
                                + "\",\"produced_at_ms\":"
                                + producedAtMs
                                + "}";
                producer.send(
                        new ProducerRecord<>(
                                topic,
                                identity.getBytes(StandardCharsets.UTF_8),
                                value.getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();
        }
    }

    private static void prepareDoris(
            String jdbcUrl, String username, String password, String table) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
            statement.execute(
                    "CREATE TABLE "
                            + DATABASE
                            + "."
                            + table
                            + " ("
                            + "source_id VARCHAR(128) NOT NULL,"
                            + "payload VARCHAR(256),"
                            + "produced_at_ms BIGINT"
                            + ") DUPLICATE KEY(source_id) "
                            + "DISTRIBUTED BY HASH(source_id) BUCKETS 1 "
                            + "PROPERTIES (\"replication_num\" = \"1\")");
        }
    }

    private static Map<String, Integer> awaitVisibleIdentities(
            String jdbcUrl,
            String username,
            String password,
            String table,
            int expectedCount,
            long deadlineMs)
            throws Exception {
        Map<String, Integer> identities = new HashMap<>();
        while (System.currentTimeMillis() < deadlineMs) {
            identities = readVisibleIdentities(jdbcUrl, username, password, table);
            int rowCount = identities.values().stream().mapToInt(Integer::intValue).sum();
            if (rowCount >= expectedCount) {
                return identities;
            }
            Thread.sleep(Duration.ofMillis(250).toMillis());
        }
        return identities;
    }

    private static Map<String, Integer> readVisibleIdentities(
            String jdbcUrl, String username, String password, String table) throws Exception {
        Map<String, Integer> identities = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT source_id, COUNT(*) FROM "
                                        + DATABASE
                                        + "."
                                        + table
                                        + " GROUP BY source_id")) {
            while (result.next()) {
                identities.put(result.getString(1), result.getInt(2));
            }
        }
        return identities;
    }

    private static void assertExactlyOnce(
            Collection<String> expectedIdentities, Map<String, Integer> actualIdentities) {
        Set<String> missing = new HashSet<>(expectedIdentities);
        missing.removeAll(actualIdentities.keySet());
        List<String> duplicates = new ArrayList<>();
        actualIdentities.forEach(
                (identity, count) -> {
                    if (count != 1) {
                        duplicates.add(identity + "=" + count);
                    }
                });
        Assert.assertTrue("Missing Doris identities: " + missing, missing.isEmpty());
        Assert.assertTrue("Duplicate Doris identities: " + duplicates, duplicates.isEmpty());
        Assert.assertEquals(
                "Unexpected Doris identities",
                new HashSet<>(expectedIdentities),
                actualIdentities.keySet());
    }

    private static List<String> identities(String suffix) {
        List<String> identities = new ArrayList<>();
        for (int index = 0; index < RECORD_COUNT; index++) {
            identities.add(String.format("%s-%04d", suffix, index));
        }
        return identities;
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing system property " + name);
        }
        return value;
    }
}
