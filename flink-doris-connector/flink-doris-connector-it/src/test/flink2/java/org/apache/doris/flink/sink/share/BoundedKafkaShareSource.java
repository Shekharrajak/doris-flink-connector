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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.share.ShareAckDecision;
import org.apache.flink.connector.kafka.share.ShareAckId;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.share.ShareAckRecord;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

final class BoundedKafkaShareSource extends RichParallelSourceFunction<DorisShareAckRecord<String>>
        implements CheckpointedFunction {
    private static final String SHARE_ACK_MODE_CONFIG = "share.acknowledgement.mode";
    private static final String SHARE_ACQUIRE_MODE_CONFIG = "share.acquire.mode";

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final long expectedRecords;

    private transient ListState<Long> emittedState;
    private transient KafkaShareConsumer<byte[], byte[]> consumer;
    private long emittedRecords;
    private volatile boolean running = true;

    BoundedKafkaShareSource(
            String bootstrapServers, String groupId, String topic, long expectedRecords) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.expectedRecords = expectedRecords;
    }

    @Override
    public void run(SourceContext<DorisShareAckRecord<String>> context) {
        Properties properties = consumerProperties();
        consumer =
                new KafkaShareConsumer<>(
                        properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(topic));
        try {
            while (running && emittedRecords < expectedRecords) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : polled) {
                    synchronized (context.getCheckpointLock()) {
                        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        ShareAckRecord acknowledgement = acknowledgement(record);
                        context.collect(
                                new DorisShareAckRecord<>(
                                        new String(record.value(), StandardCharsets.UTF_8),
                                        acknowledgement));
                        emittedRecords++;
                    }
                    if (!running || emittedRecords == expectedRecords) {
                        break;
                    }
                }
            }
        } finally {
            consumer.close(Duration.ZERO);
            consumer = null;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        emittedState.update(Collections.singletonList(emittedRecords));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        emittedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "kafka-share-emitted-records",
                                        TypeInformation.of(Long.class)));
        if (context.isRestored()) {
            Iterator<Long> restored = emittedState.get().iterator();
            emittedRecords = restored.hasNext() ? restored.next() : 0L;
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(SHARE_ACK_MODE_CONFIG, "explicit");
        properties.put(SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        return properties;
    }

    private ShareAckRecord acknowledgement(ConsumerRecord<byte[], byte[]> record) {
        ShareAckPayload payload =
                ShareAckPayload.fromKafkaObjects(
                        UUID.randomUUID().toString(),
                        invoke(consumer, "acknowledgementsForTransaction"),
                        invoke(consumer, "shareGroupMetadata"));
        ShareAckPayload.TopicPartitionAcknowledgements partition =
                payload.getAcknowledgements().stream()
                        .filter(
                                candidate ->
                                        candidate.getTopic().equals(record.topic())
                                                && candidate.getPartition() == record.partition())
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No transactional acknowledgement for "
                                                        + record.topic()
                                                        + "-"
                                                        + record.partition()));
        ShareAckId id =
                new ShareAckId(
                        payload.getGroupId(),
                        partition.getTopicId(),
                        record.topic(),
                        record.partition(),
                        record.offset());
        return new ShareAckRecord(
                id, payload.getMemberId(), payload.getMemberEpoch(), ShareAckDecision.ACCEPT);
    }

    private static Object invoke(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Kafka object does not expose " + methodName + "()", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalArgumentException("Failed to invoke " + methodName + "()", cause);
        }
    }
}
