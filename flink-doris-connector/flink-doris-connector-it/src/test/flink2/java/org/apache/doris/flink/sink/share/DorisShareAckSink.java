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

import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisWriterState;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.kafka.share.KafkaShareAckSink;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.share.ShareAckRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

final class DorisShareAckSink<IN>
        implements Sink<DorisShareAckRecord<IN>>,
                SupportsWriterState<DorisShareAckRecord<IN>, DorisWriterState>,
                SupportsCommitter<DorisShareAckCommittable> {
    private final DorisSink<IN> dorisSink;
    private final KafkaShareAckSink shareAckSink;

    DorisShareAckSink(DorisSink<IN> dorisSink, KafkaShareAckSink shareAckSink) {
        this.dorisSink = Objects.requireNonNull(dorisSink, "dorisSink");
        this.shareAckSink = Objects.requireNonNull(shareAckSink, "shareAckSink");
    }

    @Override
    public SinkWriter<DorisShareAckRecord<IN>> createWriter(WriterInitContext context)
            throws IOException {
        return createWriter(dorisSink.createWriter(context), shareAckSink.createWriter(context));
    }

    @Override
    public StatefulSinkWriter<DorisShareAckRecord<IN>, DorisWriterState> restoreWriter(
            WriterInitContext context, Collection<DorisWriterState> recoveredState)
            throws IOException {
        return createWriter(
                dorisSink.restoreWriter(context, recoveredState),
                shareAckSink.createWriter(context));
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return dorisSink.getWriterStateSerializer();
    }

    @Override
    public Committer<DorisShareAckCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new OrderedDorisShareAckCommitter(
                castDorisCommitter(dorisSink.createCommitter(context)),
                shareAckSink.createCommitter(context));
    }

    @Override
    public SimpleVersionedSerializer<DorisShareAckCommittable> getCommittableSerializer() {
        return new DorisShareAckCommittableSerializer(
                castDorisSerializer(dorisSink.getCommittableSerializer()),
                shareAckSink.getCommittableSerializer());
    }

    private DorisShareAckWriter<IN> createWriter(
            SinkWriter<IN> dorisWriter, SinkWriter<ShareAckRecord> shareAckWriter) {
        return new DorisShareAckWriter<>(
                castDorisStatefulWriter(dorisWriter),
                castDorisCommittingWriter(dorisWriter),
                castShareAckWriter(shareAckWriter));
    }

    @SuppressWarnings("unchecked")
    private static <IN> StatefulSinkWriter<IN, DorisWriterState> castDorisStatefulWriter(
            SinkWriter<IN> writer) {
        return (StatefulSinkWriter<IN, DorisWriterState>) writer;
    }

    @SuppressWarnings("unchecked")
    private static <IN> CommittingSinkWriter<IN, DorisCommittable>
            castDorisCommittingWriter(SinkWriter<IN> writer) {
        return (CommittingSinkWriter<IN, DorisCommittable>) writer;
    }

    @SuppressWarnings("unchecked")
    private static CommittingSinkWriter<ShareAckRecord, ShareAckCommittable> castShareAckWriter(
            SinkWriter<ShareAckRecord> writer) {
        return (CommittingSinkWriter<ShareAckRecord, ShareAckCommittable>) writer;
    }

    @SuppressWarnings("unchecked")
    private static Committer<DorisCommittable> castDorisCommitter(Committer<?> committer) {
        return (Committer<DorisCommittable>) committer;
    }

    @SuppressWarnings("unchecked")
    private static SimpleVersionedSerializer<DorisCommittable> castDorisSerializer(
            SimpleVersionedSerializer<?> serializer) {
        return (SimpleVersionedSerializer<DorisCommittable>) serializer;
    }
}
