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
import org.apache.doris.flink.sink.writer.DorisWriterState;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.share.ShareAckRecord;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DorisShareAckWriterTest {
    @Test
    public void testPreparesDorisBeforeShareAcks() throws Exception {
        List<String> events = new ArrayList<>();
        TestDorisWriter dorisWriter = new TestDorisWriter(events);
        TestShareAckWriter shareAckWriter = new TestShareAckWriter(events);
        DorisShareAckWriter<String> writer =
                new DorisShareAckWriter<>(dorisWriter, dorisWriter, shareAckWriter);

        Collection<DorisShareAckCommittable> committables = writer.prepareCommit();

        Assert.assertEquals(List.of("doris.prepare", "share-ack.prepare"), events);
        Assert.assertEquals(1, committables.size());
        Assert.assertEquals(
                DorisShareAckCommitPhase.PREPARED,
                committables.iterator().next().getPhase());
    }

    @Test(expected = IOException.class)
    public void testRejectsCheckpointWithOnlyShareAcks() throws Exception {
        List<String> events = new ArrayList<>();
        TestDorisWriter dorisWriter = new TestDorisWriter(events);
        dorisWriter.committables = Collections.emptyList();
        DorisShareAckWriter<String> writer =
                new DorisShareAckWriter<>(
                        dorisWriter, dorisWriter, new TestShareAckWriter(events));

        writer.prepareCommit();
    }

    private static final class TestDorisWriter
            implements StatefulSinkWriter<String, DorisWriterState>,
                    CommittingSinkWriter<String, DorisCommittable> {
        private final List<String> events;
        private Collection<DorisCommittable> committables =
                Collections.singletonList(new DorisCommittable("fe:8040", "db", 101L));

        private TestDorisWriter(List<String> events) {
            this.events = events;
        }

        @Override
        public void write(String element, SinkWriter.Context context) {
            events.add("doris.write");
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<DorisCommittable> prepareCommit() {
            events.add("doris.prepare");
            return committables;
        }

        @Override
        public List<DorisWriterState> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void close() {}
    }

    private static final class TestShareAckWriter
            implements CommittingSinkWriter<ShareAckRecord, ShareAckCommittable> {
        private final List<String> events;

        private TestShareAckWriter(List<String> events) {
            this.events = events;
        }

        @Override
        public void write(ShareAckRecord element, SinkWriter.Context context) {
            events.add("share-ack.write");
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<ShareAckCommittable> prepareCommit() {
            events.add("share-ack.prepare");
            return Collections.singletonList(
                    DorisShareAckCommittableSerializerTest.shareAckCommittable());
        }

        @Override
        public void close() {}
    }
}
