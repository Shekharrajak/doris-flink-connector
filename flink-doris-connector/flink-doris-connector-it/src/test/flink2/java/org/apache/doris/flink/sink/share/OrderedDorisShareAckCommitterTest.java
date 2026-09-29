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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class OrderedDorisShareAckCommitterTest {
    @Test
    public void testCommitsDorisBeforeShareAcks() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingCommitter<DorisCommittable> doris =
                new RecordingCommitter<>("doris", events);
        RecordingCommitter<ShareAckCommittable> shareAcks =
                new RecordingCommitter<>("share-ack", events);
        OrderedDorisShareAckCommitter committer =
                new OrderedDorisShareAckCommitter(doris, shareAcks);

        TestCommitRequest request = new TestCommitRequest(preparedCommittable());
        committer.commit(Collections.singletonList(request));

        Assert.assertEquals(List.of("doris", "share-ack"), events);
        Assert.assertNull(request.updatedCommittable);
        Assert.assertNull(request.failure);
    }

    @Test
    public void testShareAckRetryResumesAfterDorisCommit() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingCommitter<DorisCommittable> doris =
                new RecordingCommitter<>("doris", events);
        RecordingCommitter<ShareAckCommittable> shareAcks =
                new RecordingCommitter<>("share-ack", events);
        shareAcks.result = NestedResult.RETRY;
        OrderedDorisShareAckCommitter committer =
                new OrderedDorisShareAckCommitter(doris, shareAcks);

        TestCommitRequest firstAttempt = new TestCommitRequest(preparedCommittable());
        committer.commit(Collections.singletonList(firstAttempt));

        Assert.assertEquals(List.of("doris", "share-ack"), events);
        Assert.assertEquals(
                DorisShareAckCommitPhase.DORIS_COMMITTED,
                firstAttempt.updatedCommittable.getPhase());

        events.clear();
        shareAcks.result = NestedResult.SUCCESS;
        TestCommitRequest retry = new TestCommitRequest(firstAttempt.updatedCommittable);
        committer.commit(Collections.singletonList(retry));

        Assert.assertEquals(Collections.singletonList("share-ack"), events);
        Assert.assertNull(retry.updatedCommittable);
    }

    @Test
    public void testDorisRetryDoesNotCommitShareAcks() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingCommitter<DorisCommittable> doris =
                new RecordingCommitter<>("doris", events);
        doris.result = NestedResult.RETRY;
        RecordingCommitter<ShareAckCommittable> shareAcks =
                new RecordingCommitter<>("share-ack", events);
        OrderedDorisShareAckCommitter committer =
                new OrderedDorisShareAckCommitter(doris, shareAcks);

        TestCommitRequest request = new TestCommitRequest(preparedCommittable());
        committer.commit(Collections.singletonList(request));

        Assert.assertEquals(Collections.singletonList("doris"), events);
        Assert.assertEquals(
                DorisShareAckCommitPhase.PREPARED,
                request.updatedCommittable.getPhase());
    }

    private static DorisShareAckCommittable preparedCommittable() {
        return new DorisShareAckCommittable(
                DorisShareAckCommitPhase.PREPARED,
                Collections.singletonList(new DorisCommittable("fe:8040", "db", 101L)),
                Collections.singletonList(
                        DorisShareAckCommittableSerializerTest.shareAckCommittable()));
    }

    private enum NestedResult {
        SUCCESS,
        RETRY,
        FAILED_UNKNOWN
    }

    private static final class RecordingCommitter<T> implements Committer<T> {
        private final String name;
        private final List<String> events;
        private NestedResult result = NestedResult.SUCCESS;

        private RecordingCommitter(String name, List<String> events) {
            this.name = name;
            this.events = events;
        }

        @Override
        public void commit(Collection<CommitRequest<T>> requests) {
            for (CommitRequest<T> request : requests) {
                events.add(name);
                if (result == NestedResult.RETRY) {
                    request.retryLater();
                } else if (result == NestedResult.FAILED_UNKNOWN) {
                    request.signalFailedWithUnknownReason(new IOException(name));
                }
            }
        }

        @Override
        public void close() {}
    }

    private static final class TestCommitRequest
            implements Committer.CommitRequest<DorisShareAckCommittable> {
        private final DorisShareAckCommittable committable;
        @Nullable private DorisShareAckCommittable updatedCommittable;
        @Nullable private Throwable failure;

        private TestCommitRequest(DorisShareAckCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DorisShareAckCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void retryLater() {
            updatedCommittable = committable;
        }

        @Override
        public void updateAndRetryLater(DorisShareAckCommittable updated) {
            updatedCommittable = updated;
        }

        @Override
        public void signalAlreadyCommitted() {}
    }
}
