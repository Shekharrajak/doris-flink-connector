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

import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.share.ShareAckCommittableSerializer;
import org.apache.flink.connector.kafka.share.ShareAckId;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DorisShareAckCommittableSerializerTest {
    @Test
    public void testRoundTripPreservesRecoveryPhase() throws Exception {
        DorisShareAckCommittable expected =
                new DorisShareAckCommittable(
                        DorisShareAckCommitPhase.DORIS_COMMITTED,
                        Collections.singletonList(
                                new DorisCommittable("fe:8040", "db", 101L)),
                        Collections.singletonList(shareAckCommittable()));
        DorisShareAckCommittableSerializer serializer =
                new DorisShareAckCommittableSerializer(
                        new TestDorisCommittableSerializer(),
                        new ShareAckCommittableSerializer());

        DorisShareAckCommittable restored =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(expected));

        Assert.assertEquals(expected, restored);
    }

    @Test(expected = IOException.class)
    public void testRejectsUnknownVersion() throws Exception {
        DorisShareAckCommittableSerializer serializer =
                new DorisShareAckCommittableSerializer(
                        new TestDorisCommittableSerializer(),
                        new ShareAckCommittableSerializer());
        serializer.deserialize(2, new byte[0]);
    }

    static ShareAckCommittable shareAckCommittable() {
        ShareAckId id = new ShareAckId("group", "topic-id", "topic", 0, 17L);
        return new ShareAckCommittable(
                "scope", 9L, "transactional-id", 33L, (short) 2, "state", List.of(id));
    }

    private static final class TestDorisCommittableSerializer
            implements SimpleVersionedSerializer<DorisCommittable> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(DorisCommittable committable) throws IOException {
            try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeUTF(committable.getHostPort());
                output.writeUTF(committable.getDb());
                output.writeLong(committable.getTxnID());
                output.flush();
                return bytes.toByteArray();
            }
        }

        @Override
        public DorisCommittable deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                    DataInputStream input = new DataInputStream(bytes)) {
                return new DorisCommittable(input.readUTF(), input.readUTF(), input.readLong());
            }
        }
    }
}
