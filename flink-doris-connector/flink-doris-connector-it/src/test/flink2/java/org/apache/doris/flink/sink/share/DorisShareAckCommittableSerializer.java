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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class DorisShareAckCommittableSerializer
        implements SimpleVersionedSerializer<DorisShareAckCommittable> {
    private static final int VERSION = 1;

    private final SimpleVersionedSerializer<DorisCommittable> dorisSerializer;
    private final SimpleVersionedSerializer<ShareAckCommittable> shareAckSerializer;

    DorisShareAckCommittableSerializer(
            SimpleVersionedSerializer<DorisCommittable> dorisSerializer,
            SimpleVersionedSerializer<ShareAckCommittable> shareAckSerializer) {
        this.dorisSerializer = Objects.requireNonNull(dorisSerializer, "dorisSerializer");
        this.shareAckSerializer = Objects.requireNonNull(shareAckSerializer, "shareAckSerializer");
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisShareAckCommittable committable) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeUTF(committable.getPhase().name());
            writeCollection(output, committable.getDorisCommittables(), dorisSerializer);
            writeCollection(output, committable.getShareAckCommittables(), shareAckSerializer);
            output.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public DorisShareAckCommittable deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                DataInputStream input = new DataInputStream(bytes)) {
            DorisShareAckCommitPhase phase =
                    DorisShareAckCommitPhase.valueOf(input.readUTF());
            List<DorisCommittable> dorisCommittables =
                    readCollection(input, dorisSerializer);
            List<ShareAckCommittable> shareAckCommittables =
                    readCollection(input, shareAckSerializer);
            return new DorisShareAckCommittable(
                    phase, dorisCommittables, shareAckCommittables);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown commit phase", e);
        }
    }

    private static <T> void writeCollection(
            DataOutputStream output,
            List<T> values,
            SimpleVersionedSerializer<T> serializer)
            throws IOException {
        output.writeInt(serializer.getVersion());
        output.writeInt(values.size());
        for (T value : values) {
            byte[] serialized = serializer.serialize(value);
            output.writeInt(serialized.length);
            output.write(serialized);
        }
    }

    private static <T> List<T> readCollection(
            DataInputStream input, SimpleVersionedSerializer<T> serializer) throws IOException {
        int nestedVersion = input.readInt();
        int size = input.readInt();
        List<T> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            int length = input.readInt();
            byte[] serialized = new byte[length];
            input.readFully(serialized);
            values.add(serializer.deserialize(nestedVersion, serialized));
        }
        return values;
    }
}
