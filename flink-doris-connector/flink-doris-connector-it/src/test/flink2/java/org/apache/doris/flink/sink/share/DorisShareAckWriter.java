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
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.share.ShareAckRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class DorisShareAckWriter<IN>
        implements StatefulSinkWriter<DorisShareAckRecord<IN>, DorisWriterState>,
                CommittingSinkWriter<DorisShareAckRecord<IN>, DorisShareAckCommittable> {
    private final StatefulSinkWriter<IN, DorisWriterState> dorisWriter;
    private final CommittingSinkWriter<IN, DorisCommittable> dorisCommittingWriter;
    private final CommittingSinkWriter<ShareAckRecord, ShareAckCommittable> shareAckWriter;

    DorisShareAckWriter(
            StatefulSinkWriter<IN, DorisWriterState> dorisWriter,
            CommittingSinkWriter<IN, DorisCommittable> dorisCommittingWriter,
            CommittingSinkWriter<ShareAckRecord, ShareAckCommittable> shareAckWriter) {
        this.dorisWriter = Objects.requireNonNull(dorisWriter, "dorisWriter");
        this.dorisCommittingWriter =
                Objects.requireNonNull(dorisCommittingWriter, "dorisCommittingWriter");
        this.shareAckWriter = Objects.requireNonNull(shareAckWriter, "shareAckWriter");
    }

    @Override
    public void write(DorisShareAckRecord<IN> record, Context context)
            throws IOException, InterruptedException {
        dorisWriter.write(record.getDorisRecord(), context);
        shareAckWriter.write(record.getShareAckRecord(), context);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        dorisWriter.flush(endOfInput);
        shareAckWriter.flush(endOfInput);
    }

    @Override
    public Collection<DorisShareAckCommittable> prepareCommit()
            throws IOException, InterruptedException {
        Collection<DorisCommittable> dorisCommittables = dorisCommittingWriter.prepareCommit();
        Collection<ShareAckCommittable> shareAckCommittables = shareAckWriter.prepareCommit();
        if (dorisCommittables.isEmpty() && shareAckCommittables.isEmpty()) {
            return Collections.emptyList();
        }
        if (dorisCommittables.isEmpty() || shareAckCommittables.isEmpty()) {
            throw new IOException("Doris data and share ACKs must be prepared together");
        }
        return Collections.singletonList(
                new DorisShareAckCommittable(
                        DorisShareAckCommitPhase.PREPARED,
                        dorisCommittables,
                        shareAckCommittables));
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        return dorisWriter.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            dorisWriter.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            shareAckWriter.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
