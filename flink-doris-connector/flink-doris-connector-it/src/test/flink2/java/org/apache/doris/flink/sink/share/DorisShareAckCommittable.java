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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class DorisShareAckCommittable {
    private final DorisShareAckCommitPhase phase;
    private final List<DorisCommittable> dorisCommittables;
    private final List<ShareAckCommittable> shareAckCommittables;

    DorisShareAckCommittable(
            DorisShareAckCommitPhase phase,
            Collection<DorisCommittable> dorisCommittables,
            Collection<ShareAckCommittable> shareAckCommittables) {
        this.phase = Objects.requireNonNull(phase, "phase");
        this.dorisCommittables = immutableCopy(dorisCommittables, "dorisCommittables");
        this.shareAckCommittables =
                immutableCopy(shareAckCommittables, "shareAckCommittables");
        if (this.dorisCommittables.isEmpty() || this.shareAckCommittables.isEmpty()) {
            throw new IllegalArgumentException("Both Doris and share ACK committables are required");
        }
    }

    DorisShareAckCommitPhase getPhase() {
        return phase;
    }

    List<DorisCommittable> getDorisCommittables() {
        return dorisCommittables;
    }

    List<ShareAckCommittable> getShareAckCommittables() {
        return shareAckCommittables;
    }

    DorisShareAckCommittable withPhase(DorisShareAckCommitPhase newPhase) {
        return new DorisShareAckCommittable(
                newPhase, dorisCommittables, shareAckCommittables);
    }

    DorisShareAckCommittable withDorisCommittable(int index, DorisCommittable committable) {
        List<DorisCommittable> updated = new ArrayList<>(dorisCommittables);
        updated.set(index, committable);
        return new DorisShareAckCommittable(phase, updated, shareAckCommittables);
    }

    DorisShareAckCommittable withShareAckCommittable(
            int index, ShareAckCommittable committable) {
        List<ShareAckCommittable> updated = new ArrayList<>(shareAckCommittables);
        updated.set(index, committable);
        return new DorisShareAckCommittable(phase, dorisCommittables, updated);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        DorisShareAckCommittable that = (DorisShareAckCommittable) object;
        return phase == that.phase
                && dorisCommittables.equals(that.dorisCommittables)
                && shareAckCommittables.equals(that.shareAckCommittables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, dorisCommittables, shareAckCommittables);
    }

    private static <T> List<T> immutableCopy(Collection<T> values, String name) {
        return Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(values, name)));
    }
}
