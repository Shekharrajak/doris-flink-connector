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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

final class OrderedDorisShareAckCommitter implements Committer<DorisShareAckCommittable> {
    private final Committer<DorisCommittable> dorisCommitter;
    private final Committer<ShareAckCommittable> shareAckCommitter;

    OrderedDorisShareAckCommitter(
            Committer<DorisCommittable> dorisCommitter,
            Committer<ShareAckCommittable> shareAckCommitter) {
        this.dorisCommitter = Objects.requireNonNull(dorisCommitter, "dorisCommitter");
        this.shareAckCommitter =
                Objects.requireNonNull(shareAckCommitter, "shareAckCommitter");
    }

    @Override
    public void commit(Collection<CommitRequest<DorisShareAckCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<DorisShareAckCommittable> request : requests) {
            commitOne(request);
        }
    }

    private void commitOne(CommitRequest<DorisShareAckCommittable> request)
            throws IOException, InterruptedException {
        DorisShareAckCommittable current = request.getCommittable();
        if (current.getPhase() == DorisShareAckCommitPhase.PREPARED) {
            Optional<DorisShareAckCommittable> afterDoris = commitDoris(request, current);
            if (!afterDoris.isPresent()) {
                return;
            }
            current = afterDoris.get().withPhase(DorisShareAckCommitPhase.DORIS_COMMITTED);
        }
        commitShareAcks(request, current);
    }

    private Optional<DorisShareAckCommittable> commitDoris(
            CommitRequest<DorisShareAckCommittable> request,
            DorisShareAckCommittable committable)
            throws IOException, InterruptedException {
        DorisShareAckCommittable current = committable;
        for (int index = 0; index < current.getDorisCommittables().size(); index++) {
            TrackingCommitRequest<DorisCommittable> nested =
                    commitNested(
                            dorisCommitter,
                            current.getDorisCommittables().get(index),
                            request.getNumberOfRetries());
            if (nested.getUpdatedCommittable() != null) {
                current =
                        current.withDorisCommittable(index, nested.getUpdatedCommittable());
            }
            if (!handleResult(request, current, nested)) {
                return Optional.empty();
            }
        }
        return Optional.of(current);
    }

    private void commitShareAcks(
            CommitRequest<DorisShareAckCommittable> request,
            DorisShareAckCommittable committable)
            throws IOException, InterruptedException {
        DorisShareAckCommittable current = committable;
        for (int index = 0; index < current.getShareAckCommittables().size(); index++) {
            TrackingCommitRequest<ShareAckCommittable> nested =
                    commitNested(
                            shareAckCommitter,
                            current.getShareAckCommittables().get(index),
                            request.getNumberOfRetries());
            if (nested.getUpdatedCommittable() != null) {
                current =
                        current.withShareAckCommittable(
                                index, nested.getUpdatedCommittable());
            }
            if (!handleResult(request, current, nested)) {
                return;
            }
        }
    }

    private static <T> TrackingCommitRequest<T> commitNested(
            Committer<T> committer, T committable, int retries)
            throws IOException, InterruptedException {
        TrackingCommitRequest<T> request = new TrackingCommitRequest<>(committable, retries);
        committer.commit(Collections.singletonList(request));
        return request;
    }

    private static boolean handleResult(
            CommitRequest<DorisShareAckCommittable> outer,
            DorisShareAckCommittable current,
            TrackingCommitRequest<?> nested) {
        switch (nested.getResult()) {
            case SUCCESS:
            case ALREADY_COMMITTED:
                return true;
            case RETRY:
                outer.updateAndRetryLater(current);
                return false;
            case FAILED_KNOWN:
                outer.signalFailedWithKnownReason(nested.getFailure());
                return false;
            case FAILED_UNKNOWN:
                outer.signalFailedWithUnknownReason(nested.getFailure());
                return false;
            default:
                throw new IllegalStateException("Unknown nested commit result");
        }
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            dorisCommitter.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            shareAckCommitter.close();
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

    private enum CommitResult {
        SUCCESS,
        RETRY,
        FAILED_KNOWN,
        FAILED_UNKNOWN,
        ALREADY_COMMITTED
    }

    private static final class TrackingCommitRequest<T> implements CommitRequest<T> {
        private final T committable;
        private final int retries;
        private CommitResult result = CommitResult.SUCCESS;
        @Nullable private T updatedCommittable;
        @Nullable private Throwable failure;

        private TrackingCommitRequest(T committable, int retries) {
            this.committable = committable;
            this.retries = retries;
        }

        @Override
        public T getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return retries;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {
            updateResult(CommitResult.FAILED_KNOWN, throwable, null);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {
            updateResult(CommitResult.FAILED_UNKNOWN, throwable, null);
        }

        @Override
        public void retryLater() {
            updateResult(CommitResult.RETRY, null, null);
        }

        @Override
        public void updateAndRetryLater(T updated) {
            updateResult(CommitResult.RETRY, null, updated);
        }

        @Override
        public void signalAlreadyCommitted() {
            updateResult(CommitResult.ALREADY_COMMITTED, null, null);
        }

        CommitResult getResult() {
            return result;
        }

        @Nullable
        T getUpdatedCommittable() {
            return updatedCommittable;
        }

        @Nullable
        Throwable getFailure() {
            return failure;
        }

        private void updateResult(
                CommitResult newResult, @Nullable Throwable newFailure, @Nullable T updated) {
            if (result != CommitResult.SUCCESS) {
                throw new IllegalStateException("Commit result was already signalled");
            }
            result = newResult;
            failure = newFailure;
            updatedCommittable = updated;
        }
    }
}
