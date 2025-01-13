/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.coordinate;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;

import accord.api.ProtocolModifiers;
import accord.api.Result;
import accord.coordinate.ExecuteSyncPoint.ExecuteInclusive;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.PreAcceptExclusiveSyncPointTracker;
import accord.coordinate.tracking.PreAcceptTracker;
import accord.local.Node;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.SLOW;
import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;

public interface CoordinationAdapter<R>
{
    interface Factory
    {
        enum Kind { Standard, Recovery }
        <R> CoordinationAdapter<R> get(TxnId txnId, Kind kind);
    }

    void propose(Node node, @Nullable Topologies preaccept, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void stabilise(Node node, @Nullable Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void execute(Node node, @Nullable Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void persist(Node node, @Nullable Topologies any, FullRoute<?> route, Route<?> sendTo, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback);
    default void persist(Node node, @Nullable Topologies any, FullRoute<?> route, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback)
    {
        persist(node, any, route, route, txnId, txn, executeAt, deps, writes, result, callback);
    }

    class DefaultFactory implements Factory
    {
        @Override
        public <R> CoordinationAdapter<R> get(TxnId txnId, Kind kind)
        {
            switch (txnId.kind())
            {
                case ExclusiveSyncPoint:
                    // callback types are different, and we pass through the recovery adapter for sync points so should not invoke Continue
                    Invariants.checkState(kind == Recovery);
                    return (CoordinationAdapter<R>) Adapters.recoverExclusiveSyncPoint();
                case SyncPoint:
                    Invariants.checkState(kind == Recovery);
                    return (CoordinationAdapter<R>) Adapters.recoverInclusiveSyncPoint();
            }
            switch (kind)
            {
                default: throw new UnhandledEnum(kind);
                case Standard: return (CoordinationAdapter<R>) Adapters.standard();
                case Recovery: return (CoordinationAdapter<R>) Adapters.recover();
            }
        }
    }

    class Adapters
    {
        public static CoordinationAdapter<Result> standard()
        {
            return StandardTxnAdapter.INSTANCE;
        }

        // note that by default the recovery adapter is only used for the initial recovery decision - if e.g. propose is initiated
        // then we revert back to standard adapter behaviour for later steps
        public static CoordinationAdapter<Result> recover()
        {
            return RecoverTxnAdapter.INSTANCE;
        }

        public static <U extends Unseekable> SyncPointAdapter<SyncPoint<U>> inclusiveSyncPoint()
        {
            return AsyncInclusiveSyncPointAdapter.INSTANCE;
        }

        public static <U extends Unseekable> SyncPointAdapter<SyncPoint<U>> inclusiveSyncPointBlocking()
        {
            return InclusiveSyncPointBlockingAdapter.INSTANCE;
        }

        public static CoordinationAdapter<Result> recoverInclusiveSyncPoint()
        {
            return RecoverInclusiveSyncPointAdapter.INSTANCE;
        }

        public static <U extends Unseekable> SyncPointAdapter<SyncPoint<U>> exclusiveSyncPoint()
        {
            return ExclusiveSyncPointAdapter.INSTANCE;
        }

        public static CoordinationAdapter<Result> recoverExclusiveSyncPoint()
        {
            return RecoverExclusiveSyncPointAdapter.INSTANCE;
        }

        public static abstract class AbstractTxnAdapter implements CoordinationAdapter<Result>
        {
            @Override
            public void propose(Node node, @Nullable Topologies preacceptOrRecovery, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies accept = node.topology().reselect(preacceptOrRecovery, QuorumEpochIntersections.preacceptOrRecover,
                                                             route, txnId, executeAt, QuorumEpochIntersections.accept);
                new ProposeTxn(node, accept, route, kind, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies accept, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                if (!node.topology().hasEpoch(executeAt.epoch()))
                {
                    node.withEpoch(executeAt.epoch(), (success, fail) -> {
                        if (fail != null) callback.accept(null, fail);
                        else stabilise(node, accept, route, ballot, txnId, txn, executeAt, deps, callback);
                    });
                    return;
                }

                Topologies all = node.topology().reselect(accept, QuorumEpochIntersections.accept,
                                                          route, txnId, executeAt, QuorumEpochIntersections.commit);
                Topologies coordinates = all.size() == 1 ? all : accept.forEpoch(txnId.epoch());

                if (ProtocolModifiers.Faults.txnInstability) execute(node, all, route, SLOW, txnId, txn, executeAt, deps, callback);
                else new StabiliseTxn(node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, route, txnId, executeAt);

                if (txn.read().keys().isEmpty()) persist(node, all, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
                else new ExecuteTxn(node, all, route, path, txnId, txn, executeAt, deps, callback).start();
            }

            Topologies execution(Node node, Topologies preacceptOrCommit, Participants<?> participants, TxnId txnId, Timestamp executeAt)
            {
                return node.topology().reselect(preacceptOrCommit, QuorumEpochIntersections.preacceptOrCommit,
                                                participants, txnId, executeAt, QuorumEpochIntersections.stable);
            }
        }

        public static class StandardTxnAdapter extends AbstractTxnAdapter
        {
            public static final StandardTxnAdapter INSTANCE = new StandardTxnAdapter();

            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Route<?> sendTo, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, sendTo, txnId, executeAt);

                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, sendTo, txn, executeAt, deps, writes, result, route)
                    .start(Apply.FACTORY, Minimal, all, writes, result);
            }
        }

        public static class RecoverTxnAdapter extends AbstractTxnAdapter
        {
            public static final RecoverTxnAdapter INSTANCE = new RecoverTxnAdapter();
            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Route<?> sendTo, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> callback)
            {
                Topologies all = execution(node, any, sendTo, txnId, executeAt);

                if (callback != null) callback.accept(result, null);
                new PersistTxn(node, all, txnId, sendTo, txn, executeAt, deps, writes, result, route)
                    .start(Apply.FACTORY, Maximal, all, writes, result);
            }
        }

        public static abstract class SyncPointAdapter<R> implements CoordinationAdapter<R>
        {
            final BiFunction<Topologies, TxnId, PreAcceptTracker<?>> preacceptTrackerFactory;

            protected SyncPointAdapter(BiFunction<Topologies, TxnId, PreAcceptTracker<?>> preacceptTrackerFactory)
            {
                this.preacceptTrackerFactory = preacceptTrackerFactory;
            }

            abstract Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId);
            abstract Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps);
            abstract void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super R, Throwable> callback);

            @Override
            public void propose(Node node, Topologies any, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                Topologies all = forDecision(node, route, txnId);
                new ProposeSyncPoint<>(this, node, all, route, kind, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void stabilise(Node node, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);
                Topologies coordinates = all.forEpoch(txnId.epoch());
                new StabiliseSyncPoint<>(this, node, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback).start();
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                persist(node, null, route, txnId, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), callback);
            }

            @Override
            public void persist(Node node, Topologies ignore, FullRoute<?> route, Route<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);

                invokeSuccess(node, route, txnId, executeAt, txn, deps, callback);
                new PersistSyncPoint(node, all, txnId, route, txn, executeAt, deps, writes, result)
                .start(Apply.FACTORY, Maximal, all, writes, result);
            }
        }

        private static abstract class AbstractExclusiveSyncPointAdapter<R> extends SyncPointAdapter<R>
        {
            public AbstractExclusiveSyncPointAdapter()
            {
                super(PreAcceptExclusiveSyncPointTracker::new);
            }

            @Override
            Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId)
            {
                return node.topology().withOpenEpochs(route, null, txnId);
            }

            @Override
            Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps)
            {
                return node.topology().withUncompletedEpochs(route, txnId, txnId);
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                // TODO (required, consider): remember and document why we don't use fast path for exclusive sync points
                if (path == FAST) stabilise(node, any, route, Ballot.ZERO, txnId, txn, executeAt, deps, callback);
                else super.execute(node, any, route, path, txnId, txn, executeAt, deps, callback);
            }
        }

        static class RecoverExclusiveSyncPointAdapter extends AbstractExclusiveSyncPointAdapter<Result>
        {
            static final RecoverExclusiveSyncPointAdapter INSTANCE = new RecoverExclusiveSyncPointAdapter();

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                callback.accept(txn.result(txnId, executeAt, null), null);
            }
        }

        static class ExclusiveSyncPointAdapter<U extends Unseekable> extends AbstractExclusiveSyncPointAdapter<SyncPoint<U>>
        {
            static final ExclusiveSyncPointAdapter INSTANCE = new ExclusiveSyncPointAdapter();

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super SyncPoint<U>, Throwable> callback)
            {
                callback.accept(new SyncPoint<>(txnId, executeAt, deps, (FullRoute<U>)route), null);
            }
        }

        private static abstract class AbstractInclusiveSyncPointAdapter<R> extends SyncPointAdapter<R>
        {
            protected AbstractInclusiveSyncPointAdapter()
            {
                // TODO (required): this is only safe if SyncPoint is visible to other transactions
                super(FastPathTracker::new);
            }

            @Override
            Topologies forDecision(Node node, FullRoute<?> route, TxnId txnId)
            {
                return node.topology().withUnsyncedEpochs(route, txnId, txnId);
            }

            @Override
            Topologies forExecution(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Deps deps)
            {
                return node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            }
        }

        private static abstract class AbstractInitiateInclusiveSyncPointAdapter<U extends Unseekable> extends AbstractInclusiveSyncPointAdapter<SyncPoint<U>>
        {
            protected AbstractInitiateInclusiveSyncPointAdapter() {}

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super SyncPoint<U>, Throwable> callback)
            {
                callback.accept(new SyncPoint<>(txnId, executeAt, deps, (FullRoute<U>) route), null);
            }
        }

        /*
         * Async meaning that the result of the distributed sync point is not known when this returns
         * At most the caller can wait for the sync point to complete locally. This does mean that the sync
         * point is being executed and that eventually information will be known locally everywhere about the last
         * sync point for the keys/ranges this sync point covered.
         */
        public static class AsyncInclusiveSyncPointAdapter<U extends Unseekable> extends AbstractInitiateInclusiveSyncPointAdapter<U>
        {
            private static final AsyncInclusiveSyncPointAdapter INSTANCE = new AsyncInclusiveSyncPointAdapter();

            protected AsyncInclusiveSyncPointAdapter() {
                super();
            }
        }

        public static class InclusiveSyncPointBlockingAdapter<U extends Unseekable> extends AbstractInitiateInclusiveSyncPointAdapter<U>
        {
            private static final InclusiveSyncPointBlockingAdapter INSTANCE = new InclusiveSyncPointBlockingAdapter();

            protected InclusiveSyncPointBlockingAdapter() {
                super();
            }

            @Override
            public void execute(Node node, Topologies any, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super SyncPoint<U>, Throwable> callback)
            {
                Topologies all = forExecution(node, route, txnId, executeAt, deps);

                ExecuteInclusive<U> execute = ExecuteInclusive.atQuorum(node, all, new SyncPoint<>(txnId, executeAt, deps, (FullRoute<U>) route), executeAt);
                execute.addCallback(callback);
                execute.start();
            }

            @Override
            public void persist(Node node, Topologies any, FullRoute<?> route, Route<?> participants, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super SyncPoint<U>, Throwable> callback)
            {
                throw new UnsupportedOperationException();
            }
        }

        private static class RecoverInclusiveSyncPointAdapter extends AbstractInclusiveSyncPointAdapter<Result>
        {
            private static final RecoverInclusiveSyncPointAdapter INSTANCE = new RecoverInclusiveSyncPointAdapter();

            protected RecoverInclusiveSyncPointAdapter() {}

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                callback.accept(txn.result(txnId, executeAt, null), null);
            }
        }
    }

}
