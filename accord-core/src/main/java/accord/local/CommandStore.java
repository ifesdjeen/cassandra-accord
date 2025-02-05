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

package accord.local;

import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.DataStore;

import javax.annotation.Nullable;
import accord.api.Agent;

import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.RangeDeps;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Unseekables;
import accord.utils.async.AsyncChain;

import accord.api.ConfigurationService.EpochReady;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;
import org.agrona.collections.LongHashSet;

import static accord.api.ConfigurationService.EpochReady.DONE;
import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.PreLoadContext.empty;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.nonNull;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AgentExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    static class EpochUpdate
    {
        final RangesForEpoch newRangesForEpoch;
        final RedundantBefore addRedundantBefore;

        EpochUpdate(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            this.newRangesForEpoch = newRangesForEpoch;
            this.addRedundantBefore = addRedundantBefore;
        }
    }

    public static class EpochUpdateHolder extends AtomicReference<EpochUpdate>
    {
        // TODO (desired): can better encapsulate by accepting only the newRangesForEpoch and deriving the add/remove ranges
        public void add(long epoch, RangesForEpoch newRangesForEpoch, Ranges addRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(addRanges, epoch, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.minForEpoch(epoch));
            update(newRangesForEpoch, addRedundantBefore);
        }

        public void remove(long epoch, RangesForEpoch newRangesForEpoch, Ranges removeRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(removeRanges, Long.MIN_VALUE, epoch, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE);
            update(newRangesForEpoch, addRedundantBefore);
        }

        private void update(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            EpochUpdate baseUpdate = new EpochUpdate(newRangesForEpoch, addRedundantBefore);
            EpochUpdate cur = get();
            if (cur == null || !compareAndSet(cur, new EpochUpdate(newRangesForEpoch, RedundantBefore.merge(cur.addRedundantBefore, addRedundantBefore))))
                set(baseUpdate);
        }
    }

    public interface Factory
    {
        CommandStore create(int id,
                            NodeCommandStoreService node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            LocalListeners.Factory listenersFactory,
                            EpochUpdateHolder rangesForEpoch,
                            Journal journal);
    }

    private static final ThreadLocal<CommandStore> CURRENT_STORE = new ThreadLocal<>();

    protected final int id;
    protected final NodeCommandStoreService node;
    protected final Agent agent;
    protected final DataStore store;
    protected final ProgressLog progressLog;
    protected final LocalListeners listeners;
    protected final EpochUpdateHolder epochUpdateHolder;

    // Used in markShardStale to make sure the staleness includes in progress bootstraps
    private transient NavigableMap<TxnId, Ranges> bootstrapBeganAt = emptyBootstrapBeganAt(); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    private RedundantBefore redundantBefore = RedundantBefore.EMPTY;
    private MaxConflicts maxConflicts = MaxConflicts.EMPTY;
    private int maxConflictsUpdates = 0;
    protected RangesForEpoch rangesForEpoch;

    /**
     * safeToRead is related to RedundantBefore, but a distinct concept.
     * While bootstrappedAt defines the txnId bounds we expect to maintain data for locally,
     * safeToRead defines executeAt bounds we can safely participate in transaction execution for.
     * safeToRead is defined by the no-op transaction we execute after a bootstrap is initiated,
     * and creates a global bound before which we know we have complete data from our bootstrap.
     *
     * There's a smearing period during bootstrap where some keys may be ahead of others, for instance,
     * since we do not create a precise instant in the transaction log for bootstrap to avoid impeding execution.
     *
     * We also update safeToRead when we go stale, to remove ranges we may have bootstrapped but that are now known to
     * be incomplete. In this case we permit transactions to execute in any order for the unsafe key ranges.
     * But they may still be ordered for other key ranges they participate in.
     */
    private NavigableMap<Timestamp, Ranges> safeToRead = emptySafeToRead();
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    @Nullable private RejectBefore rejectBefore;

    static class WaitingOnSync
    {
        final AsyncResults.SettableResult<Void> whenDone;
        final Ranges allRanges;
        Ranges ranges;

        WaitingOnSync(AsyncResults.SettableResult<Void> whenDone, Ranges ranges)
        {
            this.whenDone = whenDone;
            this.allRanges = this.ranges = ranges;
        }
    }
    private final TreeMap<Long, WaitingOnSync> waitingOnSync = new TreeMap<>();

    protected CommandStore(int id,
                           NodeCommandStoreService node,
                           Agent agent,
                           DataStore store,
                           ProgressLog.Factory progressLogFactory,
                           LocalListeners.Factory listenersFactory,
                           EpochUpdateHolder epochUpdateHolder)
    {
        this.id = id;
        this.node = node;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.listeners = listenersFactory.create(this);
        this.epochUpdateHolder = epochUpdateHolder;
    }

    public final int id()
    {
        return id;
    }

    public void restore() {};

    public abstract Journal.Loader loader();

    @Override
    public Agent agent()
    {
        return agent;
    }

    public void updateRangesForEpoch(SafeCommandStore safeStore)
    {
        EpochUpdate update = epochUpdateHolder.get();
        if (update == null)
            return;

        update = epochUpdateHolder.getAndSet(null);
        if (update.addRedundantBefore.size() > 0)
            safeStore.upsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            safeStore.setRangesForEpoch(update.newRangesForEpoch);
    }

    @VisibleForTesting
    public void unsafeUpdateRangesForEpoch()
    {
        EpochUpdate update = epochUpdateHolder.getAndSet(null);
        if (update == null)
            return;

        if (update.addRedundantBefore.size() > 0)
            unsafeUpsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            unsafeSetRangesForEpoch(update.newRangesForEpoch);
    }

    public RangesForEpoch unsafeGetRangesForEpoch()
    {
        return rangesForEpoch;
    }

    final void unsafeSetRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        rangesForEpoch = nonNull(newRangesForEpoch);
    }

    protected final void unsafeClearRangesForEpoch()
    {
        rangesForEpoch = null;
    }

    protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        Invariants.require(this.rangesForEpoch == null);
        unsafeSetRangesForEpoch(newRangesForEpoch);
    }

    public abstract boolean inStore();

    public void maybeExecuteImmediately(Runnable task)
    {
        if (inStore()) task.run();
        else           execute(task);
    }

    public abstract AsyncChain<Void> build(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    public abstract <T> AsyncChain<T> build(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    public void execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        build(context, consumer).begin(callback);
    }

    public AsyncResult<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return build(context, consumer).beginAsResult();
    }

    public <T> void submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply, BiConsumer<? super T, Throwable> callback)
    {
        build(context, apply).begin(callback);
    }

    public <T> AsyncResult<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply)
    {
        return build(context, apply).beginAsResult();
    }

    public abstract void shutdown();

    protected abstract void registerTransitive(SafeCommandStore safeStore, RangeDeps deps);

    protected void unsafeSetRejectBefore(RejectBefore newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
    }

    final void unsafeSetRedundantBefore(RedundantBefore newRedundantBefore)
    {
        redundantBefore = newRedundantBefore;
    }

    protected void unsafeClearRedundantBefore()
    {
        unsafeSetRedundantBefore(null);
    }

    protected void loadRedundantBefore(RedundantBefore newRedundantBefore)
    {
        Invariants.require(redundantBefore == null || redundantBefore.equals(RedundantBefore.EMPTY));
        Invariants.require(newRedundantBefore != null);
        unsafeSetRedundantBefore(newRedundantBefore);
    }

    protected void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        redundantBefore = RedundantBefore.merge(redundantBefore, addRedundantBefore);
    }

    /**
     * This method may be invoked on a non-CommandStore thread
     */
    final void unsafeSetSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        this.safeToRead = newSafeToRead;
    }

    protected final void unsafeClearSafeToRead()
    {
        unsafeSetSafeToRead(null);
    }

    protected void loadSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        Invariants.require(safeToRead == null || safeToRead.equals(emptySafeToRead()));
        Invariants.require(newSafeToRead != null);
        unsafeSetSafeToRead(newSafeToRead);
        updateMaxConflicts(newSafeToRead);
    }

    final void unsafeSetBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        this.bootstrapBeganAt = newBootstrapBeganAt;
    }

    protected final void unsafeClearBootstrapBeganAt()
    {
        unsafeSetBootstrapBeganAt(null);
    }

    protected synchronized void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        Invariants.require(bootstrapBeganAt == null || bootstrapBeganAt.equals(emptyBootstrapBeganAt()));
        Invariants.require(newBootstrapBeganAt != null);
        unsafeSetBootstrapBeganAt(newBootstrapBeganAt);
        updateMaxConflicts(newBootstrapBeganAt);
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setMaxConflicts(MaxConflicts maxConflicts)
    {
        this.maxConflicts = maxConflicts;
    }

    protected int dumpCounter = 0;

    protected void updateMaxConflicts(Command prev, Command updated)
    {
        Timestamp executeAt = updated.executeAt();
        if (executeAt == null) return;
        if (prev != null && prev.executeAt() != null && prev.executeAt().compareToStrict(executeAt) >= 0) return;
        executeAt = executeAt.flattenUniqueHlc(); // this is what guarantees a bootstrap recipient can compute uniqueHlc safely
        MaxConflicts updatedMaxConflicts = maxConflicts.update(updated.participants().hasTouched(), executeAt);
        updateMaxConflicts(executeAt, updatedMaxConflicts);
    }

    protected void updateMaxConflicts(Ranges ranges, Timestamp executeAt)
    {
        updateMaxConflicts(executeAt, maxConflicts.update(ranges, executeAt));
    }

    protected void updateMaxConflicts(NavigableMap<? extends Timestamp, Ranges> map)
    {
        Timestamp max = Timestamp.NONE;
        MaxConflicts updated = maxConflicts;
        for (Map.Entry<? extends Timestamp, Ranges> e : map.entrySet())
        {
            Timestamp at = e.getKey();
            if (at.compareTo(Timestamp.NONE) > 0)
            {
                updated = updated.update(e.getValue(), at);
                max = Timestamp.max(max, at);
            }
        }
        if (updated != maxConflicts)
            updateMaxConflicts(max, updated);
    }

    protected void updateMaxConflicts(Timestamp executeAt, MaxConflicts updatedMaxConflicts)
    {
        if (++maxConflictsUpdates >= agent.maxConflictsPruneInterval())
        {
            int initialSize = updatedMaxConflicts.size();
            MaxConflicts initialConflicts = updatedMaxConflicts;
            long pruneHlc = executeAt.hlc() - agent.maxConflictsHlcPruneDelta();
            Timestamp pruneBefore = pruneHlc > 0 ? Timestamp.fromValues(executeAt.epoch(), pruneHlc, executeAt.node) : null;
            Ranges ranges = rangesForEpoch.all();
            if (pruneBefore != null)
                updatedMaxConflicts = updatedMaxConflicts.update(ranges, pruneBefore);

            int prunedSize = updatedMaxConflicts.size();
            if (initialSize > 100 && prunedSize == initialSize)
            {
                logger.debug("Ineffective prune for {}. Initial size: {}, pruned size: {}, executeAt: {}, pruneBefore: {}", ranges, initialSize, prunedSize, executeAt, pruneBefore);
                if (dumpCounter == 0)
                {
                    logger.trace("initial MaxConflicts dump: {}", initialConflicts);
                    logger.trace("pruned MaxConflicts dump: {}", updatedMaxConflicts);
                }
                dumpCounter++;
                dumpCounter %= 100;
            }
            else if (prunedSize != initialSize)
            {
                logger.trace("Successfully pruned {} to {}", initialSize, prunedSize);
            }


            maxConflictsUpdates = 0;
        }
        setMaxConflicts(updatedMaxConflicts);
    }

    public final void markExclusiveSyncPoint(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.is(ExclusiveSyncPoint));
        RejectBefore newRejectBefore = rejectBefore != null ? rejectBefore : new RejectBefore();
        newRejectBefore = RejectBefore.add(newRejectBefore, ranges, txnId);
        unsafeSetRejectBefore(newRejectBefore);
    }

    public final void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.is(ExclusiveSyncPoint));
        RedundantBefore newRedundantBefore = RedundantBefore.merge(redundantBefore, RedundantBefore.create(ranges, txnId, txnId, TxnId.NONE, TxnId.NONE, TxnId.NONE));
        safeStore.upsertRedundantBefore(newRedundantBefore);
        unsafeSetRedundantBefore(newRedundantBefore);
        updatedRedundantBefore(safeStore, txnId, ranges);
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    final Timestamp preaccept(TxnId txnId, Routables<?> keys, SafeCommandStore safeStore, boolean permitFastPath)
    {
        NodeCommandStoreService node = safeStore.node();

        boolean isExpired = node.now() - txnId.hlc() >= safeStore.agent().preAcceptTimeout() && !txnId.isSyncPoint();
        if (rejectBefore != null && !isExpired)
            isExpired = rejectBefore.rejects(txnId, keys);

        if (isExpired)
            return node.uniqueNow(txnId).asRejected();

        Timestamp min = TxnId.mergeMax(txnId, maxConflicts.get(keys));
        if (permitFastPath && txnId == min && txnId.epoch() >= node.epoch())
            return txnId;

        return node.uniqueNow(min);
    }

    protected void unsafeRunIn(Runnable fn)
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            fn.run();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    protected <T> T unsafeRunIn(Callable<T> fn) throws Exception
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            return fn.call();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{id=" + id + ", node=" + node.id().id + '}';
    }

    @Nullable
    public static CommandStore maybeCurrent()
    {
        return CURRENT_STORE.get();
    }

    public static CommandStore current()
    {
        CommandStore cs = maybeCurrent();
        if (cs == null)
            throw illegalState("Attempted to access current CommandStore, but not running in a CommandStore");
        return cs;
    }

    public static CommandStore currentOrElseSelect(Node node, Route<?> route)
    {
        CommandStore cs = maybeCurrent();
        if (cs == null)
            return node.commandStores().select(route);
        return cs;
    }

    protected static void register(CommandStore store)
    {
        if (!store.inStore())
            throw illegalState("Unable to register a CommandStore when not running in it; store " + store);
        CURRENT_STORE.set(store);
    }

    public static void checkInStore()
    {
        CommandStore store = maybeCurrent();
        if (store == null) throw illegalState("Expected to be running in a CommandStore but is not");
    }

    public static void checkNotInStore()
    {
        CommandStore store = maybeCurrent();
        if (store != null)
            throw illegalState("Expected to not be running in a CommandStore, but running in " + store);
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    final Supplier<EpochReady> bootstrapper(Node node, Ranges newRanges, long epoch)
    {
        return () -> {
            AsyncResult<EpochReady> metadata = submit(empty(), safeStore -> {
                Bootstrap bootstrap = new Bootstrap(node, this, epoch, newRanges);
                bootstraps.add(bootstrap);
                bootstrap.start(safeStore);
                return new EpochReady(epoch, null, null, bootstrap.data, bootstrap.reads);
            });

            AsyncResult<Void> readyToCoordinate = readyToCoordinate(newRanges, epoch);
            return new EpochReady(epoch, metadata.<Void>map(ignore -> null).beginAsResult(),
                readyToCoordinate.beginAsResult(),
                metadata.flatMap(e -> e.data).beginAsResult(),
                metadata.flatMap(e -> e.reads).beginAsResult());
        };
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    protected Supplier<EpochReady> sync(Node node, Ranges ranges, long epoch)
    {
        return () -> {
            AsyncResult<Void> readyToCoordinate = readyToCoordinate(ranges, epoch);
            return new EpochReady(epoch, DONE, readyToCoordinate, DONE, DONE);
        };
    }

    private AsyncResult<Void> readyToCoordinate(Ranges ranges, long epoch)
    {
        if (redundantBefore.min(ranges, RedundantBefore.Entry::locallyWitnessedBefore).epoch() >= epoch)
            return DONE;

        AsyncResults.SettableResult<Void> whenDone = new AsyncResults.SettableResult<>();
        waitingOnSync.put(epoch, new WaitingOnSync(whenDone, ranges));
        return whenDone;
    }

    Supplier<EpochReady> unbootstrap(long epoch, Ranges removedRanges)
    {
        return () -> {
            AsyncResult<Void> done = submit(empty(), safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.slice(removedRanges, Minimal);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                return null;
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    final void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    final void markBootstrapping(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        safeStore.setBootstrapBeganAt(bootstrap(globalSyncId, ranges, bootstrapBeganAt));
        updateMaxConflicts(ranges, globalSyncId);
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, globalSyncId);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        updatedRedundantBefore(safeStore, globalSyncId, ranges);
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges durableRanges)
    {
        final Ranges slicedRanges = durableRanges.slice(safeStore.ranges().allUntil(globalSyncId.epoch()), Minimal);
        TxnId locallyRedundantBefore = safeStore.redundantBefore().min(slicedRanges, e -> e.locallyAppliedBefore);
        RedundantBefore addShardRedundant = RedundantBefore.create(slicedRanges, Long.MIN_VALUE, Long.MAX_VALUE, globalSyncId, TxnId.NONE, globalSyncId, TxnId.NONE, TxnId.NONE);
        safeStore.upsertRedundantBefore(addShardRedundant);
        updatedRedundantBefore(safeStore, globalSyncId, slicedRanges);
        safeStore = safeStore; // make unusable in lambda

        if (locallyRedundantBefore.compareTo(globalSyncId) < 0)
        {
            // TODO (expected): if bootstrapping only part of the range, mark the rest for GC; or relax this as can safely GC behind bootstrap
            TxnId maxBootstrap = safeStore.redundantBefore().max(slicedRanges, e -> e.bootstrappedAt);
            if (maxBootstrap.compareTo(globalSyncId) >= 0)
                logger.info("Ignoring markShardDurable for a point we are bootstrapping. Bootstrapping: {}, Global: {}, Ranges: {}", maxBootstrap, globalSyncId, slicedRanges);
            else
                logger.warn("Trying to markShardDurable a point we have not yet caught-up to locally. Local: {}, Global: {}, Ranges: {}", locallyRedundantBefore, globalSyncId, slicedRanges);
            return;
        }

        // TODO (desired): not all systems care about HLC_BOUND for GC, make configurable
        if (globalSyncId.is(HLC_BOUND) || !requiresUniqueHlcs())
        {
            safeStore.dataStore().snapshot(slicedRanges, globalSyncId).begin((success, fail) -> {
                if (fail != null)
                {
                    agent.onCaughtException(fail, "Unsuccessful dataStore snapshot; unable to update GC markers");
                    return;
                }

                execute(PreLoadContext.empty(), safeStore0 -> {
                    RedundantBefore addGc = RedundantBefore.create(slicedRanges, Long.MIN_VALUE, Long.MAX_VALUE, globalSyncId, globalSyncId, globalSyncId, globalSyncId, TxnId.NONE);
                    safeStore0.upsertRedundantBefore(addGc);
                }, agent());
            });
        }
    }

    protected void updatedRedundantBefore(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        TxnId clearWaitingBefore = redundantBefore.minShardRedundantBefore();
        TxnId clearAnyBefore = durableBefore().min.majorityBefore;
        progressLog.clearBefore(safeStore, clearWaitingBefore, clearAnyBefore);
        listeners.clearBefore(this, clearWaitingBefore);
    }

    protected void markSynced(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        RedundantBefore newRedundantBefore = RedundantBefore.merge(redundantBefore, RedundantBefore.create(ranges, syncId, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE));
        unsafeSetRedundantBefore(newRedundantBefore);
        updatedRedundantBefore(safeStore, syncId, ranges);

        if (waitingOnSync.isEmpty())
            return;

        LongHashSet remove = null;
        for (Map.Entry<Long, WaitingOnSync> e : waitingOnSync.entrySet())
        {
            if (e.getKey() > syncId.epoch())
                break;

            Ranges remaining = e.getValue().ranges;
            Ranges synced = remaining.slice(ranges, Minimal);
            e.getValue().ranges = remaining = remaining.without(ranges);
            if (e.getValue().ranges.isEmpty())
            {
                logger.debug("Completed full sync for {} on epoch {} using {}", e.getValue().allRanges, e.getKey(), syncId);
                e.getValue().whenDone.trySuccess(null);
                if (remove == null)
                    remove = new LongHashSet();
                remove.add(e.getKey());
            }
            else
            {
                logger.debug("Completed partial sync for {} on epoch {} using {}; {} still to sync", synced, e.getKey(), syncId, remaining);
            }
        }
        if (remove != null)
            remove.forEach(waitingOnSync::remove);
    }

    public void markShardStale(SafeCommandStore safeStore, Timestamp staleSince, Ranges ranges, boolean isSincePrecise)
    {
        Timestamp staleUntilAtLeast = staleSince;
        if (isSincePrecise)
        {
            ranges = ranges.slice(safeStore.ranges().allAt(staleSince.epoch()), Minimal);
        }
        else
        {
            ranges = ranges.slice(safeStore.ranges().allSince(staleSince.epoch()), Minimal);
            // make sure no in-progress bootstrap attempts will override the stale since for commands whose staleness bounds are unknown
            staleUntilAtLeast = Timestamp.max(bootstrapBeganAt.lastKey(), staleUntilAtLeast);
        }
        agent.onStale(staleSince, ranges);

        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, staleUntilAtLeast);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        // find which ranges need to bootstrap, subtracting those already in progress that cover the id

        markUnsafeToRead(ranges);
    }

    // MUST be invoked before CommandStore reference leaks to anyone
    // The integration may have already loaded persisted values for these fields before this is called
    // so it must be a merge for each field with the initialization values. These starting values don't need to be
    // persisted since we can synthesize them at startup every time
    // TODO (review): This needs careful thought about not persisting and that purgeAndInsert is doing the right thing
    // with safeToRead
    Supplier<EpochReady> initialise(long epoch, Ranges ranges)
    {
        return () -> {
            AsyncResult<Void> done = execute(empty(), (safeStore) -> {
                // Merge in a base for any ranges that needs to be covered
                Ranges newBootstrapRanges = ranges;
                for (Ranges existing : bootstrapBeganAt.values())
                    newBootstrapRanges = newBootstrapRanges.without(existing);
                if (!newBootstrapRanges.isEmpty())
                    safeStore.setBootstrapBeganAt(bootstrap(TxnId.NONE, newBootstrapRanges, bootstrapBeganAt));
                safeStore.setSafeToRead(purgeAndInsert(safeToRead, TxnId.NONE, ranges));
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?> participants)
    {
        if (rejectBefore == null)
            return false;

        return rejectBefore.rejects(txnId, participants);
    }

    public final MaxConflicts unsafeGetMaxConflicts()
    {
        return maxConflicts;
    }

    public final RedundantBefore unsafeGetRedundantBefore()
    {
        return redundantBefore;
    }

    @Nullable
    public final RejectBefore unsafeGetRejectBefore()
    {
        return rejectBefore;
    }

    public final DurableBefore durableBefore()
    {
        return node.durableBefore();
    }

    @VisibleForTesting
    public final NavigableMap<TxnId, Ranges> unsafeGetBootstrapBeganAt() { return bootstrapBeganAt; }

    @VisibleForTesting
    public NavigableMap<Timestamp, Ranges> unsafeGetSafeToRead() { return safeToRead; }

    final void markUnsafeToRead(Ranges ranges)
    {
        if (safeToRead.values().stream().anyMatch(r -> r.intersects(ranges)))
        {
            execute(empty(), safeStore -> {
                safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            }, agent);
        }
    }

    final synchronized void markSafeToRead(Timestamp forBootstrapAt, Timestamp at, Ranges ranges)
    {
        execute(empty(), safeStore -> {
            // TODO (required): handle weird edge cases like newer at having a lower HLC than prior existing at, but higher epoch
            Ranges validatedSafeToRead = redundantBefore.validateSafeToRead(forBootstrapAt, ranges);
            safeStore.setSafeToRead(purgeAndInsert(safeToRead, at, validatedSafeToRead));
            updateMaxConflicts(ranges, at);
        }, agent);
    }

    public static ImmutableSortedMap<TxnId, Ranges> bootstrap(TxnId at, Ranges ranges, NavigableMap<TxnId, Ranges> bootstrappedAt)
    {
        Invariants.requireArgument(bootstrappedAt.lastKey().compareTo(at) < 0 || at == TxnId.NONE);
        if (at == TxnId.NONE)
            for (Ranges rs : bootstrappedAt.values())
                Invariants.require(!ranges.intersects(rs));
        Invariants.requireArgument(!ranges.isEmpty());
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        return purgeAndInsert(bootstrappedAt, at, ranges);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> purgeAndInsert(NavigableMap<T, Ranges> in, T insertAt, Ranges insert)
    {
        TreeMap<T, Ranges> build = new TreeMap<>(in);
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().without(insert)));
        build.tailMap(insertAt, true).entrySet().forEach(e -> e.setValue(e.getValue().union(MERGE_ADJACENT, insert)));
        build.entrySet().removeIf(e -> e.getKey().compareTo(Timestamp.NONE) > 0 && e.getValue().isEmpty());
        Map.Entry<T, Ranges> prev = build.floorEntry(insertAt);
        build.putIfAbsent(insertAt, prev.getValue().with(insert));
        return ImmutableSortedMap.copyOf(build);
    }

    private static ImmutableSortedMap<Timestamp, Ranges> purgeHistory(NavigableMap<Timestamp, Ranges> in, Ranges remove)
    {
        return ImmutableSortedMap.copyOf(purgeHistoryIterator(in, remove));
    }

    private static <T extends Timestamp> Iterable<Map.Entry<T, Ranges>> purgeHistoryIterator(NavigableMap<T, Ranges> in, Ranges removeRanges)
    {
        return () -> in.entrySet().stream()
                       .map(e -> without(e, removeRanges))
                       .filter(e -> !e.getValue().isEmpty() || e.getKey().equals(TxnId.NONE))
                       .iterator();
    }

    private static <T extends Timestamp> Map.Entry<T, Ranges> without(Map.Entry<T, Ranges> in, Ranges remove)
    {
        Ranges without = in.getValue().without(remove);
        if (without == in.getValue())
            return in;
        return new SimpleImmutableEntry<>(in.getKey(), without);
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    public boolean isBootstrapping()
    {
        return !bootstraps.isEmpty();
    }

    public static NavigableMap<TxnId, Ranges> emptyBootstrapBeganAt()
    {
        return ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
    }

    public static NavigableMap<Timestamp, Ranges> emptySafeToRead()
    {
        return ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    }
}
