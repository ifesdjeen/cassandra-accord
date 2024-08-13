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

package accord.impl.progresslog;

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.coordinate.AsynchronousAwait;
import accord.coordinate.FetchData;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.Query.HOME;
import static accord.api.ProgressLog.BlockedUntil.Query.SHARD;
import static accord.impl.progresslog.CallbackInvoker.invokeWaitingCallback;
import static accord.impl.progresslog.PackedKeyTracker.bitSet;
import static accord.impl.progresslog.PackedKeyTracker.clearRoundState;
import static accord.impl.progresslog.PackedKeyTracker.initialiseBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundCallbackBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundIndex;
import static accord.impl.progresslog.PackedKeyTracker.roundSize;
import static accord.impl.progresslog.PackedKeyTracker.setBitSet;
import static accord.impl.progresslog.PackedKeyTracker.setMaxRoundIndexAndClearBitSet;
import static accord.impl.progresslog.PackedKeyTracker.setRoundIndexAndClearBitSet;
import static accord.impl.progresslog.TxnStateKind.Waiting;
import static accord.impl.progresslog.WaitingState.CallbackKind.AwaitHome;
import static accord.impl.progresslog.WaitingState.CallbackKind.AwaitSlice;
import static accord.impl.progresslog.WaitingState.CallbackKind.Fetch;
import static accord.impl.progresslog.Progress.Awaiting;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.WaitingState.CallbackKind.FetchRoute;
import static accord.primitives.EpochSupplier.constant;

/**
 * This represents a simple state machine encoded in a small number of bits for efficiently gathering
 * distributed state we require locally to make progress.
 * <p>
 * TODO (required): describe state machine
 */
@SuppressWarnings("CodeBlock2Expr")
abstract class WaitingState extends BaseTxnState
{
    private static final int PROGRESS_SHIFT = 0;
    private static final long PROGRESS_MASK = 0x3;
    private static final int BLOCKED_UNTIL_SHIFT = 2;
    private static final long BLOCKED_UNTIL_MASK = 0x7;
    private static final int HOME_SATISFIES_SHIFT = 5;
    private static final long HOME_SATISFIES_MASK = 0x7;
    private static final int WAITING_AWAIT_SHIFT = 8;
    private static final long WAITING_AWAIT_MASK = (1L << WAITING_AWAIT_BITS) - 1;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT) | (BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT));
    private static final long INITIALISED_MASK = (PROGRESS_MASK << PROGRESS_SHIFT) | (BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT) | (HOME_SATISFIES_MASK << HOME_SATISFIES_SHIFT);

    WaitingState(TxnId txnId)
    {
        super(txnId);
    }

    private void set(DefaultProgressLog owner, BlockedUntil newBlockedUntil, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= ((long) newBlockedUntil.ordinal() << BLOCKED_UNTIL_SHIFT) | ((long) newProgress.ordinal() << PROGRESS_SHIFT);
        updateScheduling(owner, Waiting, newBlockedUntil, newProgress);
    }

    private void setHomeSatisfies(BlockedUntil homeStatus)
    {
        encodedState &= ~(HOME_SATISFIES_MASK << HOME_SATISFIES_SHIFT);
        encodedState |= (long) homeStatus.ordinal() << HOME_SATISFIES_SHIFT;
    }

    boolean isUninitialised()
    {
        return 0 == (encodedState & INITIALISED_MASK);
    }

    @Nonnull BlockedUntil blockedUntil()
    {
        return blockedUntil(encodedState);
    }

    @Nonnull BlockedUntil homeSatisfies()
    {
        return homeSatisfies(encodedState);
    }

    final @Nonnull Progress waitingProgress()
    {
        return waitingProgress(encodedState);
    }

    private static @Nonnull BlockedUntil blockedUntil(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> BLOCKED_UNTIL_SHIFT) & BLOCKED_UNTIL_MASK));
    }

    private static @Nonnull BlockedUntil homeSatisfies(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> HOME_SATISFIES_SHIFT) & HOME_SATISFIES_MASK));
    }

    private static @Nonnull Progress waitingProgress(long encodedState)
    {
        return Progress.forOrdinal((int) ((encodedState >>> PROGRESS_SHIFT) & PROGRESS_MASK));
    }

    private static int awaitRoundSize(Route<?> slicedRoute)
    {
        return roundSize(slicedRoute.size(), WAITING_AWAIT_BITS);
    }

    private void clearAwaitState()
    {
        encodedState = clearRoundState(encodedState, WAITING_AWAIT_SHIFT, WAITING_AWAIT_MASK);
    }

    private int awaitBitSet(int roundSize)
    {
        return bitSet(encodedState, roundSize, WAITING_AWAIT_SHIFT);
    }

    private void initialiseAwaitBitSet(Route<?> route, Unseekables<?> notReady, int roundIndex, int roundSize)
    {
        encodedState = initialiseBitSet(encodedState, route, notReady, roundIndex, roundSize, WAITING_AWAIT_SHIFT);
    }

    private void setAwaitBitSet(int bitSet, int roundSize)
    {
        encodedState = setBitSet(encodedState, bitSet, roundSize, WAITING_AWAIT_SHIFT);
    }

    private int awaitRoundIndex(int roundSize)
    {
        return roundIndex(encodedState, roundSize, WAITING_AWAIT_SHIFT, WAITING_AWAIT_MASK);
    }

    private void updateAwaitRound(int newRoundIndex, int roundSize)
    {
        Invariants.checkArgument(roundSize <= WAITING_AWAIT_BITS);
        encodedState = setRoundIndexAndClearBitSet(encodedState, newRoundIndex, roundSize, WAITING_AWAIT_SHIFT, WAITING_AWAIT_MASK);
    }

    private void setAwaitDone(int roundSize)
    {
        Invariants.checkArgument(roundSize <= WAITING_AWAIT_BITS);
        encodedState = setMaxRoundIndexAndClearBitSet(encodedState, roundSize, WAITING_AWAIT_SHIFT, WAITING_AWAIT_MASK);
    }

    Topologies contact(DefaultProgressLog owner, Unseekables<?> forKeys, long epoch)
    {
        Node node = owner.node();
        Topologies topologies = node.topology().forEpoch(forKeys, epoch);
        return node.agent().selectPreferred(node.id(), topologies);
    }

    private static EpochSupplier toLocalEpoch(SafeCommandStore safeStore, TxnId txnId, BlockedUntil blockedUntil, Command command, Timestamp executeAt)
    {
        long epoch = blockedUntil.fetchEpoch(txnId, executeAt);
        if (command.route() != null)
            epoch = Math.max(epoch, safeStore.ranges().latestEpochWithNewParticipants(epoch, command.route()));
        if (command.additionalKeysOrRanges() != null)
            epoch = Math.max(epoch, safeStore.ranges().latestEpochWithNewParticipants(epoch, command.additionalKeysOrRanges()));
        return constant(epoch);
    }

    private static Route<?> slicedRoute(SafeCommandStore safeStore, TxnId txnId, Command command, BlockedUntil blockedUntil)
    {
        Timestamp executeAt = command.executeAtIfKnown();
        EpochSupplier toLocalEpoch = toLocalEpoch(safeStore, txnId, blockedUntil, command, executeAt);

        Ranges ranges = safeStore.ranges().allBetween(txnId.epoch(), toLocalEpoch);
        return command.route().slice(ranges);
    }

    private static Route<?> slicedRoute(SafeCommandStore safeStore, TxnId txnId, Route<?> route, EpochSupplier toLocalEpoch)
    {
        Ranges ranges = safeStore.ranges().allBetween(txnId.epoch(), toLocalEpoch);
        return route.slice(ranges);
    }

    private static Route<?> awaitRoute(Route<?> slicedRoute, BlockedUntil blockedUntil)
    {
        return blockedUntil.waitsOn == HOME ? slicedRoute.homeKeyOnlyRoute() : slicedRoute;
    }

    private static Route<?> fetchRoute(Route<?> slicedRoute, Route<?> awaitRoute, BlockedUntil blockedUntil)
    {
        return blockedUntil.waitsOn == blockedUntil.fetchFrom ? awaitRoute : slicedRoute;
    }

    void setWaitingDone(DefaultProgressLog owner)
    {
        set(owner, CanApply, NoneExpected);
        owner.clearActive(Waiting, txnId);
        clearRetryCounter();
    }

    void setWaitingDoneAndMaybeRemove(DefaultProgressLog owner)
    {
        setWaitingDone(owner);
        maybeRemove(owner);
    }

    void setBlockedUntil(DefaultProgressLog owner, BlockedUntil blockedUntil)
    {
        if (txnId.toString().equals("[5,5003,7(RS),7]") && owner.commandStore.toString().equals("DelayedCommandStore{id=2,node=1}"))
            System.out.println();

        BlockedUntil currentlyBlockedUntil = blockedUntil();
        if (blockedUntil.compareTo(currentlyBlockedUntil) > 0 || isUninitialised())
        {
            clearAwaitState();
            clearRetryCounter();
            owner.clearActive(Waiting, txnId);
            set(owner, blockedUntil, Queued);
        }
    }

    void record(DefaultProgressLog owner, Command command)
    {
        BlockedUntil currentlyBlockedUntil = blockedUntil();
        if (currentlyBlockedUntil.minSaveStatus.compareTo(command.saveStatus()) <= 0)
        {
            set(owner, currentlyBlockedUntil, NoneExpected);
            owner.clearActive(Waiting, txnId);
        }
    }

    final void runWaiting(DefaultProgressLog owner, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        run(owner, safeStore, safeCommand);
    }

    private void run(DefaultProgressLog owner, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        BlockedUntil blockedUntil = blockedUntil();
        Command command = safeCommand.current();
        Invariants.checkState(!owner.hasActive(Waiting, txnId));
        Invariants.checkState(command.saveStatus().compareTo(blockedUntil.minSaveStatus) < 0, "Command has met desired criteria but progress log entry has not been cancelled");

        set(owner, blockedUntil, Querying);
        TxnId txnId = safeCommand.txnId();
        // first make sure we have enough information to obtain the command locally
        Timestamp executeAt = command.executeAtIfKnown();
        EpochSupplier toLocalEpoch = toLocalEpoch(safeStore, txnId, blockedUntil, command, executeAt);
        Participants<?> fetchKeys = Invariants.nonNull(command.maxContactable());

        if (!Route.isRoute(fetchKeys))
        {
            fetchRoute(owner, blockedUntil, txnId, executeAt, toLocalEpoch, fetchKeys);
            return;
        }

        Route<?> route = Route.castToRoute(fetchKeys);
        if (homeSatisfies().compareTo(blockedUntil) < 0)
        {
            awaitHomeKey(owner, blockedUntil, txnId, executeAt, route);
            return;
        }

        Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, toLocalEpoch);
        if (!command.hasBeen(Status.PreCommitted))
        {
            // we know it has been decided one way or the other by the home shard at least, so we attempt a fetch
            // including the home shard to get us to at least PreCommitted where we can safely wait on individual shards
            fetch(owner, blockedUntil, txnId, executeAt, toLocalEpoch, slicedRoute, slicedRoute.withHomeKey());
            return;
        }

        Route<?> awaitRoute = awaitRoute(slicedRoute, blockedUntil);
        Route<?> fetchRoute = fetchRoute(slicedRoute, awaitRoute, blockedUntil);

        if (slicedRoute.size() == 0 || awaitRoute.isHomeKeyOnlyRoute())
        {
            // at this point we can switch to polling as we know someone has the relevant state
            fetch(owner, blockedUntil, txnId, executeAt, toLocalEpoch, slicedRoute, fetchRoute);
            return;
        }

        int roundSize = awaitRoundSize(awaitRoute);
        int roundIndex = awaitRoundIndex(roundSize);
        int roundStart = roundIndex * roundSize;
        if (roundStart >= awaitRoute.size())
        {
            // at this point we can switch to polling as we know someone has the relevant state
            fetch(owner, blockedUntil, txnId, executeAt, toLocalEpoch, slicedRoute, fetchRoute);
            return;
        }

        int roundEnd = Math.min(roundStart + roundSize, awaitRoute.size());
        awaitRoute = awaitRoute.slice(roundStart, roundEnd);
        // TODO (desired): use some mechanism (e.g. random chance or another counter)
        //   to either periodically fetch the whole remaining route or gradually increase the slice length
        awaitSlice(owner, blockedUntil, txnId, executeAt, awaitRoute, (roundIndex << 1) | 1);
    }

    // note that ready and notReady may include keys not requested by this progressLog
    static void awaitOrFetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, Unseekables<?> ready, @Nullable Unseekables<?> notReady, @Nullable BlockedUntil upgrade, Throwable fail)
    {
        WaitingState state = owner.get(txnId);
        Invariants.checkState(state != null, "State has been cleared but callback was not cancelled");

        Invariants.checkState(state.waitingProgress() == Querying);
        Invariants.checkState(state.blockedUntil() == blockedUntil);

        Command command = safeCommand.current();
        Route<?> route = command.route();

        if (fail == null)
        {
            if (route == null)
            {
                Invariants.checkState(kind == FetchRoute);
                state.retry(owner, safeStore, safeCommand, blockedUntil);
                return;
            }

            if (ready.contains(route.homeKey()) && blockedUntil.compareTo(state.homeSatisfies()) > 0)
            {
                // TODO (expected): we can introduce an additional home state check that waits until DURABLE for execution;
                //  at this point it would even be redundant to wait for each separate shard for the WaitingState? Freeing up bits and simplifying.
                BlockedUntil newHomeSatisfies = blockedUntil;
                if (upgrade != null && upgrade.compareTo(newHomeSatisfies) > 0)
                    newHomeSatisfies = upgrade;
                state.setHomeSatisfies(newHomeSatisfies);
            }

            EpochSupplier toLocalEpoch = toLocalEpoch(safeStore, txnId, blockedUntil, command, command.executeAtOrTxnId());
            Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, toLocalEpoch); // the actual local keys we care about
            Route<?> awaitRoute = awaitRoute(slicedRoute, blockedUntil); // either slicedRoute or just the home key

            int roundSize = awaitRoundSize(awaitRoute);
            int roundIndex = state.awaitRoundIndex(roundSize);
            int roundStart = roundIndex * roundSize;

            switch (kind)
            {
                default: throw new AssertionError("Unhandled CallbackKind: " + kind);

                case AwaitHome:
                    if (notReady == null)
                    {
                        Invariants.checkState(0 == state.awaitRoundIndex(roundSize));
                        Invariants.checkState(0 == state.awaitBitSet(roundSize));
                        state.run(owner, safeStore, safeCommand);
                    }
                    else
                    {
                        state.set(owner, blockedUntil, Awaiting);
                    }
                    break;

                case AwaitSlice:
                    Invariants.checkState(awaitRoute == slicedRoute);
                    if (notReady == null)
                    {
                        Invariants.checkState((int) awaitRoute.findNextIntersection(roundStart, (Routables) ready, 0) / roundSize == roundIndex);
                        // TODO (desired): in this case perhaps upgrade to fetch for next round?
                        state.updateAwaitRound(roundIndex + 1, roundSize);
                        state.run(owner, safeStore, safeCommand);
                    }
                    else
                    {
                        Invariants.checkState((int) awaitRoute.findNextIntersection(roundStart, (Routables) notReady, 0) / roundSize == roundIndex);
                        // TODO (desired): would be nice to validate this is 0 in cases where we are starting a fresh round
                        //  but have to be careful as cannot zero when we restart as we may have an async callback arrive while we're waiting that then advances state machine
                        state.initialiseAwaitBitSet(awaitRoute, notReady, roundIndex, roundSize);
                        state.set(owner, blockedUntil, Awaiting);
                    }
                    break;

                case FetchRoute:
                    if (state.homeSatisfies().compareTo(blockedUntil) < 0)
                    {
                        state.run(owner, safeStore, safeCommand);
                        return;
                    }
                    // we may not have requested everything since we didn't have a Route, so calculate our own notReady and fall-through
                    notReady = slicedRoute.without(ready);

                case Fetch:
                {
                    Invariants.checkState(notReady != null, "Fetch was successful for all keys, but the WaitingState has not been cleared");
                    Invariants.checkState(notReady.intersects(slicedRoute), "Fetch was successful for all keys, but the WaitingState has not been cleared");
                    int nextIndex;
                    if (roundStart >= awaitRoute.size()) nextIndex = -1;
                    else if (slicedRoute == awaitRoute) nextIndex = (int) awaitRoute.findNextIntersection(roundStart, (Routables) notReady, 0);
                    else
                    {
                        Invariants.checkState(roundIndex == 0);
                        nextIndex = 0;
                    }

                    if (nextIndex < 0)
                    {
                        // we don't think we have anything to wait for, but we have encountered some notReady responses; queue up a retry
                        state.setAwaitDone(roundSize);
                        state.retry(owner, safeStore, safeCommand, blockedUntil);
                    }
                    else
                    {
                        Invariants.checkState(nextIndex >= roundStart);
                        roundIndex = nextIndex / roundSize;
                        state.updateAwaitRound(roundIndex, roundSize);
                        state.initialiseAwaitBitSet(awaitRoute, notReady, roundIndex, roundSize);
                        state.run(owner, safeStore, safeCommand);
                    }
                }
            }
        }
        else
        {
            state.retry(owner, safeStore, safeCommand, blockedUntil);
        }
    }

    static void fetchRouteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        fetchCallback(FetchRoute, safeStore, safeCommand, owner, txnId, blockedUntil, fetchResult, fail);
    }

    static void fetchCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        fetchCallback(Fetch, safeStore, safeCommand, owner, txnId, blockedUntil, fetchResult, fail);
    }

    static void fetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        Invariants.checkState(fetchResult != null || fail != null);
        Unseekables<?> ready = fetchResult == null ? null : fetchResult.achievedTarget;
        Unseekables<?> notReady = fetchResult == null ? null : fetchResult.didNotAchieveTarget;
        BlockedUntil upgrade = fetchResult == null ? null : BlockedUntil.forSaveStatus(fetchResult.achieved.propagatesSaveStatus());
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, blockedUntil, ready, notReady, upgrade, fail);
    }

    static void synchronousAwaitHomeCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        synchronousAwaitCallback(AwaitHome, safeStore, safeCommand, owner, txnId, blockedUntil, awaitResult, fail);
    }

    static void synchronousAwaitSliceCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        synchronousAwaitCallback(AwaitSlice, safeStore, safeCommand, owner, txnId, blockedUntil, awaitResult, fail);
    }

    static void synchronousAwaitCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        Unseekables<?> ready = awaitResult == null ? null : awaitResult.ready;
        Unseekables<?> notReady = awaitResult == null ? null : awaitResult.notReady;
        // TODO (desired): extract "upgrade" info from AsynchronousAwait
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, blockedUntil, ready, notReady, null, fail);
    }
    
    void asynchronousAwaitCallback(DefaultProgressLog owner, SafeCommandStore safeStore, SaveStatus newStatus, Node.Id from, int callbackId)
    {
        if ((callbackId & 1) != 1)
            return;

        BlockedUntil blockedUntil = blockedUntil();
        if (callbackId == Integer.MAX_VALUE)
        {
            // homeKey reply
            BlockedUntil currentHomeStatus = homeSatisfies();
            BlockedUntil newHomeStatus = BlockedUntil.forSaveStatus(newStatus);
            if (newHomeStatus.compareTo(currentHomeStatus) > 0)
                setHomeSatisfies(newHomeStatus);

            if (waitingProgress() != Awaiting)
                return;

            if (newHomeStatus.compareTo(blockedUntil) < 0 || currentHomeStatus.compareTo(blockedUntil) >= 0)
                return;

            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            if (safeCommand != null)
                run(owner, safeStore, safeCommand);
        }
        else
        {
            if (waitingProgress() != Awaiting)
                return;

            callbackId >>= 1;
            SafeCommand safeCommand = Invariants.nonNull(safeStore.unsafeGet(txnId));
            Route<?> slicedRoute = slicedRoute(safeStore, txnId, safeCommand.current(), blockedUntil);

            int roundSize = awaitRoundSize(slicedRoute);
            int roundIndex = awaitRoundIndex(roundSize);
            int updateBitSet = roundCallbackBitSet(owner, txnId, from, slicedRoute, callbackId, roundIndex, roundSize);
            if (updateBitSet == 0)
                return;

            int bitSet = awaitBitSet(roundSize);
            bitSet &= ~updateBitSet;
            setAwaitBitSet(bitSet, roundSize);

            if (bitSet == 0)
                run(owner, safeStore, safeCommand);
        }
    }

    // TODO (expected): use back-off counter here
    private void retry(DefaultProgressLog owner, SafeCommandStore safeStore, SafeCommand safeCommand, BlockedUntil blockedUntil)
    {
        if (!contactEveryone())
        {
            setContactEveryone(true);
            // try again immediately with a query to all eligible replicas
            run(owner, safeStore, safeCommand);
        }
        else
        {
            // queue a retry
            set(owner, blockedUntil, Queued);
        }
    }

    static void fetchRoute(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, EpochSupplier toLocalEpoch, Participants<?> fetchKeys)
    {
        // TODO (desired): fetch only the route
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        BiConsumer<FetchData.FetchResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, WaitingState::fetchRouteCallback);
        owner.node().withEpoch(blockedUntil.fetchEpoch(txnId, executeAt), invoker, () -> {
            FetchData.fetch(blockedUntil.minSaveStatus.known, owner.node(), txnId, fetchKeys, toLocalEpoch, executeAt, invoker);
        });
    }

    static void fetch(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, EpochSupplier toLocalEpoch, Route<?> slicedRoute, Route<?> fetchRoute)
    {
        Invariants.checkState(!slicedRoute.isEmpty());
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        BiConsumer<FetchData.FetchResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, WaitingState::fetchCallback);
        owner.node().withEpoch(blockedUntil.fetchEpoch(txnId, executeAt), invoker, () -> {
            FetchData.fetchExact(blockedUntil.minSaveStatus.known, owner.node(), txnId, fetchRoute, slicedRoute, toLocalEpoch, executeAt, invoker);
        });
    }

    void awaitHomeKey(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route)
    {
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route.homeKeyOnlyRoute(), Integer.MAX_VALUE, WaitingState::synchronousAwaitHomeCallback);
    }

    void awaitSlice(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId)
    {
        Invariants.checkState(blockedUntil.waitsOn == SHARD);
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route, callbackId, WaitingState::synchronousAwaitSliceCallback);
    }

    void await(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId, Callback<BlockedUntil, AsynchronousAwait.SynchronousResult> callback)
    {
        long epoch = blockedUntil.fetchEpoch(txnId, executeAt);
        // we MUST allocate the invoker before invoking withEpoch as this may be asynchronous and we must first register our callback for cancellation
        BiConsumer<AsynchronousAwait.SynchronousResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, callback);
        owner.node().withEpoch(epoch, invoker, () -> {
            AsynchronousAwait.awaitAny(owner.node(), contact(owner, route, epoch), txnId, route, blockedUntil, callbackId, invoker);
        });
    }

    String toStateString()
    {
        BlockedUntil blockedUntil = blockedUntil();
        Progress progress = waitingProgress();
        switch (progress)
        {
            default:
                throw new AssertionError("Unhandled Progress: " + progress);
            case NoneExpected:
                return blockedUntil == CanApply ? "Done" : "NotWaiting";
            case Queued:
                return "Queued(" + blockedUntil + ")";
            case Querying:
                return "Querying(" + blockedUntil + ")";
            case Awaiting:
                return "Awaiting(" + blockedUntil + ")";
        }
    }

    boolean isWaitingDone()
    {
        return waitingProgress() == NoneExpected && blockedUntil() == CanApply;
    }

    enum CallbackKind
    {
        Fetch, FetchRoute, AwaitHome, AwaitSlice
    }
}
