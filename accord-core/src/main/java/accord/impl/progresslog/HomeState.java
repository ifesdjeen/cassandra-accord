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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.coordinate.AsynchronousAwait;
import accord.coordinate.MaybeRecover;
import accord.coordinate.Outcome;
import accord.coordinate.RecoverWithRoute;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.FullRoute;
import accord.primitives.ProgressToken;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import static accord.api.ProgressLog.BlockedUntil.CanCoordinateExecution;
import static accord.impl.progresslog.CallbackInvoker.invokeHomeCallback;
import static accord.impl.progresslog.CoordinatePhase.AwaitReadyToExecute;
import static accord.impl.progresslog.CoordinatePhase.NotInitialisedOrDone;
import static accord.impl.progresslog.CoordinatePhase.ReadyToExecute;
import static accord.impl.progresslog.CoordinatorActivity.Monitoring;
import static accord.impl.progresslog.CoordinatorActivity.NotInitialised;
import static accord.impl.progresslog.PackedKeyTracker.bitSet;
import static accord.impl.progresslog.PackedKeyTracker.clearRoundState;
import static accord.impl.progresslog.PackedKeyTracker.initialiseBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundCallbackBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundIndex;
import static accord.impl.progresslog.PackedKeyTracker.roundSize;
import static accord.impl.progresslog.PackedKeyTracker.setBitSet;
import static accord.impl.progresslog.PackedKeyTracker.setRoundIndexAndClearBitSet;
import static accord.impl.progresslog.Progress.Awaiting;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.utils.Invariants.illegalState;

/**
 * TODO (required): describe state machine
 *
 * TODO (desired): can we merge the two await logics, so we use WaitingState when HomeState needs to wait?
 */
abstract class HomeState extends WaitingState
{
    private static final int COORDINATE_PROGRESS_SHIFT = 24;
    private static final long COORDINATE_PROGRESS_MASK = 0x3;
    private static final int COORDINATE_STATUS_SHIFT = 26;
    private static final long COORDINATE_STATUS_MASK = 0x3;
    private static final int COORDINATE_ACTIVITY_SHIFT = 28;
    private static final long COORDINATE_ACTIVITY_MASK = 0x3;
    private static final int COORDINATE_AWAIT_SHIFT = 30;
    private static final int COORDINATE_AWAIT_MASK = (1 << COORDINATE_AWAIT_BITS) - 1;
    private static final long SET_MASK = ~((COORDINATE_PROGRESS_MASK << COORDINATE_PROGRESS_SHIFT)
                                           | (COORDINATE_STATUS_MASK << COORDINATE_STATUS_SHIFT)
                                           | (COORDINATE_ACTIVITY_MASK << COORDINATE_ACTIVITY_SHIFT));

    HomeState(TxnId txnId)
    {
        super(txnId);
    }

    void set(DefaultProgressLog instance, CoordinatePhase newCoordinatePhase, CoordinatorActivity newActivity, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= (newCoordinatePhase.ordinal() << COORDINATE_STATUS_SHIFT)
                        | (newProgress.ordinal() << COORDINATE_PROGRESS_SHIFT)
                        | (newActivity.ordinal() << COORDINATE_ACTIVITY_SHIFT);

        if (newProgress == NoneExpected)
            instance.clearProgressToken(txnId);
        updateScheduling(instance, Home, CanCoordinateExecution, newProgress);
    }

    @Nonnull CoordinatePhase phase()
    {
        return phase(encodedState);
    }

    final @Nonnull Progress homeProgress()
    {
        return homeProgress(encodedState);
    }

    @Nonnull CoordinatorActivity activity()
    {
        return activity(encodedState);
    }

    private static @Nonnull CoordinatePhase phase(long encodedState)
    {
        return CoordinatePhase.forOrdinal((int) ((encodedState >>> COORDINATE_STATUS_SHIFT) & COORDINATE_STATUS_MASK));
    }

    private static @Nonnull Progress homeProgress(long encodedState)
    {
        return Progress.forOrdinal((int) ((encodedState >>> COORDINATE_PROGRESS_SHIFT) & COORDINATE_PROGRESS_MASK));
    }

    private static @Nonnull CoordinatorActivity activity(long encodedState)
    {
        return CoordinatorActivity.forOrdinal((int) ((encodedState >>> COORDINATE_ACTIVITY_SHIFT) & COORDINATE_ACTIVITY_MASK));
    }

    private static int coordinatorAwaitRoundSize(FullRoute<?> route)
    {
        return roundSize(route.size(), COORDINATE_AWAIT_BITS);
    }

    private int coordinatorAwaitBitSet(int roundSize)
    {
        return bitSet(encodedState, roundSize, COORDINATE_AWAIT_SHIFT);
    }

    private void initialiseCoordinatorAwaitBitSet(Route<?> route, Unseekables<?> notReady, int roundIndex, int roundSize)
    {
        encodedState = initialiseBitSet(encodedState, route, notReady, roundIndex, roundSize, COORDINATE_AWAIT_SHIFT);
    }

    private void setCoordinatorAwaitBitSet(int bitSet, int roundSize)
    {
        encodedState = setBitSet(encodedState, bitSet, roundSize, COORDINATE_AWAIT_SHIFT);
    }

    private int coordinatorAwaitRoundIndex(int roundSize)
    {
        return roundIndex(encodedState, roundSize, COORDINATE_AWAIT_SHIFT, COORDINATE_AWAIT_MASK);
    }

    private void updateCoordinatorAwaitRound(int newCounter, int roundSize)
    {
        Invariants.checkArgument(roundSize <= COORDINATE_AWAIT_BITS);
        encodedState = setRoundIndexAndClearBitSet(encodedState, newCounter, roundSize, COORDINATE_AWAIT_SHIFT, COORDINATE_AWAIT_MASK);
    }

    void atLeast(DefaultProgressLog instance, CoordinatePhase newPhase, Progress newProgress)
    {
        if (phase() == NotInitialisedOrDone && activity() != NotInitialised)
            return;

        if (newPhase.compareTo(phase()) > 0)
        {
            CoordinatorActivity activity = activity();
            if (activity == NotInitialised) activity = Monitoring;
            instance.clearActive(Home, txnId);
            clearRetryCounter();
            encodedState = clearRoundState(encodedState, COORDINATE_AWAIT_SHIFT, COORDINATE_AWAIT_MASK);
            set(instance, newPhase, activity, newProgress);
        }
    }

    void durable(DefaultProgressLog instance)
    {
        switch (phase())
        {
            default:
                throw new IllegalStateException();
            case NotInitialisedOrDone:
            case Undecided:
            case AwaitReadyToExecute:
            case ReadyToExecute:
                setHomeDoneAnyMaybeRemove(instance);
        }
    }

    void run(DefaultProgressLog instance, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Invariants.checkState(phase() != NotInitialisedOrDone);
        Command command = safeCommand.current();
        Invariants.checkState(!safeStore.isTruncated(command), "Command %s is truncated", command);

        // TODO (expected): when invalidated, safer to maintain HomeState until known to be globally invalidated
        // TODO (now): validate that we clear HomeState when we receive a Durable reply, to replace the token check logic
        Invariants.checkState(!command.durability().isDurableOrInvalidated(), "Command is durable or invalidated, but we have not cleared the ProgressLog");

        CoordinatorActivity activity = activity();
        CoordinatePhase phase = phase();
        switch (activity)
        {
            default:
            case NotInitialised:
                throw illegalState("Unexpected activity: " + activity);

            case Coordinating:
            {
                FullRoute<?> route = Route.tryCastToFullRoute(command.route());
                Invariants.checkState(route != null);
                switch (phase)
                {
                    default:
                    case NotInitialisedOrDone:
                        throw illegalState("Unexpected status: " + phase);

                    case AwaitReadyToExecute:
                        // TODO (expected): introduce an initial round where we contact everyone without registering
                        //  callbacks to establish the first key we're waiting on
                        int roundSize = coordinatorAwaitRoundSize(route);
                        int roundIndex = coordinatorAwaitRoundIndex(roundSize);
                        // should only be queued when are not waiting for anything
                        Invariants.checkState(coordinatorAwaitBitSet(roundSize) == 0 || homeProgress() != Queued);
                        int roundStart = roundSize * roundIndex;
                        int roundEnd = Math.min((roundSize + 1) * roundIndex, route.size());
                        Route<?> awaitOn = route.slice(roundStart, roundEnd);
                        AsynchronousAwait.awaitAny(instance.node(), instance.node().topology().forEpoch(awaitOn, txnId.epoch()),
                                                   txnId, awaitOn, CanCoordinateExecution, roundIndex << 1, invokeHomeCallback(instance, txnId, null, HomeState::synchronousAwaitCallback));
                        break;

                    case ReadyToExecute:
                        // TODO (expected): introduce a coordinator level Deps+Txn cache so we can jump straight to execution for recently paused transactions
                    case Undecided:
                        RecoverWithRoute.recover(instance.node(), txnId, route, null, invokeHomeCallback(instance, txnId, null, HomeState::recoverCallback));
                }
                set(instance, ReadyToExecute, CoordinatorActivity.Coordinating, Querying);
                break;
            }

            case Monitoring:
            {
                // just check the replica we believe is coordinating still is
                // TODO (required): just check the replica we believe is coordinating still is
            }
            case Volunteering:
            {
                ProgressToken maxProgressToken = instance.savedProgressToken(txnId).merge(command);
                MaybeRecover.maybeRecover(instance.node(), txnId, command.route(), maxProgressToken, invokeHomeCallback(instance, txnId, maxProgressToken, HomeState::recoverCallback));
                set(instance, ReadyToExecute, activity(), Querying);
            }
        }
    }

    static void recoverCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog instance, TxnId txnId, @Nullable ProgressToken prevProgressToken, Outcome success, Throwable fail)
    {
        HomeState state = instance.get(txnId);
        if (state == null)
            return;

        CoordinatePhase status = state.phase();
        if (status.isAtMostReadyToExecute() && state.homeProgress() == Querying)
        {
            if (fail != null)
            {
                state.set(instance, status, state.activity(), Queued);
            }
            else
            {
                Command command = safeCommand.current();
                ProgressToken token = success.asProgressToken().merge(command);
                if (prevProgressToken != null)
                    token = token.merge(prevProgressToken);

                if (token.durability.isDurableOrInvalidated())
                {
                    state.setHomeDoneAnyMaybeRemove(instance);
                }
                else
                {
                    if (prevProgressToken != null && token.compareTo(command) > 0)
                        instance.saveProgressToken(command.txnId(), token);
                    state.set(instance, status, state.activity(), Queued);
                }
            }
        }
    }

    static void synchronousAwaitCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog instance, TxnId txnId, Object ignore, AsynchronousAwait.SynchronousResult success, Throwable fail)
    {
        HomeState state = instance.get(txnId);
        if (state == null)
            return;

        CoordinatePhase phase = state.phase();
        if (phase == AwaitReadyToExecute && state.homeProgress() == Querying)
        {
            Invariants.checkState(state.activity() == CoordinatorActivity.Coordinating);
            FullRoute<?> route = Route.castToFullRoute(safeCommand.current().route());
            int roundSize = coordinatorAwaitRoundSize(route);
            int roundIndex = state.coordinatorAwaitRoundIndex(roundSize);

            if (fail != null)
            {
                state.set(instance, AwaitReadyToExecute, CoordinatorActivity.Coordinating, Queued);
            }
            else if (success.notReady == null)
            {
                Invariants.checkState(state.coordinatorAwaitBitSet(roundSize) == 0, "Expect an uninitialised awaitBitSet when entering awaitCallback");
                state.updateCoordinatorAwaitRound(roundIndex + 1, roundSize);
                state.run(instance, safeStore, safeCommand);
            }
            else
            {
                state.initialiseCoordinatorAwaitBitSet(route, success.notReady, roundIndex, roundSize);
                state.set(instance, AwaitReadyToExecute, CoordinatorActivity.Coordinating, Awaiting);
            }
        }
    }

    @Override
    void asynchronousAwaitCallback(DefaultProgressLog instance, SafeCommandStore safeStore, SaveStatus newStatus, Node.Id from, int callbackId)
    {
        super.asynchronousAwaitCallback(instance, safeStore, newStatus, from, callbackId);
        if (phase() != AwaitReadyToExecute || homeProgress() != Awaiting)
            return;

        if ((callbackId & 1) != 0)
            return;

        callbackId >>>= 1;
        SafeCommand safeCommand = Invariants.nonNull(safeStore.unsafeGet(txnId));
        Command command = safeCommand.current();
        FullRoute<?> route = Route.castToFullRoute(command.route());
        int roundSize = coordinatorAwaitRoundSize(route);
        int roundIndex = coordinatorAwaitRoundIndex(roundSize);
        int updateBitSet = roundCallbackBitSet(instance, txnId, from, route, callbackId, roundIndex, roundSize);
        if (updateBitSet == 0)
            return;

        int bitSet = coordinatorAwaitBitSet(roundSize);
        bitSet &= ~updateBitSet;
        setCoordinatorAwaitBitSet(bitSet, roundSize);

        if (bitSet == 0)
            run(instance, safeStore, safeCommand);
    }

    void setHomeDone(DefaultProgressLog instance)
    {
        set(instance, NotInitialisedOrDone, Monitoring, NoneExpected);
        clearRetryCounter();
        instance.clearActive(Home, txnId);
    }

    private void setHomeDoneAnyMaybeRemove(DefaultProgressLog instance)
    {
        setHomeDone(instance);
        maybeRemove(instance);
    }

    @Override
    public String toStateString()
    {
        return (isHomeUninitialised() ? "" : isHomeDone() ? "Done; " : "{" + phase() + ',' + activity() + ',' + homeProgress() + "}; ") + super.toStateString();
    }

    boolean isHomeDone()
    {
        return phase() == NotInitialisedOrDone && activity() != NotInitialised;
    }

    private boolean isHomeUninitialised()
    {
        return phase() == NotInitialisedOrDone && activity() == NotInitialised;
    }
}
