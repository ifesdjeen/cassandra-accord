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

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.LogGroupTimers;

import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;

/**
 * We have a slightly odd class hierarchy here to cleanly group the logic, though TxnState can logically
 * be read as a single class.
 * <p>
 * We have:
 * - BaseTxnState defines the state that each of the child classes operate on and provides shared
 *   methods for updating it, particularly with relation to tracking timer scheduling
 * - WaitingState defines the methods for updating the WaitingState state machine (i.e. for local commands that are waiting on a dependency's outcome)
 * - HomeState defines the methods for updating the HomeState state machine (i.e. for coordinators ensuring a transaction completes)
 * <p>
 * TODO (required): unit test all bit fiddling methods
 */
abstract class BaseTxnState extends LogGroupTimers.Timer implements Comparable<BaseTxnState>
{
    private static final int PENDING_TIMER_LOW_SHIFT = 48;
    private static final int PENDING_TIMER_LOW_BITS = 4; // can safely free up at least 6 bits here if we need some later
    private static final long PENDING_TIMER_LOW_MASK = (1 << PENDING_TIMER_LOW_BITS) - 1;
    private static final int PENDING_TIMER_HIGH_SHIFT = 52;
    private static final long PENDING_TIMER_HIGH_MASK = (1 << 5) - 1;
    private static final long PENDING_TIMER_CLEAR_MASK = ~((PENDING_TIMER_HIGH_MASK << PENDING_TIMER_HIGH_SHIFT)
                                                           | (PENDING_TIMER_LOW_MASK << PENDING_TIMER_LOW_SHIFT));
    private static final int SCHEDULED_TIMER_SHIFT = 58;
    private static final int RETRY_COUNTER_SHIFT = 59;
    private static final long RETRY_COUNTER_MASK = 0xf;
    private static final int CONTACT_ALL_SHIFT = 63;

    public final TxnId txnId;

    /**
     * bits [0..22) encode BlockingState
     * 2 bits for BlockedUntil target
     * 2 bits for BlockedUntil that home shard can satisfy
     * 2 bits for Progress
     * 16 bits for remote progress key counter [note: if we need to in future we can safely and easily reclaim bits here]
     * <p>
     * bits [22..48) encode HomeState
     * 2 bits for CoordinatePhase
     * 2 bits for CoordinatorActivity
     * 2 bits for Progress
     * 20 bits for remote progress key bitset/counter  [note: if we need to in future we can safely and easily reclaim bits here]
     * <p>
     * bits [48..57) for pending timer delay
     * bit 57 for queued timer
     * bits [58..62) for fallback queue delay back-off counter
     * bit 62 for whether the scheduled timer is a fallback timeout (primarily to validate our remote callbacks are still registered)
     * bit 63 for whether we should contact all candidate replicas (rather than just our preferred group)
     */
    long encodedState;

    BaseTxnState(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public int compareTo(BaseTxnState that)
    {
        return this.txnId.compareTo(that.txnId);
    }

    boolean contactEveryone()
    {
        return ((encodedState >>> CONTACT_ALL_SHIFT) & 1L) == 1L;
    }

    void setContactEveryone(boolean newContactEveryone)
    {
        encodedState &= encodedState & ~(1L << CONTACT_ALL_SHIFT);
        encodedState |= (newContactEveryone ? 1L : 0) << CONTACT_ALL_SHIFT;
    }

    // TODO (expected): separate retryCounter for Home and Waiting
    int retryCounter()
    {
        return (int) ((encodedState >>> RETRY_COUNTER_SHIFT) & RETRY_COUNTER_MASK);
    }

    void incrementRetryCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        long current = encodedState & shiftedMask;
        long updated = Math.min(shiftedMask, current + (1L << RETRY_COUNTER_SHIFT));
        encodedState &= ~shiftedMask;
        encodedState |= updated;
    }

    void clearRetryCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        encodedState &= ~shiftedMask;
    }

    /**
     * The state that has a timer actively scheduled, if any
     */
    TxnStateKind scheduledTimer()
    {
        if (!isScheduled())
            return null;
        return wasScheduledTimer();
    }

    /**
     * The state that last had a timer actively scheduled
     */
    TxnStateKind wasScheduledTimer()
    {
        return ((encodedState >>> SCHEDULED_TIMER_SHIFT) & 1) == 0 ? Waiting : Home;
    }

    /**
     * Set the state that has a timer actively scheduled
     */
    void setScheduledTimer(TxnStateKind kind)
    {
        encodedState &= ~(1L << SCHEDULED_TIMER_SHIFT);
        encodedState |= (long) kind.ordinal() << SCHEDULED_TIMER_SHIFT;
        Invariants.checkState(wasScheduledTimer() == kind);
    }

    /**
     * The state that has a timer pending, if any
     */
    TxnStateKind pendingTimer()
    {
        int pendingDelay = pendingTimerDelay();
        if (pendingDelay == 0)
            return null;
        return ((encodedState >>> SCHEDULED_TIMER_SHIFT) & 1) == 0 ? Home : Waiting;
    }

    void clearPendingTimerDelay()
    {
        encodedState &= PENDING_TIMER_CLEAR_MASK;
    }

    void setPendingTimerDelay(int deltaMicros)
    {
        Invariants.checkState(deltaMicros > 0);
        int highestSetBit = Integer.highestOneBit(deltaMicros);
        int highBits = Integer.numberOfLeadingZeros(highestSetBit) & 31;

        int granularityShift = Math.max(0, (highBits - PENDING_TIMER_LOW_BITS));
        // we increment by half the granularity of the low bits so that we round to the nearest rather than round down
        // this is to better handle rescheduling, so that we don't reduce the scheduling delay each time
        long lowBits = Math.min((deltaMicros - highestSetBit + (1 << granularityShift) / 2) >>> granularityShift, PENDING_TIMER_LOW_MASK);

        clearPendingTimerDelay();
        encodedState |= ((long) highBits << PENDING_TIMER_HIGH_SHIFT) | (lowBits << PENDING_TIMER_LOW_SHIFT);
    }

    int pendingTimerDelay()
    {
        int highBits = (int) ((encodedState >>> PENDING_TIMER_HIGH_SHIFT) & PENDING_TIMER_HIGH_MASK);
        int lowBits = (int) ((encodedState >>> PENDING_TIMER_LOW_SHIFT) & PENDING_TIMER_LOW_MASK);
        int highestSetBit = 0x40000000 >>> (highBits + 31);
        return highestSetBit + ((highestSetBit >>> PENDING_TIMER_LOW_BITS) * lowBits);
    }

    long pendingTimerDeadline(long scheduledDeadline)
    {
        long pendingTimerDelay = pendingTimerDelay();
        return pendingTimerDelay == 0 ? 0 : scheduledDeadline + pendingTimerDelay;
    }

    long pendingTimerDeadline(DefaultProgressLog owner)
    {
        long pendingTimerDelay = pendingTimerDelay();
        return pendingTimerDelay == 0 ? 0 : owner.deadline(this) + pendingTimerDelay;
    }

    boolean isScheduled()
    {
        return isInHeap();
    }

    abstract void updateScheduling(DefaultProgressLog instance, TxnStateKind updated, @Nullable BlockedUntil awaiting, Progress newProgress);

    abstract void maybeRemove(DefaultProgressLog instance);
}
