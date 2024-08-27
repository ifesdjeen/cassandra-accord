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

package accord.utils;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.api.Scheduler;

/**
 * Basic idea is we collect timers in buckets that are logarithmically/exponentially spaced,
 * with buckets nearer in time closer together (down to some minimum spacing).
 *
 * These buckets are disjoint, and are split on insert when they both exceed a certain size and are eligible
 * to cover a smaller span due to the passing of time.
 *
 * A bucket becomes the current epoch once "now" truncated to the minBucketSpan is equal to the bucket's epoch.
 * At this point, the bucket is heapified so that the entries may be visited in order. Prior to this point,
 * insertions and deletions within a bucket are constant time.
 *
 * This design expects to have a maximum of log2(maxDelay)-K buckets, so bucket lookups are log(log(maxDelay)).
 *
 * This design permits log(log(maxDelay)) time insertion and removal for all items not in the nearest bucket, and log(K)
 * for the nearest bucket, where K is the size of the current epoch's bucket.
 *
 * Given that we may split buckets logarithmically many times, amortised insertion time is logarithmic for entries
 * that survive multiple bucket splits. However, due to the nature of these timer events (essentially timeouts), and
 * that further out timers are grouped in exponentially larger buckets, we expect most entries to be inserted and deleted
 * in constant time.
 *
 * TODO (desired): consider the case of repeatedly splitting the nearest bucket, as can maybe lead to complexity between
 *  n.lg(n) and n^2. In the worst case every item is in the nearest bucket that has lg(D) buckets that are split lg(D)
 *  times and either
 *  (1) all stay in the same bucket. This yields lg(D).n.lg(n) complexity, but we could perhaps avoid this with some summary
 *      data about where we could split a bucket, or by shrinking the bucket to smaller than its ideal span on split when
 *      we detect it.
 *  (2) splits half into the next bucket each time. So each lg(D) round incurs (n/D^2).lg(n/D^2) costs.
 *  However, in both cases, if D is small we probably don't care - and if it is large then this will happen over a very long
 *  period of time and so we still probably don't care.
 * @param <T>
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LogGroupTimers<T extends LogGroupTimers.Timer>
{
    public static class Timer extends IntrusivePriorityHeap.Node
    {
        private int deadlineSinceEpoch;
        private int shiftedBucketEpoch;
    }

    public class Scheduling
    {
        final Scheduler scheduler;
        final Function<T, Runnable> taskFactory;
        final long schedulerImpreciseLateTolerance;
        final long schedulerPreciseLateTolerance;
        final long preciseDelayThreshold;

        Scheduler.Scheduled scheduled = Scheduler.CANCELLED;
        long lastNow; // now can go backwards in time since we calculate it before taking the lock
        long scheduledAt = Long.MAX_VALUE;;

        /**
         * Note that the parameter to taskFactory may be null
         */
        public Scheduling(Scheduler scheduler, Function<T, Runnable> taskFactory, long schedulerImpreciseLateTolerance, long schedulerPreciseLateTolerance, long preciseDelayThreshold)
        {
            this.scheduler = scheduler;
            this.taskFactory = taskFactory;
            this.schedulerImpreciseLateTolerance = schedulerImpreciseLateTolerance;
            this.schedulerPreciseLateTolerance = schedulerPreciseLateTolerance;
            this.preciseDelayThreshold = preciseDelayThreshold;
        }

        public void ensureScheduled(long now)
        {
            now = Math.max(lastNow, now);
            T next = peekIfSoonerThan(now + preciseDelayThreshold);
            long runAt;
            if (next == null)
            {
                runAt = nextDeadlineEpoch();
                if (runAt < 0)
                    return;

                // when scheduling an imprecise run time, we don't mind if we're late by some amount
                // (often we will be early)
                runAt += schedulerImpreciseLateTolerance;
            }
            else
            {
                runAt = Math.max(now, deadline(next));
            }

            if (!scheduled.isDone())
            {
                if (runAt > scheduledAt - schedulerPreciseLateTolerance)
                    return;

                scheduled.cancel();
            }

            scheduledAt = runAt;
            lastNow = now;
            long delay = runAt - now;
            scheduled = scheduler.once(taskFactory.apply(next), delay, TimeUnit.MICROSECONDS);
        }

        public void maybeReschedule(long now, long newDeadline)
        {
            if (scheduled.isDone() || newDeadline < scheduledAt - schedulerPreciseLateTolerance)
                ensureScheduled(now);
        }
    }

    static class Bucket<T extends Timer> extends IntrusivePriorityHeap<T> implements Comparable<Bucket<T>>
    {
        final long epoch;
        long bucketSpan; // we use a long to cleanly represent 0x800000000 (Integer.MAX_VALUE+1)
        final LogGroupTimers<T> owner;

        Bucket(long epoch, long bucketSpan, LogGroupTimers<T> owner)
        {
            this.epoch = epoch;
            this.bucketSpan = bucketSpan;
            this.owner = owner;
        }

        protected void shrink(long newSpan)
        {
            heapifiedSize = 0;
            bucketSpan = newSpan;
            filterUnheapified(this, Bucket::maybeRedistribute);
        }

        private boolean maybeRedistribute(T timer)
        {
            long deadline = owner.deadline(timer);
            if (contains(deadline))
                return false;

            owner.addInternal(deadline, timer);
            return true;
        }

        @Override
        protected void append(T timer)
        {
            Invariants.checkState(epoch + bucketSpan > owner.deadline(timer));
            super.append(timer);
        }

        @Override
        protected void update(T timer)
        {
            Invariants.checkState(epoch + bucketSpan > owner.deadline(timer));
            super.update(timer);
        }

        @Override
        public int compareTo(Bucket<T> that)
        {
            return Long.compare(epoch, that.epoch);
        }

        @Override
        public int compare(T a, T b)
        {
            return compareTimers(a, b);
        }

        boolean contains(long deadline)
        {
            deadline -= epoch;
            return deadline >= 0 && deadline < bucketSpan;
        }

        private static int compareTimers(Timer a, Timer b)
        {
            return Integer.compare(a.deadlineSinceEpoch, b.deadlineSinceEpoch);
        }
    }

    final TimeUnit units;
    final long bucketShift;
    final long minBucketSpan;
    final int bucketSplitSize;

    Bucket[] buckets = new Bucket[8];
    Bucket addFinger;

    int bucketsStart, bucketsEnd;
    int timerCount;
    long curEpoch;

    public LogGroupTimers(TimeUnit units)
    {
        this(units, defaultBucketShift(units));
    }

    public LogGroupTimers(TimeUnit units, int bucketShift)
    {
        this(units, bucketShift, 256);
    }

    public LogGroupTimers(TimeUnit units, int bucketShift, int bucketSplitSize)
    {
        this.units = units;
        this.bucketShift = Invariants.checkArgument(bucketShift, bucketShift < 31);
        this.minBucketSpan = 1L << bucketShift;
        this.bucketSplitSize = bucketSplitSize;
    }

    // by default group together ~16ms
    private static int defaultBucketShift(TimeUnit units)
    {
        switch (units)
        {
            default: return 0;
            case MILLISECONDS: return 4;
            case MICROSECONDS: return 14;
            case NANOSECONDS: return 24;
        }
    }

    // non-modifying to support concurrent use with advance
    public T peek()
    {
        int i = bucketsStart;
        while (i < bucketsEnd)
        {
            Bucket<T> head = buckets[i++];
            if (!head.isEmpty())
                return head.peekNode();
        }

        return null;
    }

    public T peekIfSoonerThan(long deadlineThreshold)
    {
        int i = bucketsStart;
        while (i < bucketsEnd)
        {
            Bucket<T> head = buckets[i++];
            if (head.epoch >= deadlineThreshold)
                return null;

            if (!head.isEmpty())
                return head.peekNode();
        }

        return null;
    }

    /**
     * Return the epoch of the next timer we have to schedule
     */
    public long nextDeadlineEpoch()
    {
        int i = bucketsStart;
        while (i < bucketsEnd)
        {
            Bucket<T> head = buckets[i++];
            if (!head.isEmpty())
                return head.epoch;
        }

        return -1L;
    }

    // unsafe for reentry during advance
    public T poll()
    {
        while (bucketsStart < bucketsEnd)
        {
            Bucket<T> head = buckets[bucketsStart];
            if (!head.isEmpty())
                return head.pollNode();

            buckets[bucketsStart++] = null;
            if (head == addFinger) addFinger = null;
        }

        return null;
    }

    /**
     * Visit IN ARBITRARY ORDER all timers expired at {@code now}
     *
     * Permits reentrancy on {@link #add} and {@link #peek}
     */
    public <P> void advance(long now, P param, BiConsumer<P, T> expiredTimers)
    {
        long nextEpoch = now & -minBucketSpan;
        if (nextEpoch < curEpoch)
            return;

        curEpoch = nextEpoch;
        while (bucketsStart < bucketsEnd)
        {
            // drain buckets that are wholly contained by our new time
            Bucket<T> head = buckets[bucketsStart];
            if (head.epoch + head.bucketSpan <= now)
            {
                timerCount -= head.size;
                head.drain(param, expiredTimers);
            }
            else
            {
                T timer;
                while (null != (timer = head.peekNode()))
                {
                    if (deadline(timer) > now)
                        return;

                    --timerCount;
                    Invariants.checkState(timer == head.pollNode());
                    Invariants.checkState(!timer.isInHeap());
                    expiredTimers.accept(param, timer);
                }
            }

            Invariants.checkState(head.isEmpty());
            Invariants.checkState(head == buckets[bucketsStart]);
            buckets[bucketsStart++] = null;
            if (head == addFinger)
                addFinger = null;
        }

        Invariants.checkState(addFinger == null || addFinger == findBucket(addFinger.epoch));
    }

    public void add(long deadline, T timer)
    {
        Invariants.checkState(deadline >= curEpoch);
        addInternal(deadline, timer);
        ++timerCount;
    }

    public void update(long deadline, T timer)
    {
        Invariants.checkState(deadline >= curEpoch);
        Timer t = timer; // cast to access private field
        Bucket<T> bucket = findBucket((long)t.shiftedBucketEpoch << bucketShift);
        Invariants.checkState(bucket != null);
        if (bucket.contains(deadline))
        {
            bucket.update(timer);
        }
        else
        {
            bucket.remove(timer);
            addInternal(deadline, timer);
        }
    }

    private void addInternal(long deadline, T timer)
    {
        Bucket<T> bucket = addFinger;
        if (bucket == null || !bucket.contains(deadline))
        {
            int index = findBucketIndex(buckets, bucketsStart, bucketsEnd, deadline);
            bucket = ensureBucket(index, deadline);
            if (bucket.size >= bucketSplitSize)
            {
                long idealSpan = bucketSpan(bucket.epoch);
                if (idealSpan <= bucket.bucketSpan/2)
                {
                    if (buckets[index] != bucket) ++index;
                    Bucket<T> newBucket = insertBucket(index + 1, bucket.epoch + bucket.bucketSpan/2, bucket.bucketSpan - bucket.bucketSpan/2);
                    bucket.shrink(idealSpan);
                    if (newBucket.contains(deadline))
                        bucket = newBucket;
                }
            }
        }

        set(timer, deadline, bucket.epoch);
        bucket.append(timer);
        addFinger = bucket;
    }

    public void remove(T timer)
    {
        Timer t = timer; // cast to access private field
        Bucket<T> bucket = findBucket((long)t.shiftedBucketEpoch << bucketShift);
        Invariants.checkState(bucket != null);
        bucket.remove(timer);
        --timerCount;
    }

    public int size()
    {
        return timerCount;
    }

    public boolean isEmpty()
    {
        return timerCount == 0;
    }

    private long bucketEpoch(long deadline, long bucketSpan)
    {
        // once we hit really large bucket sizes we may not be able to encode the deadlineSinceEpoch in an Integer
        // in this case just permit a linear number of buckets (should be exceptionally rare, or means minBucketMask is poorly configured)
        long epoch = deadline & -bucketSpan;
        while (deadline - epoch > Integer.MAX_VALUE)
            epoch += bucketSpan;
        return epoch;
    }

    private long bucketSpan(long deadline)
    {
        if (deadline <= curEpoch)
            return this.minBucketSpan;

        long bucketSpan = Long.highestOneBit(deadline - curEpoch);
        bucketSpan = Math.max(minBucketSpan, bucketSpan);
        bucketSpan = Math.min(0x80000000L, bucketSpan);
        return bucketSpan;
    }

    private Bucket<T> findBucket(long bucketEpoch)
    {
        int i = findBucketIndex(buckets, bucketsStart, bucketsEnd, bucketEpoch);
        if (i < bucketsStart) return null;
        return buckets[i];
    }

    private Bucket<T> ensureBucket(int index, long deadline)
    {
        Bucket<T> bucket = null;
        if (index >= bucketsStart)
        {
            bucket = buckets[index];
            if (bucket.contains(deadline))
                return bucket;
        }

        long insertBucketSpan = bucketSpan(deadline);
        long insertEpoch = bucketEpoch(deadline, insertBucketSpan);
        if (bucket != null)
            insertEpoch = Math.max(insertEpoch, bucket.epoch + bucket.bucketSpan);

        if (index + 1 < bucketsEnd)
        {
            Bucket<T> nextBucket = buckets[index + 1];
            long maxSpan = nextBucket.epoch - insertEpoch;
            if (maxSpan < insertBucketSpan)
            {
                if (maxSpan <= insertBucketSpan / 2)
                {
                    insertEpoch = nextBucket.epoch - insertBucketSpan;
                    if (bucket != null && insertEpoch < bucket.epoch + bucket.bucketSpan)
                    {
                        // extend the prior bucket if it doesn't grow by more than 50%
                        long newSpan = nextBucket.epoch - bucket.epoch;
                        if (newSpan - bucket.bucketSpan > bucket.bucketSpan / 2)
                        {
                            bucket.bucketSpan = nextBucket.epoch - bucket.epoch;
                            return bucket;
                        }
                        insertEpoch = bucket.epoch + bucket.bucketSpan;
                        insertBucketSpan = nextBucket.epoch - insertEpoch;
                    }
                }
                else insertBucketSpan = maxSpan;
            }
        }

        if (bucket != null || index < 0)
            ++index;

        return insertBucket(index, insertEpoch, insertBucketSpan);
    }

    private Bucket<T> insertBucket(int index, long bucketEpoch, long bucketSpan)
    {
        Bucket<T> bucket = new Bucket<>(bucketEpoch, bucketSpan, this);
        if (bucketsStart > 0)
        {
            if (index > bucketsStart)
            {
                int shiftCount = index-- - bucketsStart;
                System.arraycopy(buckets, bucketsStart, buckets, bucketsStart - 1, shiftCount);
            }
            --bucketsStart;
        }
        else if (bucketsEnd < buckets.length)
        {
            System.arraycopy(buckets, index, buckets, index + 1, bucketsEnd - index);
            ++bucketsEnd;
        }
        else
        {
            Bucket[] newBuckets = new Bucket[buckets.length * 2];
            System.arraycopy(buckets, 0, newBuckets, 0, index);
            System.arraycopy(buckets, index, newBuckets, index + 1, bucketsEnd - index);
            buckets = newBuckets;
            ++bucketsEnd;
        }
        buckets[index] = bucket;
        Invariants.checkState(SortedArrays.isSorted(buckets, bucketsStart, bucketsEnd, Comparator.comparingLong(b -> b.epoch)));
        return bucket;
    }

    private void set(Timer timer, long deadline, long bucketEpoch)
    {
        timer.deadlineSinceEpoch = Math.toIntExact(deadline - bucketEpoch);
        timer.shiftedBucketEpoch = Math.toIntExact(bucketEpoch >>> bucketShift);
    }

    public long deadline(Timer timer)
    {
        return deadline(timer.deadlineSinceEpoch, timer.shiftedBucketEpoch);
    }

    private long deadline(int deadlineSinceEpoch, int shiftedBucketEpoch)
    {
        return deadlineSinceEpoch + ((long)shiftedBucketEpoch << bucketShift);
    }

    // copied and simplified from SortedArrays
    private static int findBucketIndex(Bucket[] buckets, int from, int to, long find)
    {
        int lb = from;
        while (lb < to)
        {
            int i = (lb + to) >>> 1;
            int c = Long.compare(find, buckets[i].epoch);
            if (c < 0) to = i;
            else if (c > 0) lb = i + 1;
            else return i;
        }
        return lb - 1;
    }

}
