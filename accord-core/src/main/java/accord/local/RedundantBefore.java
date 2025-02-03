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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.RedundantStatus.Coverage;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.KeyDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.RedundantStatus.GC_BEFORE_ONLY;
import static accord.local.RedundantStatus.LOCALLY_REDUNDANT_ONLY;
import static accord.local.RedundantStatus.LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.NOT_OWNED_ONLY;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_OR_STALE_ONLY;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.SHARD_AND_LOCALLY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SHARD_APPLIED_AND_LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.SHARD_ONLY_APPLIED_AND_PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.TRUNCATE_BEFORE_ONLY;
import static accord.local.RedundantStatus.WAS_OWNED_LOCALLY_RETIRED;
import static accord.local.RedundantStatus.WAS_OWNED_ONLY;
import static accord.local.RedundantStatus.WAS_OWNED_SHARD_RETIRED;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.requirePartiallyOrdered;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Entry>
{
    public interface RedundantBeforeSupplier
    {
        RedundantBefore redundantBefore();
    }

    public static class SerializerSupport
    {
        public static RedundantBefore create(boolean inclusiveEnds, RoutingKey[] ends, Entry[] values)
        {
            return new RedundantBefore(inclusiveEnds, ends, values);
        }
    }

    // TODO (required): rationalise the various bounds we maintain; make merge idempotent and apply any filtering by superseding bounds on access
    public static class Entry
    {
        // TODO (desired): we don't need to maintain this now, can migrate to ReducingRangeMap.foldWithBounds
        public final Range range;
        // start inclusive, end exclusive
        public final long startOwnershipEpoch, endOwnershipEpoch;

        /**
         * Represents the maximum TxnId we know to have a record of transactions before, or else they will be invalidated.
         */
        public final @Nonnull TxnId locallyWitnessedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until locally for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         *
         * We maintain locallyAppliedBefore that were reached prior to a new bootstrap exceeding them,
         * as these were reached correctly.
         */
        public final @Nonnull TxnId locallyAppliedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until locally for the range in question,
         * and for which we guarantee that any distributed decision that might need to be consulted is also recorded
         * locally (i.e. it is known to be Stable locally, or else it did not execute)
         *
         * We maintain locallyDecidedAndAppliedBefore that were reached prior to a new bootstrap exceeding them,
         * as these were reached correctly and can be used for pruning CommandsForKey.
         *
         * However, once a bootstrap has begun we cannot safely advance until shardAppliedBefore goes
         * ahead of the bootstrappedAt, because we cannot guarantee to have any intervening decision recorded locally.
         */
        public final @Nonnull TxnId locallyDecidedAndAppliedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy non-bootstrapping replicas
         * for the range in question.
         */
        public final @Nonnull TxnId shardOnlyAppliedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy non-bootstrapping replicas
         * for the range in question, including ourselves.
         *
         * Note that in some cases we can safely use this property in place of gcBefore for cleaning up or inferring
         * invalidations, but remember that if we are erasing data we may report to peers then we must provide an RX
         * in place of that data to prevent a stale peer thinking they have enough information.
         */
        public final @Nonnull TxnId shardAppliedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy replicas for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         *
         * TODO (desired): track separate gcHlcBefore
         */
        public final @Nonnull TxnId gcBefore;

        /**
         * bootstrappedAt defines the txnId bounds we expect to maintain data for locally.
         *
         * We can bootstrap ranges at different times, and have a transaction that participates in both ranges -
         * in this case one of the portions of the transaction may be totally unordered with respect to other transactions
         * in that range because both occur prior to the bootstrappedAt point, so their dependencies are entirely erased.
         * We can also re-bootstrap the same range because bootstrap failed, and leave dangling transactions to execute
         * which then execute in an unordered fashion.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         */
        public final @Nonnull TxnId bootstrappedAt;

        /**
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;

        public Entry(Range range, long startOwnershipEpoch, long endOwnershipEpoch, @Nonnull TxnId locallyWitnessedBefore, @Nonnull TxnId locallyAppliedBefore, @Nonnull TxnId locallyDecidedAndAppliedBefore, @Nonnull TxnId shardOnlyAppliedBefore, @Nonnull TxnId shardAppliedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this.range = range;
            this.startOwnershipEpoch = startOwnershipEpoch;
            this.endOwnershipEpoch = endOwnershipEpoch;
            this.locallyWitnessedBefore = locallyWitnessedBefore;
            this.locallyAppliedBefore = locallyAppliedBefore;
            this.locallyDecidedAndAppliedBefore = locallyDecidedAndAppliedBefore;
            this.shardOnlyAppliedBefore = shardOnlyAppliedBefore;
            this.shardAppliedBefore = shardAppliedBefore;
            this.gcBefore = gcBefore;
            this.bootstrappedAt = bootstrappedAt;
            this.staleUntilAtLeast = staleUntilAtLeast;
            checkNoneOrRX(locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore,
                          shardAppliedBefore, gcBefore);
            requirePartiallyOrdered(locallyDecidedAndAppliedBefore, locallyAppliedBefore);
            requirePartiallyOrdered(shardAppliedBefore, locallyAppliedBefore);
            requirePartiallyOrdered(gcBefore, shardAppliedBefore, shardOnlyAppliedBefore);
        }

        private static void checkNoneOrRX(TxnId ... txnIds)
        {
            for (TxnId txnId : txnIds)
                checkNoneOrRX(txnId);
        }
        private static void checkNoneOrRX(TxnId txnId)
        {
            Invariants.requireArgument(txnId.equals(TxnId.NONE) || (txnId.domain().isRange() && txnId.is(ExclusiveSyncPoint)));
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry cur, Entry add)
        {
            // TODO (required): we shouldn't be trying to merge non-intersecting epochs
            if (cur.startOwnershipEpoch > add.endOwnershipEpoch)
                return cur;

            if (add.startOwnershipEpoch > cur.endOwnershipEpoch)
                return add;

            long startEpoch = Long.max(cur.startOwnershipEpoch, add.startOwnershipEpoch);
            long endEpoch = Long.min(cur.endOwnershipEpoch, add.endOwnershipEpoch);
            int cw = cur.locallyWitnessedBefore.compareTo(add.locallyWitnessedBefore);
            int cl = cur.locallyAppliedBefore.compareTo(add.locallyAppliedBefore);
            int cd = cur.locallyDecidedAndAppliedBefore.compareTo(add.locallyDecidedAndAppliedBefore);
            int cs = cur.shardOnlyAppliedBefore.compareTo(add.shardOnlyAppliedBefore);
            int cg = cur.gcBefore.compareTo(add.gcBefore);
            int cb = cur.bootstrappedAt.compareTo(add.bootstrappedAt);
            int csu = compareStaleUntilAtLeast(cur.staleUntilAtLeast, add.staleUntilAtLeast);

            if (range.equals(cur.range) && startEpoch == cur.startOwnershipEpoch && endEpoch == cur.endOwnershipEpoch && cw >= 0 && cl >= 0 && cd >= 0 && cs >= 0 && cg >= 0 && cb >= 0 && csu >= 0)
                return cur;
            if (range.equals(add.range) && startEpoch == add.startOwnershipEpoch && endEpoch == add.endOwnershipEpoch && cw <= 0 && cl <= 0 && cd >= 0 && cs <= 0 && cg <= 0 && cb <= 0 && csu <= 0)
                return add;

            TxnId locallyWitnessedBefore = cw >= 0 ? cur.locallyWitnessedBefore : add.locallyWitnessedBefore;
            TxnId locallyAppliedBefore = cl >= 0 ? cur.locallyAppliedBefore : add.locallyAppliedBefore;
            TxnId locallyDecidedAndAppliedBefore = cd >= 0 ? cur.locallyDecidedAndAppliedBefore : add.locallyDecidedAndAppliedBefore;
            TxnId shardOnlyAppliedBefore = cs >= 0 ? cur.shardOnlyAppliedBefore : add.shardOnlyAppliedBefore;
            TxnId gcBefore = cg >= 0 ? cur.gcBefore : add.gcBefore;
            TxnId bootstrappedAt = cb >= 0 ? cur.bootstrappedAt : add.bootstrappedAt;
            Timestamp staleUntilAtLeast = csu >= 0 ? cur.staleUntilAtLeast : add.staleUntilAtLeast;

            // if a NEW redundantBefore predates our current bootstrappedAt, we should not update it to avoid erroneously
            // treating transactions prior as locally redundant when they may simply have not applied yet, since we may
            // permit the sync point that defines redundancy to apply locally without waiting for these earlier
            // transactions, since we now consider them to be bootstrapping.
            // however, any locallyAppliedBefore that was set before bootstrap can be safely maintained,
            // and should not ideally go backwards (as CommandsForKey utilises it for GC)
            // TODO (desired): revisit later as semantics here evolve
            if (bootstrappedAt.compareTo(locallyAppliedBefore) >= 0)
                locallyAppliedBefore = cur.locallyAppliedBefore;

            TxnId shardAppliedBefore = TxnId.min(shardOnlyAppliedBefore, locallyAppliedBefore);
            if (bootstrappedAt.compareTo(shardAppliedBefore) >= 0)
                locallyDecidedAndAppliedBefore = cur.locallyDecidedAndAppliedBefore;
            if (staleUntilAtLeast != null && bootstrappedAt.compareTo(staleUntilAtLeast) >= 0)
                staleUntilAtLeast = null;

            return new Entry(range, startEpoch, endEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry withGcBeforeBeforeAtLeast(TxnId newGcBefore)
        {
            if (newGcBefore.compareTo(gcBefore) <= 0)
                return this;

            TxnId locallyAppliedBefore = TxnId.nonNullOrMax(this.locallyAppliedBefore, newGcBefore);
            TxnId shardAppliedBefore = TxnId.nonNullOrMax(this.shardAppliedBefore, newGcBefore);
            TxnId shardOnlyAppliedBefore = TxnId.nonNullOrMax(this.shardOnlyAppliedBefore, newGcBefore);
            return new Entry(range, startOwnershipEpoch, endOwnershipEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, newGcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry withBootstrappedAtLeast(TxnId newBootstrappedAt)
        {
            if (newBootstrappedAt.compareTo(gcBefore) <= 0)
                return this;

            return new Entry(range, startOwnershipEpoch, endOwnershipEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, newBootstrappedAt, staleUntilAtLeast);
        }

        @VisibleForImplementation
        public Entry withEpochs(long start, long end)
        {
            return new Entry(range, start, end, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        static @Nonnull Boolean isShardOnlyRedundant(Entry entry, @Nonnull Boolean prev, TxnId txnId)
        {
            return entry == null ? prev : entry.shardOnlyAppliedBefore.compareTo(txnId) >= 0;
        }

        static @Nullable RedundantStatus getAndMerge(Entry entry, @Nullable RedundantStatus prev, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (entry == null)
                return prev;
            RedundantStatus next = entry.get(txnId, executeAtIfKnown);
            return prev == null ? next : prev.merge(next);
        }

        static RangeDeps.BuilderByRange collectDep(Entry entry, @Nonnull RangeDeps.BuilderByRange prev, @Nonnull EpochSupplier minEpoch, @Nonnull EpochSupplier executeAt)
        {
            if (entry == null)
                return prev;

            // we report an RX that represents a point on or after our GC bound, so that we never report an incomplete
            // transitive dependency history. If we consistently only GC'd at gcBefore we could report this bound,
            // but since it is likely safe to use this bound in cases that don't have lagged durability,
            // we conservatively report this bound since it is expected to be applied already at all non-stale shards
            if (entry.shardAppliedBefore.compareTo(Timestamp.NONE) > 0)
                prev.add(entry.range, entry.shardAppliedBefore);

            return prev;
        }

        static Ranges validateSafeToRead(Entry entry, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (entry == null)
                return safeToRead;

            if (bootstrapAt.compareTo(entry.bootstrappedAt) < 0 || (entry.staleUntilAtLeast != null && bootstrapAt.compareTo(entry.staleUntilAtLeast) < 0))
                return safeToRead.without(Ranges.of(entry.range));

            return safeToRead;
        }

        static TxnId min(Entry entry, @Nullable TxnId min, Function<Entry, TxnId> get)
        {
            if (entry == null)
                return min;

            return TxnId.nonNullOrMin(min, get.apply(entry));
        }

        static TxnId max(Entry entry, @Nullable TxnId max, Function<Entry, TxnId> get)
        {
            if (entry == null)
                return max;

            return TxnId.nonNullOrMax(max, get.apply(entry));
        }

        static Participants<?> participantsWithoutStaleOrPreBootstrap(Entry entry, @Nonnull Participants<?> execute, TxnId txnId, @Nullable EpochSupplier executeAt)
        {
            return withoutStaleOrPreBootstrap(entry, execute, txnId, executeAt, Participants::without);
        }

        static Ranges rangesWithoutStaleOrPreBootstrap(Entry entry, @Nonnull Ranges execute, TxnId txnId, @Nullable EpochSupplier executeAt)
        {
            return withoutStaleOrPreBootstrap(entry, execute, txnId, executeAt, Ranges::without);
        }

        static <P extends Participants<?>> P withoutStaleOrPreBootstrap(Entry entry, @Nonnull P execute, TxnId txnId, @Nullable EpochSupplier executeAt, BiFunction<P, Ranges, P> without)
        {
            if (entry == null)
                return execute;

            Invariants.require(executeAt == null ? !entry.outOfBounds(txnId) : !entry.outOfBounds(txnId, executeAt));
            if (txnId.compareTo(entry.bootstrappedAt) < 0 || entry.staleUntilAtLeast != null)
                return without.apply(execute, Ranges.of(entry.range));

            return execute;
        }

        static Participants<?> withoutStaleOrPreBootstrapOrLocallyRetired(Entry entry, @Nonnull Participants<?> execute, TxnId txnId)
        {
            if (entry == null)
                return execute;

            if (txnId.compareTo(entry.bootstrappedAt) < 0 || entry.staleUntilAtLeast != null || entry.isLocallyRetired())
                return execute.without(Ranges.of(entry.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_StaleOrPreBootstrap(Entry entry, @Nonnull Participants<?> execute, TxnId txnId, @Nullable EpochSupplier executeAt)
        {
            if (entry == null)
                return execute;

            boolean outOfBounds = executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt);
            Invariants.expect(!outOfBounds, "Trying to apply withoutRedundantAnd_StaleOrPreBootstrap to %s for a range we don't own (%s), suggesting we computed ownership without an up-to-date epoch", txnId, entry);
            if (outOfBounds || (txnId.compareTo(entry.shardOnlyAppliedBefore) < 0
                                && (txnId.compareTo(entry.bootstrappedAt) < 0
                                    || entry.staleUntilAtLeast != null)))
                return execute.without(Ranges.of(entry.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_StaleOrPreBootstrapOrRetired(Entry entry, @Nonnull Participants<?> execute, TxnId txnId)
        {
            if (entry == null)
                return execute;

            if (txnId.compareTo(entry.shardOnlyAppliedBefore) < 0
                && (entry.endOwnershipEpoch <= txnId.epoch()
                    || txnId.compareTo(entry.bootstrappedAt) < 0
                    || entry.staleUntilAtLeast != null))
                return execute.without(Ranges.of(entry.range));

            return execute;
        }

        static Ranges withoutGarbage(Entry entry, @Nonnull Ranges notGarbage, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return notGarbage;

            if (txnId.compareTo(entry.gcBefore) < 0)
                return notGarbage.without(Ranges.of(entry.range));

            return notGarbage;
        }

        static Participants<?> withoutRetired(Entry entry, @Nonnull Participants<?> notRetired, TxnId txnId)
        {
            if (entry == null)
                return notRetired;

            if (txnId.compareTo(entry.shardOnlyAppliedBefore) < 0 && entry.endOwnershipEpoch <= txnId.epoch())
                return notRetired.without(Ranges.of(entry.range));

            return notRetired;
        }

        static Ranges withoutAnyRetired(Entry entry, @Nonnull Ranges notRetired)
        {
            if (entry == null || entry.endOwnershipEpoch > entry.shardAppliedBefore.epoch())
                return notRetired;

            return notRetired.without(Ranges.of(entry.range));
        }

        static Ranges withoutPreBootstrap(Entry entry, @Nonnull Ranges notPreBootstrap, TxnId txnId, Object ignore)
        {
            if (entry == null)
                return notPreBootstrap;

            if (txnId.compareTo(entry.bootstrappedAt) < 0)
                return notPreBootstrap.without(Ranges.of(entry.range));

            return notPreBootstrap;
        }

        RedundantStatus get(TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (wasOwned(txnId))
            {
                if (isShardRetired()) return WAS_OWNED_SHARD_RETIRED;
                if (isLocallyRetired()) return WAS_OWNED_LOCALLY_RETIRED;
                return WAS_OWNED_ONLY;
            }
            return getIgnoringOwnership(txnId, executeAtIfKnown);
        }

        RedundantStatus getIgnoringOwnership(TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            // we have to first check bootstrappedAt, since we are not locally redundant for the covered range
            // if the txnId is partially pre-bootstrap (since we may not have applied it for this range)
            if (staleUntilAtLeast != null || bootstrappedAt.compareTo(txnId) > 0 || txnId.epoch() < startOwnershipEpoch)
            {
                if (locallyAppliedBefore.compareTo(txnId) > 0)
                {
                    return shardOnlyAppliedBefore.compareTo(txnId) > 0
                           ? SHARD_APPLIED_AND_LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE
                           : LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE;
                }
                else
                {
                    return shardOnlyAppliedBefore.compareTo(txnId) > 0
                           ? SHARD_ONLY_APPLIED_AND_PRE_BOOTSTRAP_OR_STALE
                           : PRE_BOOTSTRAP_OR_STALE_ONLY;
                }
            }

            if (locallyAppliedBefore.compareTo(txnId) > 0)
            {
                if (gcBefore.compareTo(txnId) > 0)
                {
                    if (!requiresUniqueHlcs() || executeAtIfKnown == null || gcBefore.hlc() > executeAtIfKnown.uniqueHlc())
                        return GC_BEFORE_ONLY;
                    return TRUNCATE_BEFORE_ONLY;
                }
                if (shardOnlyAppliedBefore.compareTo(txnId) > 0)
                    return SHARD_AND_LOCALLY_APPLIED_ONLY;
                return LOCALLY_REDUNDANT_ONLY;
            }

            return RedundantStatus.NONE;
        }

        private static int compareStaleUntilAtLeast(@Nullable Timestamp a, @Nullable Timestamp b)
        {
            boolean aIsNull = a == null, bIsNull = b == null;
            if (aIsNull != bIsNull) return aIsNull ? -1 : 1;
            return aIsNull ? 0 : a.compareTo(b);
        }

        public final TxnId gcBefore()
        {
            return gcBefore;
        }

        public final TxnId shardRedundantBefore()
        {
            return shardAppliedBefore;
        }

        public final TxnId locallyWitnessedBefore()
        {
            return locallyWitnessedBefore;
        }

        public final TxnId locallyRedundantBefore()
        {
            return locallyAppliedBefore;
        }

        public final TxnId locallyRedundantOrBootstrappedBefore()
        {
            return TxnId.max(locallyAppliedBefore, bootstrappedAt);
        }

        private boolean outOfBounds(EpochSupplier lb, EpochSupplier ub)
        {
            return ub.epoch() < startOwnershipEpoch || lb.epoch() >= endOwnershipEpoch;
        }

        private boolean wasOwned(EpochSupplier lb)
        {
            return lb.epoch() >= endOwnershipEpoch;
        }

        private boolean isShardRetired()
        {
            return endOwnershipEpoch <= shardAppliedBefore.epoch();
        }

        private boolean isLocallyRetired()
        {
            return endOwnershipEpoch <= locallyAppliedBefore.epoch();
        }

        private boolean isClosed()
        {
            return endOwnershipEpoch <= locallyWitnessedBefore.epoch();
        }

        private boolean outOfBounds(Timestamp lb)
        {
            return lb.epoch() >= endOwnershipEpoch;
        }

        Entry withEpochs(int startEpoch, int endEpoch)
        {
            return new Entry(range, startEpoch, endEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry withRange(Range range)
        {
            return new Entry(range, startOwnershipEpoch, endOwnershipEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Entry that)
        {
            return this.startOwnershipEpoch == that.startOwnershipEpoch
                   && this.endOwnershipEpoch == that.endOwnershipEpoch
                   && this.locallyAppliedBefore.equals(that.locallyAppliedBefore)
                   && this.locallyDecidedAndAppliedBefore.equals(that.locallyDecidedAndAppliedBefore)
                   && this.shardAppliedBefore.equals(that.shardAppliedBefore)
                   && this.shardOnlyAppliedBefore.equals(that.shardOnlyAppliedBefore)
                   && this.gcBefore.equals(that.gcBefore)
                   && this.bootstrappedAt.equals(that.bootstrappedAt)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startOwnershipEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startOwnershipEpoch)) + ","
                   + (endOwnershipEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endOwnershipEpoch)) + ","
                   + (locallyAppliedBefore.compareTo(bootstrappedAt) >= 0 ? locallyAppliedBefore + ")" : bootstrappedAt + "*)");
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private final Ranges staleRanges;
    private final TxnId maxBootstrap, maxGcBefore;
    private final TxnId minShardRedundantBefore, minGcBefore;
    private final long maxRetiredEpoch;

    private RedundantBefore()
    {
        staleRanges = Ranges.EMPTY;
        maxBootstrap = maxGcBefore = TxnId.NONE;
        minShardRedundantBefore = minGcBefore = TxnId.MAX;
        maxRetiredEpoch = 0;
    }

    RedundantBefore(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
    {
        super(inclusiveEnds, starts, values);
        staleRanges = extractStaleRanges(values);
        TxnId maxBootstrap = TxnId.NONE, maxGcBefore = TxnId.NONE, minShardRedundantBefore = TxnId.MAX, minGcBefore = TxnId.MAX;
        long maxRetiredEpoch = 0;
        for (Entry entry : values)
        {
            if (entry == null) continue;
            if (entry.bootstrappedAt.compareTo(maxBootstrap) > 0)
                maxBootstrap = entry.bootstrappedAt;
            if (entry.gcBefore.compareTo(maxGcBefore) > 0)
                maxGcBefore = entry.gcBefore;
            if (entry.shardRedundantBefore().compareTo(minShardRedundantBefore) < 0)
                minShardRedundantBefore = entry.shardRedundantBefore();
            if (entry.gcBefore.compareTo(minGcBefore) < 0)
                minGcBefore = entry.gcBefore;
            if (entry.isLocallyRetired() && entry.endOwnershipEpoch >= maxRetiredEpoch)
                maxRetiredEpoch = entry.endOwnershipEpoch;
        }
        this.maxBootstrap = maxBootstrap;
        this.maxGcBefore = maxGcBefore;
        this.minShardRedundantBefore = minShardRedundantBefore;
        this.minGcBefore = minGcBefore;
        this.maxRetiredEpoch = maxRetiredEpoch;
        checkParanoid(starts, values);
    }

    private static Ranges extractStaleRanges(Entry[] values)
    {
        int countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                ++countStaleRanges;
        }

        if (countStaleRanges == 0)
            return Ranges.EMPTY;

        Range[] staleRanges = new Range[countStaleRanges];
        countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                staleRanges[countStaleRanges++] = entry.range;
        }
        return Ranges.ofSortedAndDeoverlapped(staleRanges).mergeTouching();
    }

    public static RedundantBefore create(AbstractRanges ranges, @Nonnull TxnId locallyWitnessedBefore, @Nonnull TxnId locallyAppliedBefore, @Nonnull TxnId shardAppliedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, locallyWitnessedBefore, locallyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, @Nonnull TxnId locallyWitnessedBefore, @Nonnull TxnId locallyAppliedBefore, @Nonnull TxnId shardAppliedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, locallyWitnessedBefore, locallyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyWitnessedBefore, @Nonnull TxnId locallyAppliedBefore, @Nonnull TxnId shardOnlyAppliedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, startEpoch, endEpoch, locallyWitnessedBefore, locallyAppliedBefore, shardOnlyAppliedBefore, gcBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyWitnessedBefore, @Nonnull TxnId locallyAppliedBefore, @Nonnull TxnId shardOnlyAppliedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        TxnId locallyDecidedAndAppliedBefore = locallyAppliedBefore;
        TxnId shardAppliedBefore = TxnId.min(locallyAppliedBefore, shardOnlyAppliedBefore);
        Entry entry = new Entry(null, startEpoch, endEpoch, locallyWitnessedBefore, locallyAppliedBefore, locallyDecidedAndAppliedBefore, shardOnlyAppliedBefore, shardAppliedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), cur.end(), entry.withRange(cur));
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.mergeIntervals(a, b, Builder::new);
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, Participants<?> participants)
    {
        RedundantStatus result = foldl(participants, Entry::getAndMerge, null, txnId, applyAtIfKnown);
        return result == null ? NOT_OWNED_ONLY : result;
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, RoutingKey key)
    {
        Entry entry = get(key);
        return entry == null ? NOT_OWNED_ONLY : entry.get(txnId, applyAtIfKnown);
    }

    public boolean isShardOnlyRedundant(TxnId txnId, Unseekables<?> participants)
    {
        return foldl(participants, Entry::isShardOnlyRedundant, false, txnId);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public Coverage preBootstrapOrStale(TxnId txnId, Participants<?> participants)
    {
        return status(txnId, null, participants).get(PRE_BOOTSTRAP_OR_STALE);
    }

    public <T extends Deps> RangeDeps.BuilderByRange collectDeps(Routables<?> participants, RangeDeps.BuilderByRange builder, EpochSupplier minEpoch, EpochSupplier executeAt)
    {
        return foldl(participants, Entry::collectDep, builder, minEpoch, executeAt);
    }

    public Ranges validateSafeToRead(Timestamp forBootstrapAt, Ranges ranges)
    {
        return foldl(ranges, Entry::validateSafeToRead, ranges, forBootstrapAt, null);
    }

    public TxnId min(Routables<?> participants, Function<Entry, TxnId> get)
    {
        return TxnId.nonNullOrMax(TxnId.NONE, foldl(participants, Entry::min, null, get));
    }

    public TxnId max(Routables<?> participants, Function<Entry, TxnId> get)
    {
        return foldl(participants, Entry::max, TxnId.NONE, get);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removeGcBefore(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.requireArgument(executeAt != null, "executeAt must not be null");
        if (txnId.compareTo(maxGcBefore) >= 0)
            return ranges;
        return foldl(ranges, Entry::withoutGarbage, ranges, txnId, executeAt);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removeRetired(Ranges ranges)
    {
        return foldl(ranges, Entry::withoutAnyRetired, ranges, r -> false);
    }

    public TxnId minShardRedundantBefore()
    {
        return minShardRedundantBefore;
    }

    public TxnId minGcBefore()
    {
        return minGcBefore;
    }

    public TxnId maxGcBefore()
    {
        return maxGcBefore;
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removePreBootstrap(TxnId txnId, Ranges ranges)
    {
        if (maxBootstrap.compareTo(txnId) <= 0)
            return ranges;
        return foldl(ranges, Entry::withoutPreBootstrap, ranges, txnId, null);
    }

    /**
     * Subtract anything we don't need to coordinate (because they are known to be shard durable),
     * and we don't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     */
    public Participants<?> expectToOwn(TxnId txnId, @Nullable EpochSupplier executeAt, Participants<?> participants)
    {
        if (txnId.is(ExclusiveSyncPoint))
        {
            if (!mayFilterStaleOrPreBootstrapOrRetired(txnId, participants))
                return participants;

            return foldl(participants, Entry::withoutRedundantAnd_StaleOrPreBootstrapOrRetired, participants, txnId);
        }
        else
        {
            if (!mayFilterStaleOrPreBootstrap(txnId, participants))
                return participants;

            return foldl(participants, Entry::withoutRedundantAnd_StaleOrPreBootstrap, participants, txnId, executeAt);
        }
    }

    /**
     * Subtract anything we won't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     */
    public Participants<?> expectToExecute(TxnId txnId, @Nullable EpochSupplier executeAt, Participants<?> participants)
    {
        if (txnId.is(ExclusiveSyncPoint))
        {
            if (!mayFilterStaleOrPreBootstrapOrRetired(txnId, participants))
                return participants;

            return foldl(participants, Entry::withoutStaleOrPreBootstrapOrLocallyRetired, participants, txnId);
        }
        else
        {
            if (!mayFilterStaleOrPreBootstrap(txnId, participants))
                return participants;

            return foldl(participants, Entry::participantsWithoutStaleOrPreBootstrap, participants, txnId, executeAt);
        }
    }

    public boolean mayFilter(TxnId txnId, Participants<?> participants)
    {
        return mayFilterStaleOrPreBootstrapOrRetired(txnId, participants);
    }

    private boolean mayFilterStaleOrPreBootstrapOrRetired(TxnId txnId, Participants<?> participants)
    {
        return maxRetiredEpoch > txnId.epoch() || mayFilterStaleOrPreBootstrap(txnId, participants);
    }

    private boolean mayFilterStaleOrPreBootstrap(TxnId txnId, Participants<?> participants)
    {
        return maxBootstrap.compareTo(txnId) > 0 || (staleRanges != null && staleRanges.intersects(participants));
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> expectToCalculateDependenciesOrConsultOnRecovery(TxnId txnId, Participants<?> participants)
    {
        if (!mayFilterStaleOrPreBootstrapOrRetired(txnId, participants))
            return participants;
        return foldl(participants, Entry::withoutRetired, participants, txnId);
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> expectToOwnOrExecuteOrConsultOnRecovery(TxnId txnId, Participants<?> participants)
    {
        if (!mayFilterStaleOrPreBootstrapOrRetired(txnId, participants))
            return participants;
        return foldl(participants, Entry::withoutRedundantAnd_StaleOrPreBootstrapOrRetired, participants, txnId);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges expectToOwnOrExecute(TxnId txnId, Ranges ranges)
    {
        if (!mayFilterStaleOrPreBootstrap(txnId, ranges))
            return ranges;
        return foldl(ranges, Entry::rangesWithoutStaleOrPreBootstrap, ranges, txnId, null);
    }

    public static class Builder extends AbstractIntervalBuilder<RoutingKey, Entry, RedundantBefore>
    {
        public Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected Entry slice(RoutingKey start, RoutingKey end, Entry v)
        {
            if (v.range.start().equals(start) && v.range.end().equals(end))
                return v;

            return new Entry(v.range.newRange(start, end), v.startOwnershipEpoch, v.endOwnershipEpoch, v.locallyWitnessedBefore, v.locallyAppliedBefore, v.locallyDecidedAndAppliedBefore, v.shardOnlyAppliedBefore, v.shardAppliedBefore, v.gcBefore, v.bootstrappedAt, v.staleUntilAtLeast);
        }

        @Override
        protected Entry reduce(Entry a, Entry b)
        {
            return Entry.reduce(a, b);
        }

        @Override
        protected Entry tryMergeEqual(Entry a, Entry b)
        {
            if (!a.equalsIgnoreRange(b))
                return null;

            Invariants.require(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startOwnershipEpoch, a.endOwnershipEpoch, a.locallyWitnessedBefore, a.locallyAppliedBefore, a.locallyDecidedAndAppliedBefore, a.shardOnlyAppliedBefore, a.shardAppliedBefore, a.gcBefore, a.bootstrappedAt, a.staleUntilAtLeast);
        }

        @Override
        public void append(RoutingKey start, RoutingKey end, @Nonnull Entry value)
        {
            if (value.range.start().compareTo(start) != 0 || value.range.end().compareTo(end) != 0)
                throw illegalState();
            super.append(start, end, value);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }

    private static void checkParanoid(RoutingKey[] starts, Entry[] values)
    {
        if (!Invariants.isParanoid())
            return;

        for (int i = 0 ; i < values.length ; ++i)
        {
            if (values[i] != null)
            {
                Invariants.requireArgument(starts[i].equals(values[i].range.start()));
                Invariants.requireArgument(starts[i + 1].equals(values[i].range.end()));
            }
        }
    }

    public final void removeRedundantDependencies(Unseekables<?> participants, Command.WaitingOn.Update builder)
    {
        // Note: we do not need to track the bootstraps we implicitly depend upon, because we will not serve any read requests until this has completed
        //  and since we are a timestamp store, and we write only this will sort itself out naturally
        // TODO (required): make sure we have no races on HLC around SyncPoint else this resolution may not work (we need to know the micros equivalent timestamp of the snapshot)
        class KeyState
        {
            Int2ObjectHashMap<RoutingKeys> partiallyBootstrapping;

            /**
             * Are the participating ranges for the txn fully covered by bootstrapping ranges for this command store
             */
            boolean isFullyBootstrapping(Command.WaitingOn.Update builder, Range range, int txnIdx)
            {
                if (builder.directKeyDeps.foldEachKey(txnIdx, range, true, (r0, k, p) -> p && r0.contains(k)))
                    return true;

                if (partiallyBootstrapping == null)
                    partiallyBootstrapping = new Int2ObjectHashMap<>();
                RoutingKeys prev = partiallyBootstrapping.get(txnIdx);
                RoutingKeys remaining = prev;
                if (remaining == null) remaining = builder.directKeyDeps.participants(txnIdx);
                else Invariants.require(!remaining.isEmpty());
                remaining = remaining.without(range);
                if (prev == null) Invariants.require(!remaining.isEmpty());
                partiallyBootstrapping.put(txnIdx, remaining);
                return remaining.isEmpty();
            }
        }

        KeyDeps directKeyDeps = builder.directKeyDeps;
        if (!directKeyDeps.isEmpty())
        {
            foldl(directKeyDeps.keys(), (e, s, d, b) -> {
                // TODO (desired, efficiency): foldlInt so we can track the lower rangeidx bound and not revisit unnecessarily
                // find the txnIdx below which we are known to be fully redundant locally due to having been applied or invalidated
                int bootstrapIdx = d.txnIdsWithFlags().find(e.bootstrappedAt);
                if (bootstrapIdx < 0) bootstrapIdx = -1 - bootstrapIdx;
                int appliedIdx = d.txnIdsWithFlags().find(e.locallyAppliedBefore);
                if (appliedIdx < 0) appliedIdx = -1 - appliedIdx;

                // remove intersecting transactions with known redundant txnId
                // note that we must exclude all transactions that are pre-bootstrap, and perform the more complicated dance below,
                // as these transactions may be only partially applied, and we may need to wait for them on another key.
                if (appliedIdx > bootstrapIdx)
                {
                    d.forEach(e.range, bootstrapIdx, appliedIdx, b, s, (b0, s0, txnIdx) -> {
                        b0.removeWaitingOnDirectKeyTxnId(txnIdx);
                    });
                }

                if (bootstrapIdx > 0)
                {
                    d.forEach(e.range, 0, bootstrapIdx, b, s, e.range, (b0, s0, r, txnIdx) -> {
                        if (b0.isWaitingOnDirectKeyTxnIdx(txnIdx) && s0.isFullyBootstrapping(b0, r, txnIdx))
                            b0.removeWaitingOnDirectKeyTxnId(txnIdx);
                    });
                }
                return s;
            }, new KeyState(), directKeyDeps, builder);
        }

        /**
         * If we have to handle bootstrapping ranges for range transactions, these may only partially cover the
         * transaction, in which case we should not remove the transaction as a dependency. But if it is fully
         * covered by bootstrapping ranges then we *must* remove it as a dependency.
         */
        class RangeState
        {
            Range range;
            int bootstrapIdx, appliedIdx;
            Map<Integer, Ranges> partiallyBootstrapping;

            /**
             * Are the participating ranges for the txn fully covered by bootstrapping ranges for this command store
             */
            boolean isFullyBootstrapping(int rangeTxnIdx)
            {
                // if all deps for the txnIdx are contained in the range, don't inflate any shared object state
                if (builder.directRangeDeps.foldEachRange(rangeTxnIdx, range, true, (r1, r2, p) -> p && r1.contains(r2)))
                    return true;

                if (partiallyBootstrapping == null)
                    partiallyBootstrapping = new HashMap<>();
                Ranges prev = partiallyBootstrapping.get(rangeTxnIdx);
                Ranges remaining = prev;
                if (remaining == null) remaining = builder.directRangeDeps.ranges(rangeTxnIdx);
                else Invariants.require(!remaining.isEmpty());
                remaining = remaining.without(Ranges.of(range));
                if (prev == null) Invariants.require(!remaining.isEmpty());
                partiallyBootstrapping.put(rangeTxnIdx, remaining);
                return remaining.isEmpty();
            }
        }

        RangeDeps rangeDeps = builder.directRangeDeps;
        foldl(participants, (e, s, d, b) -> {
            int bootstrapIdx = d.txnIdsWithFlags().find(e.bootstrappedAt);
            if (bootstrapIdx < 0) bootstrapIdx = -1 - bootstrapIdx;
            s.bootstrapIdx = bootstrapIdx;

            int appliedIdx = d.txnIdsWithFlags().find(e.locallyAppliedBefore);
            if (appliedIdx < 0) appliedIdx = -1 - appliedIdx;
            if (e.locallyAppliedBefore.epoch() >= e.endOwnershipEpoch)
            {
                // for range transactions, we should not infer that a still-owned range is redundant because a not-owned range that overlaps is redundant
                int altAppliedIdx = d.txnIdsWithFlags().find(TxnId.minForEpoch(e.endOwnershipEpoch));
                if (altAppliedIdx < 0) altAppliedIdx = -1 - altAppliedIdx;
                if (altAppliedIdx < appliedIdx) appliedIdx = altAppliedIdx;
            }
            s.appliedIdx = appliedIdx;

            // remove intersecting transactions with known redundant txnId
            if (appliedIdx > bootstrapIdx)
            {
                // TODO (desired):
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx >= s0.bootstrapIdx && txnIdx < s0.appliedIdx)
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }

            if (bootstrapIdx > 0)
            {
                // if we have any ranges where bootstrap is involved, we have to do a more complicated dance since
                // this may imply only partial redundancy (we may still depend on the transaction for some other range)
                s.range = e.range;
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx < s0.bootstrapIdx && b0.isWaitingOnDirectRangeTxnIdx(txnIdx) && s0.isFullyBootstrapping(txnIdx))
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }
            return s;
        }, new RangeState(), rangeDeps, builder);
    }

    public final boolean hasLocallyRedundantDependencies(TxnId minimumDependencyId, Timestamp executeAt, Participants<?> participantsOfWaitingTxn)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        return status(minimumDependencyId, executeAt, participantsOfWaitingTxn).any(LOCALLY_REDUNDANT);
    }
}
