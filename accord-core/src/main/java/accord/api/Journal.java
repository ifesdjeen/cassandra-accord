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

package accord.api;

import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.PersistentField.Persister;

/**
 * Persisted journal for transactional recovery.
 */
public interface Journal
{
    Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, Load load, RedundantBefore redundantBefore, DurableBefore durableBefore);

    // TODO (required): use OnDone instead of Runnable
    void saveCommand(int store, CommandUpdate value, Runnable onFlush);

    void purge(CommandStores commandStores);
    void replay(CommandStores commandStores);

    RedundantBefore loadRedundantBefore(int commandStoreId);
    NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId);
    NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId);
    CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId);

    Persister<DurableBefore, DurableBefore> durableBeforePersister();

    void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush);

    class CommandUpdate
    {
        public final TxnId txnId;
        public final Command before;
        public final Command after;

        public CommandUpdate(@Nullable Command before, @Nonnull Command after)
        {
            this.txnId = after.txnId();
            this.before = before;
            this.after = after;
        }
    }

    class FieldUpdates
    {
        // TODO (required): use persisted field logic
        public RedundantBefore newRedundantBefore;
        public NavigableMap<TxnId, Ranges> newBootstrapBeganAt;
        public NavigableMap<Timestamp, Ranges> newSafeToRead;
        public CommandStores.RangesForEpoch newRangesForEpoch;

        public String toString()
        {
            return "FieldUpdates{" +
                   "newRedundantBefore=" + newRedundantBefore +
                   ", newBootstrapBeganAt=" + newBootstrapBeganAt +
                   ", newSafeToRead=" + newSafeToRead +
                   ", newRangesForEpoch=" + newRangesForEpoch +
                   '}';
        }
    }

    enum Load
    {
        ALL,
        PURGEABLE,
        MINIMAL
    }

    /**
     * Helper for CommandStore to restore Command states.
     */
    interface Loader
    {
        void load(Command next, OnDone onDone);
        void apply(Command next, OnDone onDone);
    }


    interface OnDone
    {
        void success();
        void failure(Throwable t);
    }
}
