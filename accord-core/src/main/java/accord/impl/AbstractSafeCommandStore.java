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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import static accord.api.Journal.FieldUpdates;
import static accord.local.CommandStores.RangesForEpoch;
import static accord.local.KeyHistory.TIMESTAMPS;
import static accord.utils.Invariants.illegalArgument;

public abstract class AbstractSafeCommandStore<C extends SafeCommand,
                                              TFK extends SafeTimestampsForKey,
                                              CFK extends SafeCommandsForKey,
                                              Caches extends AbstractSafeCommandStore.CommandStoreCaches<C, TFK, CFK>>
extends SafeCommandStore
{
    protected final PreLoadContext context;

    private final CommandStore commandStore;
    private FieldUpdates fieldUpdates;

    protected AbstractSafeCommandStore(PreLoadContext context, CommandStore commandStore)
    {
        this.context = context;
        this.commandStore = commandStore;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    public interface CommandStoreCaches<C, TFK, CFK> extends AutoCloseable
    {
        void close();

        C acquireIfLoaded(TxnId txnId);
        CFK acquireIfLoaded(RoutingKey key);
        TFK acquireTfkIfLoaded(RoutingKey key);
    }

    protected abstract Caches tryGetCaches();
    protected abstract C add(C safeCommand, Caches caches);
    protected abstract CFK add(CFK safeCfk, Caches caches);
    protected abstract TFK add(TFK safeTfk, Caches caches);

    // get anything we've already loaded and referenced
    protected abstract TFK timestampsForKeyInternal(RoutingKey key);

    @Override
    public PreLoadContext canExecute(PreLoadContext with)
    {
        if (with.isEmpty()) return with;
        if (with.keys().domain() == Routable.Domain.Range)
            return with.isSubsetOf(this.context) ? with : null;

        if (!context().keyHistory().satisfies(with.keyHistory()))
            return null;

        try (Caches caches = tryGetCaches())
        {
            if (caches == null)
                return with.isSubsetOf(this.context) ? with : null;

            for (TxnId txnId : with.txnIds())
            {
                if (null != getInternal(txnId))
                    continue;

                C safeCommand = caches.acquireIfLoaded(txnId);
                if (safeCommand == null)
                    return null;

                add(safeCommand, caches);
            }

            KeyHistory keyHistory = with.keyHistory();
            if (keyHistory == KeyHistory.NONE)
                return with;

            List<RoutingKey> unavailable = null;
            Unseekables<?> keys = with.keys();
            if (keys.isEmpty())
                return with;

            for (int i = 0 ; i < keys.size() ; ++i)
            {
                RoutingKey key = (RoutingKey) keys.get(i);
                if (keyHistory == TIMESTAMPS)
                {
                    if (null != timestampsForKeyInternal(key))
                        continue; // already in working set

                    TFK safeTfk = caches.acquireTfkIfLoaded(key);
                    if (safeTfk != null)
                    {
                        add(safeTfk, caches);
                        continue;
                    }
                }
                else
                {
                    if (null != getInternal(key))
                        continue; // already in working set

                    CFK safeCfk = caches.acquireIfLoaded(key);
                    if (safeCfk != null)
                    {
                        add(safeCfk, caches);
                        continue;
                    }
                }
                if (unavailable == null)
                    unavailable = new ArrayList<>();
                unavailable.add(key);
            }

            if (unavailable == null)
                return with;

            if (unavailable.size() == keys.size())
                return null;

            return PreLoadContext.contextFor(with.primaryTxnId(), with.additionalTxnId(), keys.without(RoutingKeys.ofSortedUnique(unavailable)), keyHistory);
        }
    }

    @Override
    public PreLoadContext context()
    {
        return context;
    }

    @Override
    protected C ifLoadedInternal(TxnId txnId)
    {
        try (Caches caches = tryGetCaches())
        {
            if (caches == null)
                return null;

            C command = caches.acquireIfLoaded(txnId);
            if (command == null)
                return null;

            return add(command, caches);
        }
    }

    @Override
    protected CFK ifLoadedInternal(RoutingKey txnId)
    {
        try (Caches caches = tryGetCaches())
        {
            if (caches == null)
                return null;

            CFK cfk = caches.acquireIfLoaded(txnId);
            if (cfk == null)
                return null;

            return add(cfk, caches);
        }
    }

    @Override
    public TFK timestampsForKey(RoutingKey key)
    {
        TFK safeTfk = timestampsForKeyInternal(key);
        if (safeTfk == null)
            throw illegalArgument("%s not referenced in %s", key, context);
        return safeTfk;
    }

    public void postExecute()
    {
        if (fieldUpdates == null)
            return;

        if (fieldUpdates.newRedundantBefore != null)
            super.unsafeSetRedundantBefore(fieldUpdates.newRedundantBefore);

        if (fieldUpdates.newBootstrapBeganAt != null)
            super.setBootstrapBeganAt(fieldUpdates.newBootstrapBeganAt);

        if (fieldUpdates.newSafeToRead != null)
            super.setSafeToRead(fieldUpdates.newSafeToRead);

        if (fieldUpdates.newRangesForEpoch != null)
            super.setRangesForEpoch(fieldUpdates.newRangesForEpoch);
    }

    /**
     * Persistent field update logic
     */

    @Override
    public final void upsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        // TODO (required): fix RedundantBefore sorting issue and switch to upsert mode
        ensureFieldUpdates().newRedundantBefore = RedundantBefore.merge(redundantBefore(), addRedundantBefore);
        unsafeUpsertRedundantBefore(addRedundantBefore);
    }

    @Override
    public final void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        ensureFieldUpdates().newBootstrapBeganAt = newBootstrapBeganAt;
    }

    @Override
    public final void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        ensureFieldUpdates().newSafeToRead = newSafeToRead;
    }

    @Override
    public void setRangesForEpoch(RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
        {
            super.setRangesForEpoch(rangesForEpoch);
            ensureFieldUpdates().newRangesForEpoch = rangesForEpoch;
        }
    }

    @Override
    public RangesForEpoch rangesForEpoch()
    {
        if (fieldUpdates != null && fieldUpdates.newRangesForEpoch != null)
            return fieldUpdates.newRangesForEpoch;

        return null;
    }

    @Override
    public NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        if (fieldUpdates != null && fieldUpdates.newBootstrapBeganAt != null)
            return fieldUpdates.newBootstrapBeganAt;

        return super.bootstrapBeganAt();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> safeToReadAt()
    {
        if (fieldUpdates != null && fieldUpdates.newSafeToRead != null)
            return fieldUpdates.newSafeToRead;

        return super.safeToReadAt();
    }

    @Override
    public RedundantBefore redundantBefore()
    {
        if (fieldUpdates != null && fieldUpdates.newRedundantBefore != null)
            return fieldUpdates.newRedundantBefore;

        return super.redundantBefore();
    }

    private FieldUpdates ensureFieldUpdates()
    {
        if (fieldUpdates == null) fieldUpdates = new FieldUpdates();
        return fieldUpdates;
    }

    public FieldUpdates fieldUpdates()
    {
        return fieldUpdates;
    }
}
