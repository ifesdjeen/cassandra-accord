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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import accord.api.LocalListeners;
import accord.api.RemoteListeners;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;

// TODO (required): evict to disk
public class DefaultLocalListeners implements LocalListeners
{
    public static class Factory implements LocalListeners.Factory
    {
        final RemoteListeners remoteListeners;
        final NotifySink notifySink;

        public Factory(Node node)
        {
            this(node, DefaultNotifySink.INSTANCE);
        }

        public Factory(Node node, NotifySink notifySink)
        {
            this.remoteListeners = node.remoteListeners();
            this.notifySink = notifySink;
        }

        @Override
        public LocalListeners create(CommandStore store)
        {
            return new DefaultLocalListeners(remoteListeners, notifySink);
        }
    }

    public interface NotifySink
    {
        void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener);
        boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener);
    }

    public static class DefaultNotifySink implements NotifySink
    {
        public static final DefaultNotifySink INSTANCE = new DefaultNotifySink();

        @Override
        public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listenerId)
        {
            SafeCommand listener = safeStore.ifLoadedAndInitialised(listenerId);
            if (listener != null) Commands.listenerUpdate(safeStore, listener, safeCommand);
            else
            {
                //noinspection SillyAssignment,ConstantConditions
                safeStore = safeStore; // prevent use in lambda
                TxnId updatedId = safeCommand.txnId();
                PreLoadContext context = PreLoadContext.contextFor(listenerId, updatedId);
                safeStore.commandStore()
                         .execute(context, safeStore0 -> notify(safeStore0, listenerId, updatedId))
                         .begin(safeStore.agent());
            }
        }

        private static void notify(SafeCommandStore safeStore, TxnId listenerId, TxnId updatedId)
        {
            Commands.listenerUpdate(safeStore, safeStore.unsafeGet(listenerId), safeStore.unsafeGet(updatedId));
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener)
        {
            return listener.notify(safeStore, safeCommand);
        }
    }

    static class TxnListeners extends TxnId
    {
        final SaveStatus await;
        TxnId[] listeners = NO_TXNIDS;
        int count;

        TxnListeners(TxnId txnId, SaveStatus await)
        {
            super(txnId);
            this.await = await;
        }

        public int compareListeners(TxnListeners that)
        {
            int c = this.compareTo(that);
            if (c == 0) c = this.await.compareTo(that.await);
            return c;
        }

        public static int compareBefore(TxnId txnId, TxnListeners that)
        {
            int c = txnId.compareTo(that);
            if (c == 0) c = -1;
            return c;
        }

        public static int compare(TxnId txnId, SaveStatus await, TxnListeners that, int ifEqual)
        {
            int c = txnId.compareTo(that);
            if (c == 0) c = await.compareTo(that.await);
            if (c == 0) c = ifEqual;
            return c;
        }

        void notify(NotifySink notifySink, SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            trim();
            for (int i = 0 ; i < count ; ++i)
            {
                TxnId listenerId = listeners[i];
                notifySink.notify(safeStore, safeCommand, listenerId);
            }
        }

        private int trim()
        {
            Arrays.sort(listeners, 0, count);
            int removedCount = 0;
            for (int i = 1 ; i < count ; ++i)
            {
                if (listeners[i - 1].compareTo(listeners[i]) == 0) ++removedCount;
                else if (removedCount > 0) listeners[i - removedCount] = listeners[i];
            }

            if (removedCount != 0)
            {
                int prevCount = count;
                count -= removedCount;
                Arrays.fill(listeners, count, prevCount, null);
            }
            return removedCount;
        }

        void add(TxnId listener)
        {
            if (count == listeners.length)
            {
                if (count == 0)
                {
                    listeners = new TxnId[4];
                }
                else
                {
                    int removedCount = trim();
                    if (removedCount < listeners.length / 2)
                    {
                        TxnId[] newListeners = new TxnId[count * 2];
                        System.arraycopy(listeners, 0, newListeners, 0, count);
                        listeners = newListeners;
                    }
                }
            }

            listeners[count++] = listener;
        }
    }

    class RegisteredComplexListener implements Registered
    {
        final TxnId txnId;
        final ComplexListener listener;
        RegisteredComplexListener(TxnId txnId, ComplexListener listener)
        {
            this.listener = listener;
            this.txnId = txnId;
        }

        @Override
        public void cancel()
        {
            complexListeners.getOrDefault(txnId, Collections.emptyList()).remove(this);
        }
    }

    private static final EnumMap<SaveStatus, AsymmetricComparator<TxnId, TxnListeners>> compareExact, compareAfter;
    static
    {
        compareAfter = new EnumMap<>(SaveStatus.class);
        compareExact = new EnumMap<>(SaveStatus.class);
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            compareAfter.put(saveStatus, (id, listeners) -> TxnListeners.compare(id, saveStatus, listeners, 1));
            compareExact.put(saveStatus, (id, listeners) -> TxnListeners.compare(id, saveStatus, listeners, 0));
        }
    }

    private final RemoteListeners remoteListeners;
    private final NotifySink notifySink;

    private final ConcurrentHashMap<TxnId, List<RegisteredComplexListener>> complexListeners = new ConcurrentHashMap<>();
    private Object[] txnListeners = BTree.empty();

    public DefaultLocalListeners(RemoteListeners remoteListeners, NotifySink notifySink)
    {
        this.remoteListeners = remoteListeners;
        this.notifySink = notifySink;
    }

    @Override
    public void register(TxnId txnId, SaveStatus await, TxnId listener)
    {
        TxnListeners entry = BTree.find(txnListeners, compareExact.get(await), txnId);
        if (entry == null)
            txnListeners = BTree.update(txnListeners, BTree.singleton(entry = new TxnListeners(txnId, await)), TxnListeners::compareListeners);
        entry.add(listener);
    }

    @Override
    public Registered register(TxnId txnId, ComplexListener listener)
    {
        RegisteredComplexListener entry = new RegisteredComplexListener(txnId, listener);
        complexListeners.compute(txnId, (id, cur) -> {
            if (cur == null)
                cur = new ArrayList<>();
            cur.add(entry);
            return cur;
        });
        return entry;
    }

    @Override
    public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev)
    {
        notifyTxnListeners(safeStore, safeCommand);
        notifyComplexListeners(safeStore, safeCommand);
        remoteListeners.notify(safeStore, safeCommand, prev);
    }

    private void notifyTxnListeners(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Object[] txnListeners = this.txnListeners;
        TxnId txnId = safeCommand.txnId();
        SaveStatus saveStatus = safeCommand.current().saveStatus();
        // TODO (desired): faster iteration, currently this is O(n.lg(n))
        int start = -1 - BTree.findIndex(txnListeners, TxnListeners::compareBefore, txnId);
        int end = -1 - BTree.findIndex(txnListeners, compareAfter.get(saveStatus), txnId);
        while (start < end)
        {
            TxnListeners notify = BTree.findByIndex(txnListeners, start);
            Invariants.checkState(txnId.equals(notify));
            notify.notify(notifySink, safeStore, safeCommand);
            if (this.txnListeners != txnListeners)
            {
                // listener registrations were changed by this listener's notify invocation, so reset our cursor
                txnListeners = this.txnListeners;
                start = BTree.findIndex(txnListeners, TxnListeners::compareListeners, notify);
                end = -1 - BTree.findIndex(txnListeners, compareAfter.get(saveStatus), txnId);
                if (start < 0)
                {
                    // the listener was removed by its invocation, so ignore stillListening and continue to next listener
                    start = -1 - start;
                    continue;
                }
            }

            this.txnListeners = txnListeners = BTreeRemoval.remove(txnListeners, TxnListeners::compareListeners, notify);
            --end;
        }
        this.txnListeners = txnListeners;
    }

    private void notifyComplexListeners(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (complexListeners.get(safeCommand.txnId()) == null)
            return;

        complexListeners.compute(safeCommand.txnId(), (id, cur) -> {
            if (cur == null)
                return null;

            int i = 0, removed = 0;
            while (i < cur.size())
            {
                RegisteredComplexListener next = cur.get(i);
                if (!notifySink.notify(safeStore, safeCommand, next.listener))
                    removed++;
                else if (removed > 0)
                    cur.set(i - removed, next);
                ++i;
            }
            while (removed-- > 0)
                cur.remove(cur.size() - 1);
            return cur;
        });
    }

}
