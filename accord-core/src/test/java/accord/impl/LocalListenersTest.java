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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.LocalListeners;
import accord.api.LocalListeners.ComplexListener;
import accord.api.RemoteListeners.NoOpRemoteListeners;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;

import static accord.local.Status.Durability.NotDurable;

public class LocalListenersTest
{
    @Test
    public void test()
    {
        Random random = new Random();
        for (int i = 0 ; i < 100 ; ++i)
            testOne(random.nextLong(), 1000 + random.nextInt(9000));
    }

    @Test
    public void testOne()
    {
        testOne(-392552404799190391L, 10000);
    }

    private void testOne(long seed, int ops)
    {
        System.out.println(seed);
        RandomTestRunner.test().withSeed(seed).check(rnd -> new TestCase(rnd).run(ops));
    }

    static class TestCase
    {
        final RandomSource rnd;
        final Supplier<TxnId> txnIds;
        final Supplier<SaveStatus> awaits;

        final SavingNotifySink canonicalSink = new SavingNotifySink();
        final TreeMap<TxnId, TxnState> canonical = new TreeMap<>();
        final SavingNotifySink testSink = new SavingNotifySink();
        final LocalListeners test = new DefaultLocalListeners(new NoOpRemoteListeners(), testSink);
        int nextId = 0;

        TestCase(RandomSource rnd)
        {
            this.rnd = rnd;
            Supplier<TxnId> txnIdSupplier = AccordGens.txnIds().asSupplier(rnd);
            if (rnd.decide(0.5f)) this.txnIds = txnIdSupplier;
            else
            {
                int count = 8 << rnd.nextInt(1, 6);
                TxnId[] txnIds = Stream.generate(txnIdSupplier).limit(count).toArray(TxnId[]::new);
                this.txnIds = () -> txnIds[rnd.nextInt(txnIds.length)];
            }
            this.awaits = rnd.randomWeightedPicker(SaveStatus.values());
        }

        void run(int ops)
        {
            float notifyChance = 0.5f * rnd.nextFloat();
            float objRatio = rnd.nextFloat();
            for (int op = 0; op < ops; ++op)
            {
                if (canonical.isEmpty() || !rnd.decide(notifyChance)) registerOne(objRatio);
                else notifyOne(awaits.get());
            }
            while (!canonical.isEmpty())
                notifyOne(SaveStatus.Invalidated);
        }

        void registerOne(float objRatio)
        {
            TxnId txnId = txnIds.get();
            TxnState state = canonical.computeIfAbsent(txnId, ignore -> new TxnState());
            int registerCount = rnd.nextInt(1, 10);
            while (registerCount-- > 0)
            {
                if (rnd.decide(objRatio))
                {
                    int invokeCount = rnd.nextInt(1, 4);
                    test.register(txnId, new TestListener(nextId, invokeCount));
                    state.objs.add(new TestListener(nextId, invokeCount));
                    ++nextId;
                }
                else
                {
                    SaveStatus await = awaits.get();
                    TxnId listenerId = txnIds.get();
                    state.txns.computeIfAbsent(await, ignore -> new TreeSet<>()).add(listenerId);
                    test.register(txnId, await, listenerId);
                }
            }
        }

        void notifyOne(SaveStatus newStatus)
        {
            TxnId txnId = txnIds.get();
            txnId = canonical.floorKey(txnId);
            if (txnId == null) txnId = canonical.firstKey();
            TxnState state = canonical.get(txnId);
            TestSafeCommand safeCommand = new TestSafeCommand(txnId, newStatus, NotDurable);
            state.objs.removeIf(listener -> !canonicalSink.notify(null, safeCommand, listener));
            Map<SaveStatus, TreeSet<TxnId>> subMap = state.txns.headMap(newStatus, true);
            subMap.values().forEach(ids -> ids.forEach(id -> canonicalSink.notify(null, safeCommand, id)));
            test.notify(null, safeCommand, null);
            Assertions.assertEquals(canonicalSink.objs, testSink.objs);
            Assertions.assertEquals(canonicalSink.objResults, testSink.objResults);
            Assertions.assertEquals(canonicalSink.txns, testSink.txns);
            subMap.clear();
            if (state.txns.isEmpty() && state.objs.isEmpty())
                canonical.remove(txnId);
            canonicalSink.clear();
            testSink.clear();
        }
    }

    static class TxnState
    {
        final TreeMap<SaveStatus, TreeSet<TxnId>> txns = new TreeMap<>();
        final Set<ComplexListener> objs = new LinkedHashSet<>();
    }

    static class SavingNotifySink implements DefaultLocalListeners.NotifySink
    {
        final List<TxnId> txns = new ArrayList<>();
        final List<ComplexListener> objs = new ArrayList<>();
        final List<Boolean> objResults = new ArrayList<>();

        @Override
        public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener)
        {
            txns.add(listener);
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener)
        {
            objs.add(listener);
            boolean result = listener.notify(safeStore, safeCommand);
            objResults.add(result);
            return result;
        }

        void clear()
        {
            txns.clear();
            objs.clear();
        }
    }

    static class TestListener implements ComplexListener
    {
        final int id;
        int count;

        public TestListener(int id, int count)
        {
            this.id = id;
            this.count = count;
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Invariants.checkState(count > 0);
            return --count > 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj.getClass() == TestListener.class && ((TestListener) obj).id == id;
        }

        @Override
        public String toString()
        {
            return Integer.toString(id);
        }
    }

    static class TestSafeCommand extends SafeCommand
    {
        Command current;
        public TestSafeCommand(TxnId txnId, SaveStatus saveStatus, final Durability durability)
        {
            super(txnId);
            current = new TestCommand(txnId, saveStatus, durability);
        }

        @Override
        public Command current() { return current; }

        @Override
        public void invalidate() {}

        @Override
        public boolean invalidated() { return false; }

        @Override
        protected void set(Command command)
        {
            current = command;
        }
    }

    static class TestCommand extends Command
    {
        final TxnId txnId;
        final SaveStatus saveStatus;
        final Durability durability;

        TestCommand(TxnId txnId, SaveStatus saveStatus, Durability durability)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.durability = durability;
        }

        @Nullable
        @Override
        public Route<?> route()
        {
            return null;
        }

        @Nullable
        @Override
        public Participants<?> participants()
        {
            return null;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public Ballot promised()
        {
            return null;
        }

        @Override
        public Durability durability()
        {
            return durability;
        }

        @Override
        public SaveStatus saveStatus()
        {
            return saveStatus;
        }

        @Override
        public Timestamp executeAt()
        {
            return null;
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return null;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Nullable
        @Override
        public Seekables<?, ?> additionalKeysOrRanges()
        {
            return null;
        }

        @Override
        public Seekables<?, ?> keysOrRanges()
        {
            return null;
        }

        @Nullable
        @Override
        public PartialDeps partialDeps()
        {
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return null;
        }
    }



}
