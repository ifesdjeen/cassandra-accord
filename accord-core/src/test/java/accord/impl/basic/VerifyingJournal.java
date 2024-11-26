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

package accord.impl.basic;

import java.util.NavigableMap;

import accord.api.Journal;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

/**
 * A simple version of journal that can be useful for debugging issues with an implementation that checks command loading
 * from SUT vs model.
 */
public class VerifyingJournal implements Journal
{
    private final Journal sut;
    private final Journal model;

    public VerifyingJournal(Journal model, Journal sut)
    {
        this.model = model;
        this.sut = sut;
    }

    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Command model = this.model.loadCommand(commandStoreId, txnId, redundantBefore, durableBefore);
        Command sut = this.sut.loadCommand(commandStoreId, txnId, redundantBefore, durableBefore);
        Invariants.checkState(sut.equals(model));
        return sut;
    }

    public void saveCommand(int store, CommandUpdate update, Runnable onFlush)
    {
        model.saveCommand(store, update, null);
        sut.saveCommand(store, update, onFlush);
    }

    public void purge(CommandStores commandStores)
    {
        model.purge(commandStores);
        sut.purge(commandStores);
    }

    public void replay(CommandStores commandStores)
    {
        sut.replay(commandStores);
    }

    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        RedundantBefore model = this.model.loadRedundantBefore(commandStoreId);
        RedundantBefore sut = this.sut.loadRedundantBefore(commandStoreId);
        Invariants.checkState(sut.equals(model), "%s should equal %s", sut, model);
        return sut;
    }

    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        NavigableMap<TxnId, Ranges> model = this.sut.loadBootstrapBeganAt(commandStoreId);
        NavigableMap<TxnId, Ranges> sut = this.sut.loadBootstrapBeganAt(commandStoreId);
        Invariants.checkState(sut.equals(model), "%s should equal %s", sut, model);
        return sut;
    }

    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        NavigableMap<Timestamp, Ranges> model = this.model.loadSafeToRead(commandStoreId);
        NavigableMap<Timestamp, Ranges> sut = this.sut.loadSafeToRead(commandStoreId);
        Invariants.checkState(sut.equals(model), "%s should equal %s", sut, model);
        return sut;
    }

    public CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId)
    {
        CommandStores.RangesForEpoch model = this.sut.loadRangesForEpoch(commandStoreId);
        CommandStores.RangesForEpoch sut = this.sut.loadRangesForEpoch(commandStoreId);
        Invariants.checkState(sut.equals(model), "%s should equal %s", sut, model);
        return sut;
    }

    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        model.saveStoreState(store, fieldUpdates, onFlush);
        sut.saveStoreState(store, fieldUpdates, onFlush);
    }
}
