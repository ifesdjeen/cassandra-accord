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

package accord.messages;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.api.LocalListeners;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.RemoteListeners;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.messages.TxnRequest.computeScope;

public class Await implements Request, MapReduceConsume<SafeCommandStore, Void>, PreLoadContext, LocalListeners.ComplexListener
{
    public static class SerializerSupport
    {
        public static Await create(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, int callbackId)
        {
            return new Await(txnId, scope, blockedUntil, callbackId);
        }
    }

    public final TxnId txnId;
    public final Participants<?> scope;
    public final BlockedUntil blockedUntil;
    public final int callbackId; // < 0 means synchronous await

    private transient Node node;
    private transient Id replyTo;
    private transient ReplyContext replyContext;

    private transient volatile RemoteListeners.Registration registration;
    private static final AtomicReferenceFieldUpdater<Await, RemoteListeners.Registration> registrationUpdater = AtomicReferenceFieldUpdater.newUpdater(Await.class, RemoteListeners.Registration.class, "registration");

    private transient volatile int synchronouslyWaitingOn;
    private static final AtomicIntegerFieldUpdater<Await> synchronouslyWaitingOnUpdater = AtomicIntegerFieldUpdater.newUpdater(Await.class, "synchronouslyWaitingOn");

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil)
    {
        this(to, topologies, txnId, participants, blockedUntil, Integer.MIN_VALUE);
    }

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil, int callbackId)
    {
        this.txnId = txnId;
        this.callbackId = callbackId;
        this.scope = computeScope(to, topologies, participants);
        this.blockedUntil = blockedUntil;
    }

    public Await(Id to, Topology topology, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil)
    {
        this(to, topology, txnId, participants, blockedUntil, Integer.MIN_VALUE);
    }

    public Await(Id to, Topology topology, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil, int callbackId)
    {
        this.txnId = txnId;
        this.scope = participants.slice(topology.rangesForNode(to));
        this.blockedUntil = blockedUntil;
        this.callbackId = callbackId;
    }

    private Await(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, int callbackId)
    {
        this.txnId = txnId;
        this.scope = scope;
        this.blockedUntil = blockedUntil;
        this.callbackId = callbackId;
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.node = node;
        this.replyTo = replyToNode;
        this.replyContext = replyContext;
        node.mapReduceConsumeLocal(this, scope, txnId.epoch(), txnId.epoch(), this);
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, scope);
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(blockedUntil.minSaveStatus) >= 0)
            return null;

        // TODO (desired): consider merging these methods?
        command = Commands.updateRouteOrParticipants(safeCommand, scope);
        Commands.ensureHomeIsMonitoring(safeStore, safeCommand, command, scope);

        if (callbackId >= 0)
        {
            RemoteListeners.Registration registered = registration;
            if (registered == null)
            {
                registered = node.remoteListeners().register(txnId, blockedUntil.minSaveStatus, blockedUntil.remoteDurability, replyTo, callbackId);
                if (!registrationUpdater.compareAndSet(this, null, registered))
                {
                    registered.cancel();
                    registered = registration;
                }
            }
            registered.add(safeStore, safeCommand);
        }
        else
        {
            synchronouslyWaitingOnUpdater.incrementAndGet(this);
            safeStore.register(txnId, this);
        }

        safeStore.progressLog().waiting(blockedUntil, safeCommand, null, scope);
        return null;
    }

    @Override
    public Void reduce(Void o1, Void o2)
    {
        return null;
    }

    @Override
    public void accept(Void result, Throwable failure)
    {
        if (failure != null)
        {
            registration.cancel();
            node.reply(replyTo, replyContext, null, failure);
        }
        else if (callbackId >= 0)
        {
            AwaitOk reply = registration == null || 0 == registration.done() ? AwaitOk.Ready : AwaitOk.NotReady;
            node.reply(replyTo, replyContext, reply, null);
        }
        else
        {
            if (-1 == synchronouslyWaitingOnUpdater.decrementAndGet(this))
                node.reply(replyTo, replyContext, AwaitOk.Ready, null);
        }
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public MessageType type()
    {
        return MessageType.AWAIT_REQ;
    }

    public enum AwaitOk implements Reply
    {
        NotReady, Ready;

        @Override
        public MessageType type()
        {
            return MessageType.AWAIT_RSP;
        }
    }

    public static class AsyncAwaitComplete implements Request, PreLoadContext, Consumer<SafeCommandStore>
    {
        public final TxnId txnId;
        public final Route<?> route; // at least those Routable we registered with
        public final SaveStatus newStatus;
        public final int callbackId;
        transient Node.Id from;

        public AsyncAwaitComplete(TxnId txnId, Route<?> route, SaveStatus newStatus, int callbackId)
        {
            this.txnId = txnId;
            this.route = Invariants.checkArgument(route, route != null);
            this.newStatus = newStatus;
            this.callbackId = callbackId;
        }

        @Override
        public MessageType type()
        {
            return MessageType.ASYNC_AWAIT_COMPLETE_REQ;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            this.from = from;
            node.forEachLocal(this, route, txnId.epoch(), Long.MAX_VALUE, this);
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            if (safeCommand == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
                return;

            Command command = safeCommand.current();
            if (!Route.isFullRoute(command.route()))
                safeCommand.updateAttributes(command.mutable().route(route));
            safeStore.progressLog().remoteCallback(safeStore, safeCommand, newStatus, callbackId, from);
        }
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch();
    }

    @Override
    public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(blockedUntil.minSaveStatus) >= 0)
            return true;

        if (-1 == synchronouslyWaitingOnUpdater.decrementAndGet(this))
            node.reply(replyTo, replyContext, AwaitOk.Ready, null);

        return false;
    }
}
