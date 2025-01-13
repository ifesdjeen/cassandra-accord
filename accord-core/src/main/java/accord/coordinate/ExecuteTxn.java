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

package accord.coordinate;

import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.messages.Request;
import accord.messages.StableThenRead;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import org.agrona.collections.IntHashSet;

import static accord.api.ProtocolModifiers.Toggles.permitLocalExecution;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.messages.Commit.Kind.StableFastPath;
import static accord.messages.Commit.Kind.StableSlowPath;
import static accord.messages.Commit.Kind.StableWithTxnAndDeps;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Phase.Execute;
import static accord.primitives.Status.PreAccepted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (expected): return Waiting from ReadData if not ready to execute, and do not submit more than one speculative retry in this case
// TODO (expected): by default, if we can execute locally, never contact a remote replica regardless of local outcome
public class ExecuteTxn extends ReadCoordinator<ReadReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ExecuteTxn.class);
    private static boolean SEND_MINIMUM_STABLE_MESSAGES = true;
    public static void setSendMinimumStableMessages(boolean sendMin) {SEND_MINIMUM_STABLE_MESSAGES = sendMin; }

    final ExecutePath path;
    final Txn txn;
    final FullRoute<?> route;
    final Timestamp executeAt;
    final Deps stableDeps;
    final Topologies allTopologies;
    final BiConsumer<? super Result, Throwable> callback;
    private Participants<?> readScope;
    private Data data;

    ExecuteTxn(Node node, Topologies topologies, FullRoute<?> route, ExecutePath path, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, topologies.forEpoch(executeAt.epoch()), txnId);
        this.path = path;
        this.txn = txn;
        this.route = route;
        this.allTopologies = topologies;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.callback = callback;
        Invariants.checkState(!txnId.awaitsOnlyDeps());
        Invariants.checkState(!txnId.awaitsPreviouslyOwned());
    }

    @Override
    protected void startOnceInitialised()
    {
        if (permitLocalExecution() && tryIfUniversal(node.id()))
        {
            new LocalExecute(txnId).process(node, node.agent().localExpiresAt(Execute, MICROSECONDS));
        }
        else if (path == FAST && txnId.hasPrivilegedCoordinator())
        {
            // we can't safely take the fast path via PRIVILEGED_COORDINATOR optimisation if we aren't permitted to execute locally,
            // so we take the MEDIUM or SLOW path
            adapter().propose(node, null, route, txnId.hasMediumPath() ? Accept.Kind.MEDIUM : Accept.Kind.SLOW,
                              Ballot.ZERO, txnId, txn, executeAt, stableDeps, callback);
        }
        else
        {
            super.startOnceInitialised();
        }
    }

    @Override
    protected void start(Iterable<Id> to)
    {
        IntHashSet readSet = new IntHashSet();
        to.forEach(i -> readSet.add(i.id));
        Commit.stableAndRead(node, allTopologies, commitKind(), txnId, txn, route, executeAt, stableDeps, readSet, this, SEND_MINIMUM_STABLE_MESSAGES && path != RECOVER);
    }

    private Commit.Kind commitKind()
    {
        switch (path)
        {
            default: throw new UnhandledEnum(path);
            case FAST:    return StableFastPath;
            case SLOW:
            case MEDIUM:  return StableSlowPath;
            case RECOVER: return StableWithTxnAndDeps;
        }
    }

    @Override
    public void contact(Id to)
    {
        CommandStore commandStore = CommandStore.currentOrElseSelect(node, route);
        if (SEND_MINIMUM_STABLE_MESSAGES && path != RECOVER)
        {
            Request request = Commit.requestTo(to, true, allTopologies, commitKind(), Ballot.ZERO, txnId, txn, route, executeAt, stableDeps, false);
            // we are always sending to a replica in the latest epoch and requesting a read, so onlyContactOldAndReadSet is a redundant parameter
            node.send(to, request, commandStore, this);
        }
        else
        {
            if (readScope == null)
                readScope = txn.read().keys().toParticipants();
            node.send(to, new ReadTxnData(to, topologies(), txnId, readScope, executeAt.epoch()), commandStore, this);
        }
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            ReadOk ok = ((ReadOk) reply);
            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Rejected:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case Insufficient:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Commit.stableMaximal(node, from, txn, txnId, executeAt, route, stableDeps);
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            Result result = txn.result(txnId, executeAt, data);
            adapter().persist(node, allTopologies, route, txnId, txn, executeAt, stableDeps, txn.execute(txnId, executeAt, data), result, callback);
        }
        else
        {
            callback.accept(null, failure);
        }
    }

    protected CoordinationAdapter<Result> adapter()
    {
        return node.coordinationAdapter(txnId, Standard);
    }

    @Override
    public String toString()
    {
        return "ExecuteTxn{" +
               "txn=" + txn +
               ", route=" + route +
               '}';
    }

    class LocalExecute extends ReadData
    {
        public LocalExecute(TxnId txnId)
        {
            super(txnId, route, executeAt.epoch());
        }

        @Override
        protected CommitOrReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
        {
            if (txnId.hasPrivilegedCoordinator() && path == FAST && safeCommand.current().status() != PreAccepted)
                return CommitOrReadNack.Rejected;
            Commands.commit(safeStore, safeCommand, participants, Stable, Ballot.ZERO, txnId, route, txn, executeAt, stableDeps);
            return super.apply(safeStore, safeCommand, participants);
        }

        @Override
        protected ExecuteOn executeOn()
        {
            return StableThenRead.EXECUTE_ON;
        }

        @Override
        protected boolean cancel()
        {
            if (!super.cancel())
                return false;

            // TODO (desired): if we fail to commit locally we can submit a slow/medium path request
            if (txnId.hasPrivilegedCoordinator())
                finishOnFailure(new Timeout(txnId, route.homeKey(), "Could not promptly commit on privileged coordinator"), true);
            else
                onFailure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly commit on local coordinator"));
            return true;
        }

        @Override
        protected void reply(ReadReply reply, Throwable fail)
        {
            if (fail == null) onSuccess(node.id(), reply);
            else onFailure(node.id(), fail);
        }

        @Override
        public ReadType kind() { throw new UnsupportedOperationException(); }

        @Override
        public MessageType type() { throw new UnsupportedOperationException(); }
    }
}
