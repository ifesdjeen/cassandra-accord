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

import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.DebugMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;

import static accord.coordinate.ExecutePath.SLOW;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Kind.CommitWithTxn;
import static accord.utils.Invariants.debug;

public abstract class Stabilise<R> extends AbstractCoordination<R, ReadReply> implements Callback<ReadReply>
{
    final Txn txn;
    final FullRoute<?> route;
    final Route<?> sendTo;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stabiliseDeps;

    final QuorumTracker tracker;
    final Topologies allTopologies;
    private final DebugMap debug;

    public Stabilise(Node node, SequentialAsyncExecutor executor, Topologies coordinates, Topologies allTopologies, Route<?> sendTo, FullRoute<?> route, TxnId txnId, Ballot ballot, Txn txn, Timestamp executeAt, Deps stabiliseDeps, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId, callback);
        this.txn = txn;
        this.sendTo = sendTo;
        this.route = route;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.stabiliseDeps = stabiliseDeps;
        // we only care about coordination epoch for stability, as it is a recovery condition
        this.tracker = new QuorumTracker(coordinates);
        this.allTopologies = allTopologies;
        this.debug = debug() ? new DebugMap(allTopologies.nodes()) : null;
    }

    @Override
    void start()
    {
        SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (allTopologies.size() > 1)
            contact = contact.with(allTopologies.nodes().without(tracker.nodes()).without(allTopologies::isFaulty));

        if (contact == null)
        {
            finishOnExaustion();
        }
        else
        {
            super.start();
            Commit.commitMinimalNoRead(contact, node, executor, tracker.topologies(), allTopologies, ballot, txnId, txn, route, executeAt, stabiliseDeps, this);
        }
    }

    @Override
    public void onSuccess(Node.Id from, ReadReply reply)
    {
        if (isDone())
            return;

        if (debug != null)
            debug.debug(from, reply);

        if (reply.isOk())
        {
            if (tracker.recordSuccess(from) == RequestStatus.Success)
                onStabilised();
        }
        else
        {
            switch ((CommitOrReadNack)reply)
            {
                default: throw new UnhandledEnum((CommitOrReadNack)reply);
                case Redundant:
                    finishWithFailureOverride(new Redundant(txnId, route.homeKey(), executeAt));
                    break;
                case Rejected:
                    recordFailure(from, Preempted.preempted(node.agent(), txnId, route.homeKey()));
                    break;
                case Insufficient:
                    node.send(from, new Commit(CommitWithTxn, from, allTopologies,
                                               txnId, txn, route, ballot, executeAt, stabiliseDeps));
                    break;
            }
        }
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        if (isDone())
            return;

        if (debug != null)
            debug.debug(from, failure);

        recordFailure(from, failure);
    }

    private void recordFailure(Node.Id from, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    protected void onStabilised()
    {
        adapter().execute(node, executor, allTopologies, route, ballot, SLOW, CoordinationFlags.none(), txnId, txn, executeAt, stabiliseDeps, stabiliseDeps, finishAndTakeCallback());
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Stabilise;
    }

    @Override
    public Unseekables<?> scope()
    {
        return route;
    }

    @Override
    public Ballot ballot()
    {
        return ballot;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "ballot=" + ballot + ", executeAt=" + executeAt;
    }

    protected abstract CoordinationAdapter<R> adapter();
}
