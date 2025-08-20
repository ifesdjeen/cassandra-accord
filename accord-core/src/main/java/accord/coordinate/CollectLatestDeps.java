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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.GetLatestDeps;
import accord.messages.GetLatestDeps.GetLatestDepsOk;
import accord.messages.GetLatestDeps.GetLatestDepsReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedList;
import accord.utils.SortedListMap;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Routables.Slice.Minimal;

public class CollectLatestDeps extends AbstractCoordination<List<LatestDeps>, GetLatestDepsReply>
{
    final Route<?> route;
    final Timestamp executeAt;
    final @Nullable Ballot ballot;

    private final SortedListMap<Id, GetLatestDepsOk> oks;
    private final QuorumTracker tracker;

    CollectLatestDeps(Node node, Topologies topologies, TxnId txnId, Route<?> route, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        super(node, node.someSequentialExecutor(), txnId, callback);
        this.route = route;
        this.executeAt = executeAt;
        this.ballot = ballot;
        this.oks = new SortedListMap<>(topologies.nodes(), GetLatestDepsOk[]::new);
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withLatestDeps(Node node, TxnId txnId, FullRoute<?> fullRoute, Unseekables<?> collectFrom, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        Route<?> route = fullRoute.intersecting(collectFrom, Minimal);
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        CollectLatestDeps collect = new CollectLatestDeps(node, topologies, txnId, route, ballot, executeAt, callback);
        collect.start();
    }

    @Override
    void start()
    {
        SortedArrayList<Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            finishOnExaustion();
        }
        else
        {
            super.start();
            node.send(contact, to -> new GetLatestDeps(to, tracker.topologies(), route, txnId, ballot, executeAt), executor, this);
        }
    }

    @Override
    public void onSuccess(Id from, GetLatestDepsReply ok)
    {
        if (isDone())
            return;

        if (ok.isOk())
        {
            oks.put(from, (GetLatestDepsOk) ok);
            if (tracker.recordSuccess(from) == Success)
                onQuorum();
        }
        else
        {
            onFailure(from, null);
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone())
            return;

        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    private void onQuorum()
    {
        if (isDone())
            return;

        List<LatestDeps> result = new ArrayList<>(oks.size());
        for (GetLatestDepsOk ok : oks.values())
            result.add(ok.deps);
        finishWithSuccess(result);
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.CollectLatestDeps;
    }

    @Override
    public Unseekables<?> scope()
    {
        return route;
    }

    @Override
    public SortedList<Id> nodes()
    {
        return tracker.nodes();
    }

    @Override
    public SortedListMap<Id, ?> replies()
    {
        return oks;
    }
}
