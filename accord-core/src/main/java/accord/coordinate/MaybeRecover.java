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

import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.primitives.*;
import accord.utils.Invariants;

import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShards<Route<?>>
{
    final ProgressToken prevProgress;
    final BiConsumer<Outcome, Throwable> callback;
    final long reportLowEpoch, reportHighEpoch;

    MaybeRecover(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Route<?> someRoute, ProgressToken prevProgress, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        // we only want to enquire with the home shard, but we prefer maximal route information for running Invalidation against, if necessary
        super(node, txnId, someRoute.withHomeKey(), IncludeInfo.Route, invalidIf);
        this.prevProgress = prevProgress;
        this.callback = callback;
        this.reportLowEpoch = reportLowEpoch;
        this.reportHighEpoch = reportHighEpoch;
    }

    public static Object maybeRecover(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Route<?> someRoute, ProgressToken prevProgress, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, invalidIf, someRoute, prevProgress, reportLowEpoch, reportHighEpoch, callback);
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        // We don't accept a single truncated response - must have a quorum so we can make inferences about invalidation
        return !ok.map.hasTruncated() && (hasMadeProgress(ok) || ok.durability.isDurableOrInvalidated());
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        // TODO (required, liveness): if Ballot.hlc is stale enough then preempt; also do not query isCoordinating, query directly the node that owns the ballot (or TxnId if Ballot is ZERO)
        return ok != null && (ok.isCoordinating
                              || ok.toProgressToken().compareTo(prevProgress) > 0);
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        // TODO (desired): we don't need a full quorum to proceed, just a quorum that intersects a full quorum (i.e. when rf=2, we need only 1 reply)
        //  this can be helpful in mitigating flakiness and helping forward progress for large transactions spanning many shards
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else
        {
            Invariants.checkState(merged != null);
            CheckStatusOk full = merged.finish(this.route, this.route, this.route, success.withQuorum, previouslyKnownToBeInvalidIf);
            Known known = full.maxKnown();
            Route<?> someRoute = full.route;

            switch (known.outcome())
            {
                default: throw new AssertionError();
                case Unknown:
                    // ErasedOrInvalidated takes Unknown, and so permits invalidation to be initiated.
                    // This might prima facie seem unsafe, as ErasedOrInvalidated might mean the command
                    // has been executed and erased, in which case it is not safe to invalidate.
                    // However, replicas that have erased the history for these commands also cannot vote to proceed with
                    // invalidation, so the Invalidation state machine must special-case this scenario.
                    // If there exists a shard that has not been decided, then the outcome must be invalidated and it
                    // may be disseminated globally. However, if all shards are erased then the outcome must be
                    // decided locally by the application of GC points.
                    // TODO (expected): replicas may be stale in this case, and should detect this and stop attempting to coordinate/invalidate.
                    if (known.canProposeInvalidation() && !Route.isFullRoute(full.route))
                    {
                        // for correctness reasons, we have not necessarily preempted the initial pre-accept round and
                        // may have raced with it, so we must attempt to recover anything we see pre-accepted.
                        Invalidate.invalidate(node, txnId, someRoute, callback);
                        break;
                    }
                    // fall through otherwise to recovery

                case Apply:
                    // we have included the home key, and one that witnessed the definition has responded, so it should also know the full route
                    if (hasMadeProgress(full))
                    {
                        if (full.durability.isDurable())
                            InformDurable.informDefault(node, topologies, txnId, route, full.executeAtIfKnown(), full.durability);
                        callback.accept(full.toProgressToken(), null);
                    }
                    else
                    {
                        Invariants.checkState(Route.isFullRoute(someRoute), "Require a full route but given %s", full.route);
                        node.recover(txnId, full.invalidIf, Route.castToFullRoute(someRoute), reportLowEpoch, reportHighEpoch).addCallback(callback);
                    }
                    break;

                case WasApply:
                case Erased:
                    // TODO (required): if we're home replica, don't want to cancel coordination without either invalidating or applying unless we're stale
                    // TODO (required): on Erased, should we maybe mark stale, or leave to FetchData?
                    callback.accept(full.toProgressToken(), null);
                    break;

                case Abort:
                    Commit.Invalidate.commitInvalidate(node, txnId, Route.merge(full.route, (Route)route), txnId.epoch());
                    locallyInvalidateAndCallback(node, txnId, txnId, txnId, someRoute, full.toProgressToken(), callback);
                    break;
            }
        }
    }
}
