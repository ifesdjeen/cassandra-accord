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

import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.WrappableException;

// TODO (expected): move message sending here, so we can invalidate any pending callbacks when we advance the state machine
public abstract class AbstractCoordination<R, C> extends AbstractSimpleCoordination implements Callback<C>
{
    private BiConsumer<? super R, Throwable> callback;

    protected AbstractCoordination(Node node, SequentialAsyncExecutor executor, TxnId txnId, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId);
        this.callback = Invariants.nonNull(callback);
    }

    void finishWithSuccess(R success)
    {
        finishAndInvokeCallback(success, null);
    }

    void finishWithFailureOverride(Throwable failure)
    {
        finishAndInvokeCallback(null, FailureAccumulator.append(failure, this.failure()));
    }

    void finishOnExaustion()
    {
        finishOnFailure();
    }

    void finishOnFailure()
    {
        finishAndInvokeCallback(null, FailureAccumulator.fail(node.agent(), this.failure(), txnId, Route.tryCastToRoute(scope())));
    }

    void withEpochExact(long epoch, Runnable runnable)
    {
        node.withEpochExact(epoch, executor, (ignore, failure) -> finishWithFailureOverride(failure), WrappableException::wrap, runnable);
    }

    @Override
    public boolean onCallbackFailure(Node.Id from, Throwable failure)
    {
        if (isDone())
            return false;
        setDone();
        BiConsumer<?, Throwable> callback = tryTakeCallback();
        if (callback != null) callback.accept(null, failure);
        else node.agent().onUncaughtException(failure);
        return true;
    }

    void finishAndInvokeCallback(R success, Throwable failure)
    {
        finishAndTakeCallback().accept(success, failure);
    }

    BiConsumer<? super R, Throwable> finishAndTakeCallback()
    {
        setDone();
        return takeCallback();
    }

    private BiConsumer<? super R, Throwable> takeCallback()
    {
        BiConsumer<? super R, Throwable> callback = this.callback;
        this.callback = null;
        Invariants.require(callback != null);
        return callback;
    }

    private BiConsumer<? super R, Throwable> tryTakeCallback()
    {
        if (callback == null)
            return null;
        return takeCallback();
    }

    @Override
    public boolean abort()
    {
        if (isDone())
            return false;

        finishWithFailureOverride(Aborted.aborted(txnId, Route.tryCastToRoute(scope())));
        return true;
    }

    @Override
    public String toString()
    {
        String describe = describe();
        return getClass().getSimpleName() + ":" + txnId + (describe.isEmpty() ? "" : ": " + describe);
    }
}
