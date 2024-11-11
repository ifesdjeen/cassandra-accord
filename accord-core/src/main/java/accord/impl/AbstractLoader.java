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

import java.util.function.BiConsumer;

import accord.api.Journal;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.Commands;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;

import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;

public abstract class AbstractLoader implements Journal.Loader
{
    protected Command loadInternal(Command command, SafeCommandStore safeStore)
    {
        TxnId txnId = command.txnId();
        if (command.status() != Truncated && command.status() != Invalidated)
        {
            Cleanup cleanup = Cleanup.shouldCleanup(safeStore, command, command.participants());
            switch (cleanup)
            {
                case NO:
                    break;
                case INVALIDATE:
                case TRUNCATE_WITH_OUTCOME:
                case TRUNCATE:
                case ERASE:
                    command = Commands.purge(command, command.participants(), cleanup);
            }
        }

        return safeStore.unsafeGet(txnId).update(safeStore, command);
    }

    protected void applyWrites(Command command, SafeCommandStore safeStore, BiConsumer<SafeCommand, Command> apply)
    {
        TxnId txnId = command.txnId();
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        Command local = safeCommand.current();
        if (local.is(Stable) || local.is(PreApplied))
        {
            Commands.maybeExecute(safeStore, safeCommand, local, true, true);
        }
        else if (local.saveStatus().compareTo(Applying) >= 0 && !local.hasBeen(Truncated))
        {
            apply.accept(safeCommand, local);
        }
    }
}
