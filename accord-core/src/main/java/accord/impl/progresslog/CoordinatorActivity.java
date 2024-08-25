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

package accord.impl.progresslog;

/**
 * What this replica is doing to ensure the transaction is successfully coordinated
 */
enum CoordinatorActivity
{
    /**
     * Not currently known to be the home shard
     */
    NotInitialised,

    /**
     * Another replica is known to be coordinating, so we simply monitor to ensure it continues
     * TODO (expected): implement this behavioural refinement
     */
    Monitoring,

    /**
     * No other replica is known to be coordinating, so we are volunteering to coordinate by invoking MaybeRecover
     */
    Volunteering,

    /**
     * We are actively coordinating the transaction
     * TODO (expected): implement this behavioural refinement
     */
    Coordinating,
    ;

    private static final CoordinatorActivity[] lookup = values();

    public static CoordinatorActivity forOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal > lookup.length)
            throw new IndexOutOfBoundsException(ordinal);
        return lookup[ordinal];
    }
}
