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

package accord.burn;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;

import accord.utils.DefaultRandom;

import static accord.utils.Invariants.illegalArgument;

public class BurnTest extends BurnTestBase
{
    @Test
    public void testOne()
    {
        run(System.nanoTime());
    }

    public static void main(String[] args)
    {
        int count = 1;
        int operations = 1000;
        Long overrideSeed = null;
        boolean reconcile = false;
        LongSupplier seedGenerator = ThreadLocalRandom.current()::nextLong;
        boolean hasOverriddenSeed = false;
        for (int i = 0 ; i < args.length ; i += 2)
        {
            switch (args[i])
            {
                default: throw illegalArgument("Invalid option: " + args[i]);
                case "-c":
                    count = Integer.parseInt(args[i + 1]);
                    if (hasOverriddenSeed)
                        throw illegalArgument("Cannot override both seed (-s) and number of seeds to run (-c)");
                    overrideSeed = null;
                    break;
                case "-s":
                    overrideSeed = Long.parseLong(args[i + 1]);
                    hasOverriddenSeed = true;
                    count = 1;
                    break;
                case "-o":
                    operations = Integer.parseInt(args[i + 1]);
                    break;
                case "-r":
                    reconcile = true;
                    --i;
                    break;
                case "--loop-seed":
                    seedGenerator = new DefaultRandom(Long.parseLong(args[i + 1]))::nextLong;
            }
        }
        while (count-- > 0)
        {
            if (!reconcile)
            {
                run(overrideSeed != null ? overrideSeed : seedGenerator.getAsLong(), operations);
            }
            else
            {
                reconcile(overrideSeed != null ? overrideSeed : seedGenerator.getAsLong(), operations);
            }
        }
    }
}
