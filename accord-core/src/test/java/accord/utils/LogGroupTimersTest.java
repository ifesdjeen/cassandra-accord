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

package accord.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class LogGroupTimersTest
{
    @Test
    public void test()
    {
        Random random = new Random();
        for (int i = 0 ; i < 1000 ; ++i)
            testOne(random.nextLong(), 1000, 100);
    }

    @Test
    public void testOne()
    {
        testOne(-5699729545424491488L, 1000, 100);
    }

    static class Timer extends LogGroupTimers.Timer
    {
        final long deadline;
        final int randomWeight;
        final int uniqueId;
        Timer(long deadline, int randomWeight, int uniqueId)
        {
            this.deadline = deadline;
            this.randomWeight = randomWeight;
            this.uniqueId = uniqueId;
        }

        private static int compareDeadline(Timer a, Timer b)
        {
            int c = Long.compare(a.deadline, b.deadline);
            if (c == 0) c = Integer.compare(a.uniqueId, b.uniqueId);
            return c;
        }

        private static int compareUnique(Timer a, Timer b)
        {
            return Integer.compare(a.uniqueId, b.uniqueId);
        }

        private static int compareRandom(Timer a, Timer b)
        {
            int c = Integer.compare(a.randomWeight, b.randomWeight);
            if (c == 0) c = Integer.compare(a.uniqueId, b.uniqueId);
            return c;
        }

        @Override
        public String toString()
        {
            return String.format("[%d,%d]", deadline, uniqueId);
        }
    }

    private void testOne(long seed, int rounds, int maxBatchSize)
    {
        RandomTestRunner.test().withSeed(seed).check(rnd -> {
            TimeUnit scale = rnd.randomWeightedPicker(new TimeUnit[] { MINUTES, HOURS, DAYS }).get();
            long globalMaxDeadline = scale.toMicros(rnd.nextInt(1, 8));
            long maxDeadlineClustering = scale.toMicros(1) / rnd.nextInt(1, 8);
            int maxRemovalBatchSize = Math.max(1, (int) ((0.4f + (rnd.nextFloat() * 0.5f)) * maxBatchSize));
            LogGroupTimers<Timer> test = new LogGroupTimers<>(MICROSECONDS);
            TreeSet<Timer> canonical = new TreeSet<>(Timer::compareDeadline);
            PriorityQueue<Timer> randomOrder = new PriorityQueue<>(Timer::compareRandom);
            int timerId = 0;
            for (int round = 0 ; round < rounds ; ++round)
            {
                int adds = rnd.nextInt(maxBatchSize);
                int removes = rnd.nextInt(maxRemovalBatchSize);
                long deadlineClustering = rnd.nextLong(maxDeadlineClustering / 8, maxDeadlineClustering);
                long minDeadline = rnd.nextLong(globalMaxDeadline - deadlineClustering);
                long maxDeadline = minDeadline + deadlineClustering;

                while (adds > 0 || (removes > 0 && !canonical.isEmpty()))
                {
                    boolean shouldAdd = adds > 0 && (removes == 0 || canonical.isEmpty() || rnd.nextBoolean());
                    if (shouldAdd)
                    {
                        --adds;
                        Timer add = new Timer(rnd.nextLong(minDeadline, maxDeadline), rnd.nextInt(), ++timerId);
                        test.add(add.deadline, add);
                        canonical.add(add);
                        randomOrder.add(add);
                    }
                    else
                    {
                        --removes;
                        Timer remove = randomOrder.poll();
                        canonical.remove(remove);
                        test.remove(remove);
                    }
                }
            }

            Assertions.assertEquals(canonical.size(), test.size());
            List<Timer> nextCanonical = new ArrayList<>();
            List<Timer> nextTest = new ArrayList<>();
            @SuppressWarnings("unused") int i = 0; // to assist debugging
            while (!canonical.isEmpty())
            {
                int batchSize = Math.min(canonical.size(), rnd.nextInt(1, maxBatchSize));
                if (rnd.nextBoolean())
                {
                    for (int j = 0 ; j < batchSize ; ++j)
                    {
                        nextCanonical.add(canonical.pollFirst());
                        nextTest.add(test.poll());
                        while (!canonical.isEmpty() && canonical.first().deadline == nextCanonical.get(nextCanonical.size() - 1).deadline)
                        {
                            nextCanonical.add(canonical.pollFirst());
                            nextTest.add(test.poll());
                            --batchSize;
                        }
                    }

                    for (int j = 1 ; j < nextTest.size() ; ++j)
                        Assertions.assertTrue(nextTest.get(j - 1).deadline <= nextTest.get(j).deadline);
                }
                else
                {
                    for (int j = 0 ; j < batchSize ; ++j)
                        nextCanonical.add(canonical.pollFirst());
                    while (!canonical.isEmpty() && canonical.first().deadline == nextCanonical.get(nextCanonical.size() - 1).deadline)
                        nextCanonical.add(canonical.pollFirst());

                    long advanceTo = nextCanonical.get(nextCanonical.size() - 1).deadline;
                    test.advance(advanceTo, nextTest, List::add);
                }

                nextTest.sort(Timer::compareDeadline);
                Assertions.assertEquals(nextCanonical, nextTest);
                nextCanonical.clear();
                nextTest.clear();
                ++i;
            }
        });
    }

}
