/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.ducktest.tests;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Loading random uuids to cache.
 */
public class UuidDataStreamerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws InterruptedException {
        String cacheName = jNode.get("cacheName").asText();

        int dataSize = Optional.ofNullable(jNode.get("dataSize"))
                .map(JsonNode::asInt)
                .orElse(1024);

        long iterSize = Optional.ofNullable(jNode.get("iterSize"))
                .map(JsonNode::asLong)
                .orElse(1024L);

        assert dataSize > 0;
        assert iterSize > 0;

        CacheConfiguration<UUID, byte[]> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(2);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setIndexedTypes(UUID.class, byte[].class);

        ignite.getOrCreateCache(cacheCfg);

        long start = System.currentTimeMillis();

        markInitialized();

        workParallel(ignite, cacheName, iterSize, dataSize);

        recordResult("DURATION", System.currentTimeMillis() - start);

        markFinished();
    }

    /** */
    private void workParallel(Ignite ignite, String cacheName, long iterSize, int dataSize)
            throws InterruptedException {
        int threads = Runtime.getRuntime().availableProcessors() / 2;

        long iterThread = iterSize / threads;

        assert iterThread > 0;

        CountDownLatch latch = new CountDownLatch(threads);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("UuidDataStreamer-%d")
                .build();

        for (int i = 0; i < threads; i++)
            threadFactory.newThread(new UuidDataStreamer(ignite, cacheName, latch, iterThread, dataSize))
                    .start();

        latch.await();
    }

    /** */
    private static class UuidDataStreamer implements Runnable {
        /** Ignite. */
        private final Ignite ignite;

        /** Cache name. */
        private final String cacheName;

        /** Latch. */
        private final CountDownLatch latch;

        /** Iteration size. */
        private final long iterSize;

        /** Data size. */
        private final int dataSize;

        /** */
        public UuidDataStreamer(Ignite ignite, String cacheName, CountDownLatch latch, long iterSize, int dataSize) {
            this.ignite = ignite;
            this.cacheName = cacheName;
            this.latch = latch;
            this.iterSize = iterSize;
            this.dataSize = dataSize;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (IgniteDataStreamer<UUID, byte[]> dataStreamer = ignite.dataStreamer(cacheName)) {
                dataStreamer.autoFlushFrequency(100L);

                for (long i = 0L; i <= iterSize; i++) {
                    UUID uuid = UUID.randomUUID();

                    byte[] data = new byte[dataSize];

                    ThreadLocalRandom.current().nextBytes(data);

                    dataStreamer.addData(uuid, data);
                }
            }

            latch.countDown();
        }
    }
}
