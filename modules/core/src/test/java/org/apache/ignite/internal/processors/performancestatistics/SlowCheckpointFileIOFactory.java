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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;

/**
 * Create File I/O that emulates poor checkpoint write speed.
 */
public class SlowCheckpointFileIOFactory implements FileIOFactory {
    /** Default checkpoint park nanos. */
    private static final int DEFAULT_CHECKPOINT_PARK_NANOS = 50_000_000;

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Delegate factory. */
    private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

    /** Checkpoint park nanos. */
    private final long checkpointParkNanos;

    /** */
    public SlowCheckpointFileIOFactory() {
        this.checkpointParkNanos = DEFAULT_CHECKPOINT_PARK_NANOS;
    }

    /** */
    public SlowCheckpointFileIOFactory(long checkpointParkNanos) {
        this.checkpointParkNanos = checkpointParkNanos;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
        final FileIO delegate = delegateFactory.create(file, openOption);

        return new FileIODecorator(delegate) {
            @Override public int write(ByteBuffer srcBuf) throws IOException {
                parkIfNeeded();

                return delegate.write(srcBuf);
            }

            @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                parkIfNeeded();

                return delegate.write(srcBuf, position);
            }

            @Override public int write(byte[] buf, int off, int len) throws IOException {
                parkIfNeeded();

                return delegate.write(buf, off, len);
            }

            /**
             * Parks current checkpoint thread if slow mode is enabled.
             */
            private void parkIfNeeded() {
                if (Thread.currentThread().getName().contains("checkpoint"))
                    LockSupport.parkNanos(checkpointParkNanos);
            }
        };
    }
}
