/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.snapshot;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * @see IgniteSnapshot#statusSnapshot()
 */
@GridInternal
public class VisorSnapshotStatusTask extends VisorOneNodeTask<Void, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, String> job(Void arg) {
        return new VisorSnapshotStatusJob(debug);
    }

    /** */
    private static class VisorSnapshotStatusJob extends VisorJob<Void, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatusJob(boolean debug) {
            super(null, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(Void arg) throws IgniteException {
            Collection<Object> ids = ignite.context().cache().context().snapshotMgr().statusSnapshot().get().stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            if (ids.isEmpty() )
                return "No snapshot operations.";

            StringBuilder sb = new StringBuilder("Snapshot operation in progress on nodes with Consistent ID:")
                    .append(IgniteUtils.nl());

            ids.stream()
                    .map(String::valueOf)
                    .forEach(s -> sb.append(s).append(IgniteUtils.nl()));

            return sb.toString();


        }
    }
}
