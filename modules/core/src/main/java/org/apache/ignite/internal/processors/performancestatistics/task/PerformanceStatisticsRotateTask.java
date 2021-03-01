/*
 * Copyright 2021 JSC SberTech
 */

package org.apache.ignite.internal.processors.performancestatistics.task;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 *
 */
public class PerformanceStatisticsRotateTask extends VisorMultiNodeTask<Void, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected PerformanceStatisticsRotateJob job(Void arg) {
        return new PerformanceStatisticsRotateJob(arg, true);
    }

    /** {@inheritDoc} */
    @Override protected Void reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<Void> arg) {
        return ignite.context().discovery().allNodes().stream().map(ClusterNode::id)
            .collect(Collectors.toList());
    }

    /** */
    private static class PerformanceStatisticsRotateJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        protected PerformanceStatisticsRotateJob(@Nullable Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable Void arg) throws IgniteException {
            ignite.context().performanceStatistics().rotateWriter();
            return null;
        }
    }
}
