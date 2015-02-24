package io.mesosphere.mesos.frameworks.cassandra.state;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Marker;

public class ReplaceNodeJob extends ClusterJob {
    private final ExecutorMetadata replaceTarget;

    public ReplaceNodeJob(CassandraCluster cassandraCluster, ExecutorMetadata replaceTarget) {
        super(cassandraCluster, null);
        this.replaceTarget = replaceTarget;
    }

    // If replace target is a seed node:
    // 1. promote another node as a seed
    // 2. rollout the configuration change (non-restart)
    // 3. replace target is no longer a seed node

    // Node replacement:
    // Start new node with "-Dcassandra.replace_address=address_of_dead_node"

    @Override
    public boolean schedule(Marker marker, SchedulerDriver driver, Protos.Offer offer, ExecutorMetadata executorMetadata) {
        throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    // TODO implement
    // We would only need to shutdown the C* node + executor and inform CassandraCluster that the next new node is a replacement node.
}
