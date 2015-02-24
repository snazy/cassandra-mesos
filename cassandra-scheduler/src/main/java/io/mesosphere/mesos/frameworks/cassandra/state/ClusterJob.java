/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra.state;

import com.google.common.collect.Maps;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for all cluster-wide operations.
 */
public abstract class ClusterJob<S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterJob.class);

    protected final CassandraCluster cassandraCluster;

    private final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> remainingNodes;
    private final Map<String, S> processedNodes = Maps.newConcurrentMap();
    private final Set<Protos.ExecutorID> restriction;
    private volatile ExecutorMetadata currentNode;

    private final long startedTimestamp = System.currentTimeMillis();
    private Long finishedTimestamp;

    private volatile boolean abort;

    public ClusterJob(CassandraCluster cassandraCluster, Set<Protos.ExecutorID> restriction) {
        this.cassandraCluster = cassandraCluster;

        remainingNodes = Maps.newConcurrentMap();
        remainingNodes.putAll(cassandraCluster.executorMetadataMap);

        this.restriction = restriction;
    }

    public void started() {
        LOGGER.info("Created {}", this.getClass().getSimpleName());
    }

    public boolean hasRemainingNodes() {
        return !remainingNodes.isEmpty();
    }

    public Collection<ExecutorMetadata> allRemainingNodes() {
        return remainingNodes.values();
    }

    protected ExecutorMetadata remainingNodeForExecutor(Protos.ExecutorID executorID) {
        return remainingNodes.remove(executorID);
    }

    protected void shutdown() {
        LOGGER.info("Shutting down repair job");
        finishedTimestamp = System.currentTimeMillis();
        cassandraCluster.clusterJobFinished(this);
    }

    public void abort() {
        abort = true;
    }

    public boolean isAborted() {
        return abort;
    }

    public long getStartedTimestamp() {
        return startedTimestamp;
    }

    public Long getFinishedTimestamp() {
        return finishedTimestamp;
    }

    protected boolean canStartOnExecutor(Protos.ExecutorID executorID) {
        if (currentNode == null) {
            if (abort || !hasRemainingNodes()) {
                shutdown();
                return false;
            }

            return !(restriction != null && !restriction.contains(executorID));
        }
        return false;
    }

    protected void markNodeProcessed(ExecutorMetadata c, S status) {
        processedNodes.put(c.getSlaveMetadata().getIp(), status);
    }

    protected void setCurrentNode(ExecutorMetadata currentNode) {
        this.currentNode = currentNode;
    }

    public ExecutorMetadata getCurrentNode() {
        return currentNode;
    }

    protected boolean checkNodeStatus(CassandraTaskProtos.CassandraNodeHealthCheckDetails hc) {
        return hc.getHealthy();
    }

    public Map<String, S> getProcessedNodes() {
        return processedNodes;
    }

    public String getCurrentNodeIp() {
        return currentNode != null ? currentNode.getSlaveMetadata().getIp() : null;
    }

    public List<String> getRemainingNodeIps() {
        List<String> ips = new ArrayList<>();
        for (ExecutorMetadata executorMetadata : allRemainingNodes()) {
            String ip = executorMetadata.getSlaveMetadata().getIp();
            if (ip != null)
                ips.add(ip);
        }
        return ips;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterJob that = (ClusterJob) o;

        if (restriction != null ? !restriction.equals(that.restriction) : that.restriction != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return restriction != null ? restriction.hashCode() : 0;
    }

    public abstract boolean schedule(Marker marker, SchedulerDriver driver, Protos.Offer offer, ExecutorMetadata executorMetadata);
}
