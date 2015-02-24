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

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import io.mesosphere.mesos.util.Clock;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.Protos;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.Functions.headOption;
import static io.mesosphere.mesos.util.Functions.unmodifiableHashMap;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

/**
 * STUB to abstract state management.
 */
public final class CassandraCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCluster.class);

    // see: http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
    private static final Map<String, Long> defaultCassandraPortMappings = unmodifiableHashMap(
            tuple2("storage_port", 7000L),
            tuple2("ssl_storage_port", 7001L),
            tuple2("jmx", 7199L),
            tuple2("native_transport_port", 9042L),
            tuple2("rpc_port", 9160L)
    );

    public static final int DEFAULT_SEED_NODE_COUNT = 3;

    final Clock clock = new SystemClock();

    final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> executorMetadataMap = Maps.newConcurrentMap();
    private final ConcurrentMap<Protos.SlaveID, SlaveMetadata> slaveMetadataMap = Maps.newConcurrentMap();
    private final Map<Protos.TaskID, Protos.ExecutorID> taskToExecutor = Maps.newConcurrentMap();

    private String name;
    private double cpuCores;
    private long memMb;
    private long diskMb;
    private int nodeCount;
    private int seedNodeCount;

    private long nextNodeAddTime;

    private boolean jmxSsl;
    private String jmxUsername;
    private String jmxPassword;

    private int jmxPort;
    private int storagePort;
    private int sslStoragePort;
    private int nativePort;
    private int rpcPort;

    private long healthCheckIntervalMillis;

    private long bootstrapGraceTimeMillis = TimeUnit.MINUTES.toMillis(2);

    private final Lock clusterJobLock = new ReentrantLock();
    private final Queue<ClusterJob> clusterJobQueue = new LinkedList<>();
    private ClusterJob currentClusterJob;

    private final Map<String, ClusterJob> lastCompletedJobs = Maps.newConcurrentMap();

    private String cassandraVersion;

    private static final AtomicReference<CassandraCluster> singleton = new AtomicReference<>();

    public CassandraCluster() {
        if (!singleton.compareAndSet(null, this))
            throw new RuntimeException("oops");
    }

    public static CassandraCluster singleton() {
        return singleton.get();
    }

    public void configure(String name, int nodeCount,
                          Duration healthCheckInterval,
                          Duration bootstrapGraceTime,
                          double cpuCores, long memMb, long diskMb, String cassandraVersion) {
        this.name = name;
        this.nodeCount = nodeCount;

        this.cpuCores = cpuCores;
        this.memMb = memMb;
        this.diskMb = diskMb;

        this.cassandraVersion = cassandraVersion;

        this.jmxPort = defaultCassandraPortMappings.get("jmx").intValue();
        this.nativePort = defaultCassandraPortMappings.get("native_transport_port").intValue();
        this.rpcPort = defaultCassandraPortMappings.get("rpc_port").intValue();
        this.storagePort = defaultCassandraPortMappings.get("storage_port").intValue();
        this.sslStoragePort = defaultCassandraPortMappings.get("ssl_storage_port").intValue();

        this.seedNodeCount = Math.min(DEFAULT_SEED_NODE_COUNT, nodeCount);

        this.healthCheckIntervalMillis = healthCheckInterval.getMillis();
        this.bootstrapGraceTimeMillis = bootstrapGraceTime.getMillis();
    }

    public boolean isJmxSsl() {
        return jmxSsl;
    }

    public void setJmxSsl(boolean jmxSsl) {
        this.jmxSsl = jmxSsl;
    }

    public String getJmxPassword() {
        return jmxPassword;
    }

    public void setJmxPassword(String jmxPassword) {
        this.jmxPassword = jmxPassword;
    }

    public String getJmxUsername() {
        return jmxUsername;
    }

    public void setJmxUsername(String jmxUsername) {
        this.jmxUsername = jmxUsername;
    }

    public String getCassandraVersion() {
        return cassandraVersion;
    }

    public String getName() {
        return name;
    }

    public int getNativePort() {
        return nativePort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    //

    public ExecutorMetadata allocateNewExecutor(Protos.SlaveID slaveId, String hostname) {
        SlaveMetadata slaveMetadata = slaveMetadataMap.get(slaveId);
        if (slaveMetadata.isBlacklisted())
            return null;

        for (ExecutorMetadata executorMetadata : executorMetadataMap.values())
            if (hostname.equals(executorMetadata.getSlaveMetadata().getHostname()))
                return null;

        slaveMetadata = new SlaveMetadata(slaveId, hostname);
        slaveMetadataMap.put(slaveId, slaveMetadata);

        Protos.ExecutorID executorId = executorId(name + ".node." + hostname + ".executor");

        ExecutorMetadata executorMetadata = new ExecutorMetadata(slaveMetadata, executorId);
        ExecutorMetadata existing = executorMetadataMap.putIfAbsent(executorId, executorMetadata);
        if (existing != null)
            assert false;
        else
            executorMetadata.updateJmxPort(jmxPort);

        LOGGER.debug("Allocated new executor {} on host {}/{}", executorId.getValue(), hostname, executorMetadata.getSlaveMetadata().getIp());

        return executorMetadata;
    }

    public boolean replaceNode(ExecutorMetadata executorMetadata) {
        return executorMetadata.getSlaveMetadata().blacklisted()
                && clusterJobStart(new ReplaceNodeJob(this, executorMetadata));
    }

    public ExecutorMetadata metadataForExecutor(Protos.ExecutorID executorId) {
        return executorId != null ? executorMetadataMap.get(executorId) : null;
    }

    public ExecutorMetadata metadataForTask(Protos.TaskID taskId) {
        return metadataForExecutor(taskToExecutor.get(taskId));
    }

    public ExecutorMetadata unassociateTaskId(Protos.TaskID taskId) {
        Protos.ExecutorID executorId = taskToExecutor.remove(taskId);
        LOGGER.debug("removing taskId {} from executor {}", taskId.getValue(), executorId != null ? executorId.getValue() : null);
        return metadataForExecutor(executorId);
    }

    public Protos.TaskID createTaskId(ExecutorMetadata executorMetadata, String suffix) {
        return associateTaskId(executorMetadata, executorMetadata.createTaskId(suffix));
    }

    public Protos.TaskID associateTaskId(ExecutorMetadata executorMetadata, Protos.TaskID taskId) {
        LOGGER.debug("associated taskId {} to executor {}", taskId.getValue(), executorMetadata.getExecutorId().getValue());
        taskToExecutor.put(taskId, executorMetadata.getExecutorId());
        return taskId;
    }

    public void executorLost(Protos.ExecutorID executorId) {
        LOGGER.debug("executor {} lost", executorId.getValue());

        for (Iterator<Map.Entry<Protos.TaskID, Protos.ExecutorID>> iter = taskToExecutor.entrySet().iterator();
             iter.hasNext(); ) {
            Map.Entry<Protos.TaskID, Protos.ExecutorID> entry = iter.next();
            if (entry.getValue().equals(executorId)) {
                LOGGER.debug("executor {} lost with task {}", executorId.getValue(), entry.getKey().getValue());
                iter.remove();
            }
        }

        ExecutorMetadata executorMetadata = executorMetadataMap.remove(executorId);
        if (executorMetadata != null)
            executorMetadata.executorLost();

        // TODO check repair + cleanup jobs
    }

    public void serverLost(ExecutorMetadata executorMetadata) {
        LOGGER.debug("Cassandra server {} lost", executorMetadata.getExecutorId().getValue());

        executorMetadata.serverLost();

        // TODO check repair + cleanup jobs
    }

    //

    public boolean hasRequiredSeedNodes() {
        int cnt = 0;
        for (ExecutorMetadata executorMetadata : executorMetadataMap.values())
            if (executorMetadata.isSeed())
                cnt++;
        return cnt >= seedNodeCount;
    }

    public boolean hasRequiredNodes() {
        return executorMetadataMap.size() >= nodeCount;
    }

    public boolean canAddNode() {
        return nextNodeAddTime < clock.now().getMillis();
    }

    public void nodeRunStateUpdate() {
        nextNodeAddTime = clock.now().getMillis() + bootstrapGraceTimeMillis;
    }

    public long getNextNodeLaunchTime() {
        return nextNodeAddTime;
    }

    public long getBootstrapGraceTimeMillis() {
        return bootstrapGraceTimeMillis;
    }

    public boolean shouldRunHealthCheck(Protos.ExecutorID executorID) {
        ExecutorMetadata executorMetadata = metadataForExecutor(executorID);

        return executorMetadata != null
                && clock.now().getMillis() > executorMetadata.getLastHealthCheck() + healthCheckIntervalMillis;

    }

    public long getHealthCheckIntervalMillis() {
        return healthCheckIntervalMillis;
    }

    public int getSeedNodeCount() {
        return seedNodeCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public Iterable<String> seedsIpList() {
        List<String> ips = new ArrayList<>();
        for (ExecutorMetadata executorMetadata : executorMetadataMap.values())
            if (executorMetadata.isSeed())
                ips.add(executorMetadata.getSlaveMetadata().getIp());
        return ips;
    }

    public List<ExecutorMetadata> allNodes() {
        return new ArrayList<>(executorMetadataMap.values());
    }

    public int updateNodeCount(int nodeCount) {
        if (nodeCount < seedNodeCount)
            return this.nodeCount;

        if (nodeCount < this.nodeCount)
            return this.nodeCount;

        this.nodeCount = nodeCount;
        return nodeCount;
    }

    public void nodeLaunched(ExecutorMetadata executorMetadata) {
        executorMetadata.setRunning();

        nodeRunStateUpdate();
    }

    // cluster jobs

    public boolean clusterJobStart(ClusterJob clusterJob) {
        if (clusterJob == null)
            return false;
        clusterJobLock.lock();
        try {
            if (currentClusterJob!=null) {
                for (ClusterJob job : clusterJobQueue) {
                    if (job.equals(clusterJob))
                        return false;
                }
                clusterJobQueue.add(clusterJob);
                return true;
            }

            currentClusterJob = clusterJob;
            clusterJob.started();
        } finally {
            clusterJobLock.unlock();
        }
        return true;
    }

    public List<ClusterJob> getQueuedJobs() {
        clusterJobLock.lock();
        try {
            return new ArrayList<>(clusterJobQueue);
        } finally {
            clusterJobLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public <J extends ClusterJob> J currentClusterJob() {
        return (J) currentClusterJob;
    }

    @SuppressWarnings("unchecked")
    public <J extends ClusterJob> J currentClusterJob(Class<J> type) {
        ClusterJob current = currentClusterJob;
        return current != null && type.isAssignableFrom(current.getClass())
                ? (J) current : null;
    }

    public <J extends ClusterJob> void clusterJobFinished(J job) {
        if (job != null) {
            clusterJobLock.lock();
            try {
                if (job == currentClusterJob)
                    currentClusterJob = null;

                if (currentClusterJob == null && !clusterJobQueue.isEmpty())
                    (currentClusterJob = clusterJobQueue.poll()).started();

                lastCompletedJobs.put(job.getClass().getSimpleName(), job);
            } finally {
                clusterJobLock.unlock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <J extends ClusterJob> J lastCompletedJob(Class<J> type) {
        return (J) lastCompletedJobs.get(type.getSimpleName());
    }

    public <J extends ClusterJob> boolean abortClusterJob(Class<J> type) {
        ClusterJob current = currentClusterJob(type);
        if (current == null)
            return false;
        current.abort();
        return true;
    }

    // repair + cleanup

    public boolean cleanupStart(Set<Protos.ExecutorID> restriction) {
        return clusterJobStart(new CleanupJob(this, restriction));
    }

    public boolean repairStart() {
        return clusterJobStart(new RepairJob(this, null));
    }

    // health check

    public void updateHealthCheck(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeHealthCheckDetails healthCheckDetails) {
        ExecutorMetadata executorMetadata = metadataForExecutor(executorId);
        if (executorMetadata != null)
            executorMetadata.updateHealthCheck(clock.now().getMillis(), healthCheckDetails);
    }

    //

    public double getCpuCores() {
        return cpuCores;
    }

    public long getDiskMb() {
        return diskMb;
    }

    public long getMemMb() {
        return memMb;
    }

    public Iterable<? extends Protos.Resource> resourcesForExecutor(ExecutorMetadata executorMetadata) {
        return newArrayList(
                cpu(cpuCores),
                mem(memMb),
                disk(diskMb),
                ports(portMapping(executorMetadata).values())
        );
    }

    public Map<String, Long> portMapping(ExecutorMetadata executorMetadata) {
        Map<String, Long> result = new HashMap<>(defaultCassandraPortMappings);
//        result.put("jmx", (long) executorMetadata.getJmxPort());
        return result;
    }

    public List<String> checkResources(Protos.Offer offer, ExecutorMetadata executorMetadata) {
        List<String> errors = newArrayList();

        ListMultimap<String, Protos.Resource> index = from(offer.getResourcesList()).index(resourceToName());

        Double availableCpus = resourceValueDouble(headOption(index.get("cpus"))).or(0.0);
        Long availableMem = resourceValueLong(headOption(index.get("mem"))).or(0L);
        Long availableDisk = resourceValueLong(headOption(index.get("disk"))).or(0L);
        if (availableCpus <= cpuCores) {
            errors.add(String.format("Not enough cpu resources. Required %f only %f available.", cpuCores, availableCpus));
        }
        if (availableMem <= memMb) {
            errors.add(String.format("Not enough mem resources. Required %d only %d available", memMb, availableMem));
        }
        if (availableDisk <= diskMb) {
            errors.add(String.format("Not enough disk resources. Required %d only %d available", diskMb, availableDisk));
        }

        TreeSet<Long> ports = resourceValueRange(headOption(index.get("ports")));
        for (Map.Entry<String, Long> entry : portMapping(executorMetadata).entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            if (!ports.contains(value))
                errors.add(String.format("Unavailable port %d(%s). %d other ports available.", value, key, ports.size()));
        }
        return errors;
    }
}
