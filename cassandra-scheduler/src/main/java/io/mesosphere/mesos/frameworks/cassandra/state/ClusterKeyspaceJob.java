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

import com.google.protobuf.ByteString;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Collections;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public abstract class ClusterKeyspaceJob extends ClusterJob<CassandraTaskProtos.KeyspaceJobStatus> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterJob.class);

    private final CassandraTaskProtos.KeyspaceJobType keyspaceJobType;
    private final String taskSuffix;
    private final long statusInterval;
    private long nextStatus;

    public ClusterKeyspaceJob(CassandraCluster cassandraCluster, Set<Protos.ExecutorID> restriction, long statusInterval, CassandraTaskProtos.KeyspaceJobType keyspaceJobType, String taskSuffix) {
        super(cassandraCluster, restriction);
        this.statusInterval = statusInterval;
        this.keyspaceJobType = keyspaceJobType;
        this.taskSuffix = taskSuffix;
    }

    @Override
    protected boolean checkNodeStatus(CassandraTaskProtos.CassandraNodeHealthCheckDetails hc) {
        return super.checkNodeStatus(hc) && "NORMAL".equals(hc.getInfo().getOperationMode());
    }

    public String getTaskSuffix() {
        return taskSuffix;
    }

    public CassandraTaskProtos.KeyspaceJobType getKeyspaceJobType() {
        return keyspaceJobType;
    }

    public boolean needsStart(Protos.ExecutorID executorID) {
        if (!super.canStartOnExecutor(executorID))
            return false;

        ExecutorMetadata candidate = remainingNodeForExecutor(executorID);
        if (candidate == null)
            return false;

        CassandraTaskProtos.CassandraNodeHealthCheckDetails hc = candidate.getLastHealthCheckDetails();
        if (!checkNodeStatus(hc)) {
            LOGGER.info("skipping {} for {} (executor {})",
                    candidate.getSlaveMetadata(), getClass().getSimpleName(),
                    candidate.getExecutorId().getValue());
            return false;
        }

        LOGGER.info("moving current {} target to {} (executor {})", getClass().getSimpleName(),
                candidate.getSlaveMetadata(), candidate.getExecutorId().getValue());
        setCurrentNode(candidate);
        scheduleNextStatus();

        return true;
    }

    protected void scheduleNextStatus() {
        nextStatus = System.currentTimeMillis() + statusInterval;
    }

    public void gotStatusFromExecutor(Protos.ExecutorID executorId, CassandraTaskProtos.KeyspaceJobStatus status) {
        LOGGER.debug("gotRepairStatus for executor {}: {}", executorId.getValue(), protoToString(status));
        ExecutorMetadata c = getCurrentNode();
        if (c != null && c.getExecutorId().equals(executorId)) {
            boolean finished = !status.getRunning();
            LOGGER.debug("gotRepairStatus for current repair target {} (executor {}) - running:{}",
                    c.getSlaveMetadata(), c.getExecutorId().getValue(), !finished);
            if (finished) {
                nodeFinished(c);
                markNodeProcessed(c, status);
                setCurrentNode(null);
            }
        }
    }

    protected abstract void nodeFinished(ExecutorMetadata c);

    public boolean needsStatus(Protos.ExecutorID executorID) {
        ExecutorMetadata c = getCurrentNode();
        if (c != null && c.getExecutorId().equals(executorID)) {
            if (nextStatus <= System.currentTimeMillis()) {
                scheduleNextStatus();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean schedule(Marker marker, SchedulerDriver driver, Protos.Offer offer, ExecutorMetadata executorMetadata) {
        if (needsStatus(executorMetadata.getExecutorId())) {
            submitKeyspaceJobStatus(marker, driver, offer, executorMetadata, getKeyspaceJobType(), getTaskSuffix() + "-status");
            return true;
        }
        else if (needsStart(executorMetadata.getExecutorId())) {
            submitKeyspaceJobStart(marker, driver, offer, executorMetadata, getKeyspaceJobType(), getTaskSuffix());
            return true;
        }
        return false;
    }

    private void submitKeyspaceJobStatus(Marker marker, SchedulerDriver driver, Protos.Offer offer, ExecutorMetadata executorMetadata,
                                         CassandraTaskProtos.KeyspaceJobType keyspaceJobType, String suffix) {
        Protos.TaskID taskId = cassandraCluster.createTaskId(executorMetadata, suffix);
        CassandraTaskProtos.TaskDetails taskDetails = CassandraTaskProtos.TaskDetails.newBuilder()
                .setTaskType(CassandraTaskProtos.TaskDetails.TaskType.CASSANDRA_NODE_KEYSPACE_JOB_STATUS)
                .setCassandraNodeKeyspaceJobStatusTask(CassandraTaskProtos.CassandraNodeKeyspaceJobStatusTask.newBuilder().setType(keyspaceJobType))
                .build();
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();
        LOGGER.debug(marker, "Launching CASSANDRA_NODE_KEYSPACE_JOB_STATUS task for {} : {}", keyspaceJobType, protoToString(task));
        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
    }

    private void submitKeyspaceJobStart(Marker marker, SchedulerDriver driver, Protos.Offer offer, ExecutorMetadata executorMetadata,
                                        CassandraTaskProtos.KeyspaceJobType keyspaceJobType, String suffix) {
        Protos.TaskID taskId = cassandraCluster.createTaskId(executorMetadata, suffix);
        CassandraTaskProtos.TaskDetails taskDetails = CassandraTaskProtos.TaskDetails.newBuilder()
                .setTaskType(CassandraTaskProtos.TaskDetails.TaskType.CASSANDRA_NODE_KEYSPACE_JOB)
                .setCassandraNodeKeyspaceJobTask(CassandraTaskProtos.CassandraNodeKeyspaceJobTask.newBuilder()
                        .setType(keyspaceJobType)
                        .setJmx(executorMetadata.getJmxConnect()))
                .build();
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();
        LOGGER.debug(marker, "Launching CASSANDRA_NODE_KEYSPACE_JOB task for {}: {}", keyspaceJobType, protoToString(task));
        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
    }
}
