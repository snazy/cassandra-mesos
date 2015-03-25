/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@State(Scope.Benchmark)
public class ClusterJobSizingBench extends AbstractBench {
    @Param({"5", "50"})
    int numberOfKeyspaces;

    @Benchmark
    public void clusterJobWork(Counter counter) {
        int c = counter.counter++;
        if (counter.counter == numberOfNodes) {
            counter.counter = 0;
        }

        if (c == 0) {
            if (cluster.getCurrentClusterJob() != null) {
                CassandraFrameworkProtos.ClusterJobStatus j = cluster.getCurrentClusterJob();
                System.err.println(j.toString());
                throw new RuntimeException("boo");
            }

            // start job
            cluster.startClusterTask(CassandraFrameworkProtos.ClusterJobType.REPAIR);
        }

        CassandraFrameworkProtos.NodeJobStatus.Builder nodeJobStatus =
            CassandraFrameworkProtos.NodeJobStatus.newBuilder()
                .setFailed(false)
                .setExecutorId(executorId(c))
                .setStartedTimestamp(System.currentTimeMillis())
                .setFinishedTimestamp(System.currentTimeMillis())
                .setJobType(CassandraFrameworkProtos.ClusterJobType.REPAIR)
                .setRunning(false)
                .setTaskId(taskId(c, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.CLUSTER_JOB));
        for (int k = 0; k < numberOfKeyspaces; k ++) {
            nodeJobStatus.addProcessedKeyspaces(CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                .setDuration(99999)
                .setKeyspace("some-keyspace-nr-" + k)
                .setStatus("JMH"));
        }

        cluster.getTasksForOffer(createOffer(Protos.FrameworkID.newBuilder().setValue("framework").build(),
                "host-" + c,
                Protos.OfferID.newBuilder().setValue("offer-" + c).build(),
                Protos.SlaveID.newBuilder().setValue(executorId(c)).build(),
                Protos.ExecutorID.newBuilder().setValue(executorId(c)).build()));

        cluster.onNodeJobStatus(CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                .build()
        );

        cluster.removeTask(taskId(c, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.CLUSTER_JOB),
            Protos.TaskStatus.newBuilder()
                .setState(Protos.TaskState.TASK_FINISHED)
                .setHealthy(true)
                .setMessage("foo bar")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(executorId(c)))
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(c, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.CLUSTER_JOB)))
                .build());

        cluster.getTasksForOffer(createOffer(Protos.FrameworkID.newBuilder().setValue("framework").build(),
            "host-" + c,
            Protos.OfferID.newBuilder().setValue("offer-" + c).build(),
            Protos.SlaveID.newBuilder().setValue(executorId(c)).build(),
            Protos.ExecutorID.newBuilder().setValue(executorId(c)).build()));

    }
}
