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
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.Protos;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
public abstract class AbstractBench {

    protected State state;
    protected PersistedCassandraClusterState clusterState;
    protected PersistedCassandraClusterHealthCheckHistory healthCheckHistory;
    protected PersistedCassandraClusterJobs jobsState;
    protected PersistedCassandraFrameworkConfiguration configuration;
    protected CassandraCluster cluster;

    @org.openjdk.jmh.annotations.State(Scope.Benchmark)
    public static class Counter {
        int counter;
    }

    @Param({"5", "50", "100", "500", "1000"})
    protected int numberOfNodes;

    @Param({"-", "zk://localhost:2181/cassandra-jmh"})
    protected String zkUrl;

    @Language("RegExp")
    private static final String userAndPass     = "[^/@]+";
    @Language("RegExp")
    private static final String hostAndPort     = "[A-z0-9-.]+(?::\\d+)?";
    @Language("RegExp")
    private static final String zkNode          = "[^/]+";
    @Language("RegExp")
    private static final String REGEX = "^zk://((?:" + userAndPass + "@)?(?:" + hostAndPort + "(?:," + hostAndPort + ")*))(/" + zkNode + "(?:/" + zkNode + ")*)$";
    private static final Pattern zkURLPattern = Pattern.compile(REGEX);

    @TearDown
    public void cleanup() {
        cluster = null;
        configuration = null;
        jobsState = null;
        healthCheckHistory = null;
        clusterState = null;

        if (state != null) {
            try {
                System.err.println();
                for (Iterator<String> nameIter = state.names().get(); nameIter.hasNext(); ) {
                    String name = nameIter.next();
                    Variable var = state.fetch(name).get();
                    byte[] value = var.value();
                    state.expunge(var).get();
                    System.err.printf("size-of %-50s = %d%n", name, value.length);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        state = null;
    }

    @Setup
    public void setup() {
        state = buildState(zkUrl);
        clusterState = new PersistedCassandraClusterState(state, numberOfNodes, 3);
        healthCheckHistory = new PersistedCassandraClusterHealthCheckHistory(state);
        jobsState = new PersistedCassandraClusterJobs(state);
        configuration = new PersistedCassandraFrameworkConfiguration(state,
            "my-super-cluster",
            10L, 10L,
            "3.4.5",
            2d, 131072L, 32768L, 8192L,
            numberOfNodes, 3, "THE_ROLE",
            ".");
        cluster = new CassandraCluster(new SystemClock(),
            "http://127.99.99.99/",
            new ExecutorCounter(state, 0L),
            clusterState,
            healthCheckHistory,
            jobsState,
            configuration);

        List<CassandraFrameworkProtos.CassandraNode> nodes = new ArrayList<>();
        List<CassandraFrameworkProtos.ExecutorMetadata> executorMetadata = new ArrayList<>();

        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(CassandraFrameworkProtos.CassandraNode.newBuilder()
                .setHostname("host-" + i)
                .setIp("1.1.1." + i)
                .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder()
                    .setIp("1.1.1." + i)
                    .setJmxPort(7199))
                .setSeed(false)
                .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
                .addDataVolumes(CassandraFrameworkProtos.DataVolume.newBuilder()
                    .setPath("/where/is/my/data")
                    .setSizeMb(131072))
                .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                    .setExecutorId(executorId(i))
                    .setResources(resources())
                    .setSource("source")
                    .setTaskEnv(CassandraFrameworkProtos.TaskEnv.newBuilder()
                        .addAllVariables(Arrays.asList(
                            CassandraFrameworkProtos.TaskEnv.Entry.newBuilder()
                                .setName("some_name_1")
                                .setValue("some_value_1")
                                .build(),
                            CassandraFrameworkProtos.TaskEnv.Entry.newBuilder()
                                .setName("some_name_2")
                                .setValue("some_value_2")
                                .build()
                        )))
                    .addAllCommand(Arrays.asList(
                        "where-is-my-java-executable",
                        "-with",
                        "-the",
                        "-correct",
                        "-command",
                        "-line",
                        "-arguments"))
                    .addDownload(CassandraFrameworkProtos.FileDownload.newBuilder()
                        .setDownloadUrl("eoifjewo fjioewjf ioeiof jewoif jioej foiwj oifjewoifj eoiwef")
                        .setExecutable(false)
                        .setExtract(true))
                    .addDownload(CassandraFrameworkProtos.FileDownload.newBuilder()
                        .setDownloadUrl("foo bar baz foo bar baz foo bar baz foo bar baz foo bar baz foo bar baz foo bar baz")
                        .setExecutable(false)
                        .setExtract(true)))
                .addAllTasks(Arrays.asList(
                    CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                        .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.METADATA)
                        .setTaskId(taskId(i, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.METADATA))
                        .setResources(resources())
                        .setTaskDetails(CassandraFrameworkProtos.TaskDetails.newBuilder()
                            .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA)
                            .setExecutorMetadataTask(CassandraFrameworkProtos.ExecutorMetadataTask.newBuilder()
                                .setExecutorId(executorId(i))
                                .setIp("127.99.99.99")))
                        .build(),
                    CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                        .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER)
                        .setTaskId(taskId(i, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER))
                        .setResources(resources())
                        .setTaskDetails(CassandraFrameworkProtos.TaskDetails.newBuilder()
                            .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN)
                            .setCassandraServerRunTask(CassandraFrameworkProtos.CassandraServerRunTask.newBuilder()
                                .setVersion("3.4.5")
                                .setCassandraServerConfig(CassandraFrameworkProtos.CassandraServerConfig.newBuilder()
                                        .setCassandraYamlConfig(CassandraFrameworkProtos.TaskConfig.newBuilder()
                                        )
                                        .setTaskEnv(CassandraFrameworkProtos.TaskEnv.newBuilder()
                                        )
                                )
                                .setJmx(CassandraFrameworkProtos.JmxConnect.newBuilder()
                                    .setIp("1.1.1." + i)
                                    .setJmxPort(7199))
                                .addAllCommand(Arrays.asList(
                                    "where-is-my-java-executable",
                                    "-with",
                                    "-the",
                                    "-correct",
                                    "-command",
                                    "-line",
                                    "-arguments")))
                            .setExecutorMetadataTask(CassandraFrameworkProtos.ExecutorMetadataTask.newBuilder()
                                .setExecutorId(executorId(i))
                                .setIp("127.99.99.99")))
                        .build()
                ))
                .build());

            executorMetadata.add(CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setIp("127.99.99.99")
                .setExecutorId(executorId(i))
                .setWorkdir("/tmp/foo/bar/baz/foo/bar/baz/foo/bar/baz/foo/bar/baz/foo/bar/baz/" + i)
                .build());
        }
        clusterState.nodes(nodes);
        clusterState.executorMetadata(executorMetadata);
    }

    protected static State buildState(String zkUrl) {
        if (zkUrl == null || zkUrl.isEmpty() || "-".equals(zkUrl)) {
            return new InMemoryState();
        }
        Matcher matcher = zkURLPattern.matcher(zkUrl);
        if (!matcher.matches()) {
            throw new RuntimeException(String.format("Invalid zk url format: '%s'", zkUrl));
        }
        return new ZooKeeperState(
            matcher.group(1),
            10000,
            TimeUnit.MILLISECONDS,
            matcher.group(2)
        );
    }

    @NotNull
    protected static CassandraFrameworkProtos.TaskResources.Builder resources() {
        return CassandraFrameworkProtos.TaskResources.newBuilder()
            .setCpuCores(1)
            .setMemMb(1024)
            .setDiskMb(1024)
            .addAllPorts(Arrays.asList(7001L, 7000L, 7199L, 9042L, 9160L));
    }

    @NotNull
    protected static String executorId(int i) {
        return "cassandra.executor." + i;
    }

    @NotNull
    protected static String taskId(int i, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType type) {
        return "cassandra.executor." + i + '.' + type.name();
    }

    protected Protos.Offer createOffer(Protos.FrameworkID frameworkId, String hostname, Protos.OfferID offerId, Protos.SlaveID slaveId, Protos.ExecutorID executorId) {
        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
            .setFrameworkId(frameworkId)
            .setHostname(hostname)
            .addExecutorIds(executorId)
            .setId(offerId)
            .setSlaveId(slaveId);

        builder.addResources(Protos.Resource.newBuilder()
            .setName("cpus")
            .setRole("*")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)));
        builder.addResources(Protos.Resource.newBuilder()
            .setName("mem")
            .setRole("*")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
            .setName("disk")
            .setRole("*")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("ports")
                .setRole("*")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder()
                    .addRange(Protos.Value.Range.newBuilder().setBegin(7000).setEnd(10000)))
        );

        return builder.build();
    }
}
