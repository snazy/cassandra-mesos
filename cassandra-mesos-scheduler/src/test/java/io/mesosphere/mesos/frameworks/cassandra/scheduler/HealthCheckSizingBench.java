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
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@State(Scope.Benchmark)
public class HealthCheckSizingBench extends AbstractBench {

    @Benchmark
    public void healthcheckWork(Counter counter) {
        int c = counter.counter++;
        if (counter.counter == numberOfNodes) {
            counter.counter = 0;
        }

        cluster.recordHealthCheck(executorId(c),
            CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
                .setHealthy(true)
                .setMsg("message text")
                .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                    .setNativeTransportRunning(true)
                    .setRpcServerRunning(true)
                    .setUptimeMillis(System.currentTimeMillis())
                    .setClusterName("super-cluster")
                    .setDataCenter("DC-MINE")
                    .setRack("RAC_SIDE")
                    .setEndpoint("127.99.99.99")
                    .setGossipInitialized(true)
                    .setGossipRunning(true)
                    .setHostId(executorId(c))
                    .setJoined(true)
                    .setOperationMode("NORMAL")
                    .setTokenCount(256 + c)
                    .setVersion("3.4.5")
                    .build())
                .build());
    }
}
