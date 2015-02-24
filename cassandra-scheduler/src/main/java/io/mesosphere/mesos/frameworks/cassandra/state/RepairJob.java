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

import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.apache.mesos.Protos;

import java.util.Set;

public final class RepairJob extends ClusterKeyspaceJob {

    private static final long REPAIR_STATUS_INVERVAL = 10000L;
    private static final String REPAIR_SUFFIX = ".repair";

    RepairJob(CassandraCluster cassandraCluster, Set<Protos.ExecutorID> restriction) {
        super(cassandraCluster, restriction, REPAIR_STATUS_INVERVAL, CassandraTaskProtos.KeyspaceJobType.REPAIR, REPAIR_SUFFIX);
    }

    @Override
    protected void nodeFinished(ExecutorMetadata c) {
        c.repairDone(cassandraCluster.clock.now().getMillis());
    }
}
