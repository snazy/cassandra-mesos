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

import org.apache.mesos.Protos;

import java.util.Set;

/**
 * Upgrade C* software.
 */
public class UpgradeJob extends RestartJob {
    public UpgradeJob(CassandraCluster cassandraCluster, Set<Protos.ExecutorID> restriction) {
        super(cassandraCluster, restriction);
    }

    // TODO implement
}
