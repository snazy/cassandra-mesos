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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class SlaveMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorMetadata.class);

    private final Protos.SlaveID slaveID;
    private final String hostname;
    private final String ip;
    private boolean blacklisted;

    public SlaveMetadata(Protos.SlaveID slaveID, String hostname) {
        this.slaveID = slaveID;
        this.hostname = hostname;
        try {
            InetAddress iadr = InetAddress.getByName(hostname);
            this.ip = iadr.getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to resolve host name '" + hostname + "' to IP.", e);
            throw new RuntimeException("Failed to resolve host name '" + hostname + '\'', e);
        }
    }

    public boolean blacklisted() {
        if (blacklisted)
            return false;
        this.blacklisted = true;
        return true;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIp() {
        return ip;
    }

    public boolean isBlacklisted() {
        return blacklisted;
    }

    @Override
    public String toString() {
        return hostname + '/' + ip + '@' + slaveID.getValue();
    }
}
