# Cassandra & Mesos -- Scalable Enterprise Storage


## Overview

In this post we'll show you how deploying Cassandra on Mesos can simplify your life running a Cassandra cluster. We will introduce you to Cassandra and Mesos fundamentals and conclude with running some queries and scaling up our Cassandra cluster.

We will be using [Elastic Mesos](https://elastic.mesosphere.io/) as an easy way to access a Mesos cluster, but the exercises will also work on a [local single node cluster](https://github.com/mesosphere/playa-mesos) (with the exception of the scaling excercise).

**Disclaimer:** Please be advised that Cassandra on Mesos is a fairly new port. It is reasonably well tested, but like any new technology you should put it through its paces and test it for yourself. If you have any feature requests or patches feel free to submit [an issue](https://github.com/mesosphere/cassandra-mesos/issues) or [pull request](https://github.com/mesosphere/cassandra-mesos/pulls). 
 
## Why Cassandra & Mesos

Cassandra is a highly scalable NoSQL database which is well suited to run on Mesos due to its peer-to-peer architecture. It posesses some great features to back today's web applications with its horizontal scalability, no single point of failure and a simple query language (CQL). 

Mesos is a cluster manager which provides a general runtime environment providing all the essentials to deploy, run and manage distributed applications. Its resource management and isolation helps getting the most out of your servers, freeing you from the "one app per server" paradigm.


## Howto

Deploying a Cassandra cluster typically involves downloading the Cassandra distribution, changing configuration settings like seed nodes and starting the servers up. This in itself doesn't sound too involved for a couple of servers, but gets pretty involved when you're running on dozens of servers or more. 

Of course this could be automated with a tool like Puppet or Chef, but by running Cassandra on Mesos all the deployment and configuration will be handled from a central location rendering deployment tools unnecessary.

Let's install Cassandra on Mesos so you can see for yourself:


1. Login to your Mesos master:   
```
ssh ubuntu@[my mesos master host]
```  

1. Download the Cassandra on Mesos distribution:   
```
wget http://downloads.mesosphere.io/cassandra/cassandra-mesos-2.0.5-1.tgz
```

1. Unpack the distribution and change into it:   
```
tar xvzf cassandra-mesos*tgz && cd cassandra-mesos*
``` 

1. Change the Mesos master URL and Zookeeper server list in `conf/mesos.yaml` to match your cluster:   
```
mesos.master.url: 'zk://localhost:2181/mesos'   
state.zk: 'localhost:2181'
```   
   
   **Note:** If you run a local cluster you can leave all defaults.

1. Change the number of hardware nodes you want Cassandra to be deployed:  
```
cassandra.noOfHwNodes: 1
```   
   
   **Note:** If you want to try out the scaling feature don't deploy it to all your Mesos nodes initially.

1. Start Cassandra on Mesos   
```
bin/cassandra-mesos
```
   
The scheduler (aka the `bin/cassandra-mesos` process) automatically deploys Cassandra to suitable Mesos nodes observing the set resource limits on CPU, memory and disk space. Each task will then request the node confguration from the scheduler before starting up. This ensures that all nodes are configured exactly the same.

The Mesos UI (http://[mesos master URL]:5050/) will show your running Cassandra tasks:

![Cassandra Tasks on Mesos](https://dl.dropboxusercontent.com/u/6682366/cassandra-mesos-tasks.png =700x)
 

## Running Queries
Cassandra supports a simplified SQL-like query language called CQL (Cassandra Query Language). It was built with modern web application workloads in mind and provides facilities to support many of the usual use cases through distributed counters and native collection data types.

Running CQL is done through the CQL shell `cqlsh`:

1. Go to the Mesos UI and copy down any host running Cassandra.   
   You can find it by clicking on the Cassandra framework. The hostname is displayed in the last column of the active task list.

1. Go to the mesos master shell and start the CQL shell:   
```
cd ~/cassandra-mesos* &&  bin/cqlsh [cassandra host]
```
  
1. Execute a CQL command:  
```
cqlsh> select * from system.schema_keyspaces ;

 keyspace_name | durable_writes | strategy_class                              | strategy_options
---------------+----------------+---------------------------------------------+----------------------------
        system |           True |  org.apache.cassandra.locator.LocalStrategy |                         {}
 system_traces |           True | org.apache.cassandra.locator.SimpleStrategy | {"replication_factor":"2"}

(2 rows)

cqlsh>
```
**Please note that the system keyspace is not to be messed with and was just chosen as a simple example.**

A much better way to learn CQL is to follow a good introduction like [this one](http://www.datastax.com/documentation/cql/3.0/webhelp/cql/ddl/ddl_intro_c.html).


## Scaling
Cassandra makes it very easy to scale your database in case your application becomes popular. Running Cassandra on Mesos makes it even easier as it takes care of all the operational aspects of scaling up your system:

1. Stop the Cassandra scheduler  
   If you are running it inside your terminal you can just `Ctrl-C` the process. Otherwise you can run `kill $(pgrep -f cassandra)` to stop the scheduler. 
   
   Please note that the Cassandra tasks will continue to run.

1. Scale up your Cassandra nodes by increasing the number of `cassandra.noOfHwNodes` in `conf/mesos.yaml`

   **Please stay within the number of machines you have otherwise Mesos will appear to be hanging while it waits for more resources to come online.**

1. Restart the Cassandra scheduler   
   The scheduler will automatically connect back with your existing Cassandra tasks and provision additional nodes to run Cassandra. If you loging to the Mesos UI you will see the new nodes. 
   
You can also use the Cassandra's `nodetool` to check that indeed all nodes have joined the same cluster: 

```
cd ~/cassandra-mesos* && bin/nodetool --host [cassandra host] status

Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)  Host ID                               Rack
UN  10.170.61.134  40.91 KB   256     67.1%             b77a7bec-bea1-4a60-bb04-c5c64bc3617e  rack1
UN  10.234.11.69   40.87 KB   256     66.2%             369234cd-9685-456b-b239-bb6b723db74d  rack1
UN  10.43.144.109  40.85 KB   256     66.8%             cd95a45a-607d-485c-b67c-27e0e5428c90  rack1
ubuntu@ec2-23-23-54-129:~/cassandra-mesos-2.0.5-1$
``` 

# Outlook
There are several features which we are thinking about that we haven't tackled yet:

1. Autoscaling  
Wouldn't it be awesome if Cassandra would automatically sping up more instances when your application gets featured on Hacker News? If you have a need for this we'd love to talk to you and find out the specifics.

1. Stats  
Cassandra has a nice JMX API to retrieve runtime statistics about each node, the workload and cluster. It would be nice to surface these automatically in Mesos.

1. Online Cassandra upgrades  
The current version of the Cassandra on Mesos scheduler requires a Cassandra shutdown to upgrade the Cassandra cluster (unless one is willing to copy the data into a new upgraded Cassandra cluster manually). A future version could automate the upgrade process.



# Conclusion
We have shown you how easy it is to run a distributed Cassandra cluster on Mesos. Mesos takes care of most of the annoying steps involved and largely elimiates the need for complicated deployment tools. It makes sacaing 

If you have any feedback or would like to contribute patches feel free to email <erich@mesosphere.io>. 



