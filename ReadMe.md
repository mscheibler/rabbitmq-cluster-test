# Demonstration for RabbitMQ client cluster reconnect
This project ist just to demonstrate the difference and limits of the RabbitMQ client
automatic connection and topology recovery.

Consider the following setup:

    +----------------------------------------------+
    |Rabbit cluster                                |
    |                                              |
    |   +--------+                    +--------+   |
    |   | Node 1 +------------------->| Node 3 |   |
    |   | with   |                    |        |   |
    |   | Queue  |<-------------------+        |   |
    |   +-----+--+                    +-----+--+   |
    |    ^ ^  |                          ^  |      |
    |    | |  |                          |  |      |
    |    | |  |        +--------+        |  |      |
    |    | |  +------->| Node 2 +--------+  |      |
    |    | |           |        |           |      |
    |    | +-----------+        |<----------+      |
    |    |             +--------+                  |
    |    |                ^                        |
    |    |                |                        |
    +----+----------------+------------------------+
         |                |
         |                |
    +----+----+      +----+----+
    | Client  |      | Client  |
    |   1     |      |   2     |
    +---------+      +---------+

The exchange and queue in this example are durable, so they will survive a restart
of their node.

The clients in this test connect to specific nodes of the cluster. In most production use
cases a load balancer distributes the client connections among the nodes. So the clients
do not (need to) know to which node of a cluster they are connected to.

## Reconnect behavior
Since version 4 of the Java client an automatic recovery of connection and topology is
available and active by default.

### Connection recovery
Is triggered by [connection problems][1] between the client and the server.

It is NOT triggered when the client closes the connection or by channel-level problems
within the connection.

If the recovery fails, the client will retry it after a certain interval until the connection
could be re-established.

### Topology recovery
Is always **triggered as part of a connection recovery**. It will **not** occur on its own.

>Topology recovery includes the following actions, performed for every channel
>- Re-declare exchanges (except for predefined ones)
>- Re-declare queues
>- Recover all bindings
>- Recover all consumers
>
> Topology recovery relies on a per-connection cache of entities (queues, exchanges, bindings, consumers). 

Closed channels within a connection will not recover.

### Recovery from a restart of the client connected node
When a node becomes restarted all connected clients will lose their connections and thus
will begin the automatic recovery procedures. This includes the topology recovery.

In this case the RabbitMQ Java client will also recover all consumers.
- `Client 1` will start automatic recovery if `Node 1` is restarted
- `Client 2` will start automatic recovery if `Node 2` is restarted

Consumers of this client will receive a `shutdown` and a `recover-ok` signal on begin of the
connection loss and after the recovery. However, the **subscription will not be canceled**.

### Recovery from a restart of another cluster node
When a cluster node restarts, other than the one the client is connected to, the client may
not even notice the restart. The client will not lose the connection and thus no automatic
recovery becomes triggered.

Whether a client suffers any effect from the node restart depends on the subscriptions to
queues that live on the restarted node. In case of the picture above:
- `Client 1` will not notice anything when `Node 2` becomes restarted
- `Client 2` will receive cancel signals for consumers of the durable queue when `Node 1`
  becomes restarted

See [RabbitMQ HA documentation][2]
>If leader node of a queue becomes unavailable, the behaviour of a non-mirrored queue
>depends on its durability. A durable queue will become unavailable until the node comes back.

If a client has a subscription to such a queue, it will receive a `cancel` signal and no
recovery from the RabbitMQ client will become triggered.

So it falls into the responsibility of the client application to handle this and re-subscribe
to such a queue when it becomes available again.

[1]: https://www.rabbitmq.com/api-guide.html#connection-recovery
[2]: https://www.rabbitmq.com/ha.html#non-mirrored-queue-behavior-on-node-failure
