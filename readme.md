
#### zio-cassandra

```text
scala = 2.13.2
cassandra-driver = 4.6.1
```

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)


#### Usage

Check driver config documentation on [datastax](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/)

```scala
# Layer:
  val sessionLayer = Session.create(config)

# In case of runtime node discovery:
  val cassandraRuntimeConfig = CassandraConnectionConfig(...)
  val sessionLayer = Session.create(cassandraRuntimeConfig)

# Access:
  ZIO.access[Session](...)

```
