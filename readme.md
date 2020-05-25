
#### zio-cassandra

```text
scala = 2.13.2
cassandra-driver = 4.6.1
zio = 1.0.0-RC19-2
```

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)


#### Usage

Check driver config documentation on [datastax](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/)

```scala
// Layer:
  val sessionLayer = Session.live(config)
//OR
  val sessionLayer = Session.live(cqlSessionBuilder)
// Use:
  for {
    session <- ZIO.service[Session]
      ...
  } yield ...
  

```
