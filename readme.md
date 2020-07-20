
#### zio-cassandra

```text
scala = 2.13.2
cassandra-driver = 4.7.2
zio = 1.0.0-RC21-2
```

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)


#### Usage

Check driver config documentation on [datastax](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/)

```scala
// Cassandra Session:
  val session = CassandraSession.make(config)
//OR
  val session = CassandraSession.make(cqlSessionBuilder)

// Use:
  val job = for {
    session  <- ZIO.service[CassandraSession]
    _        <- session.execute("insert ...")
    prepared <- session.prepare("select ...")
    select   <- session.bind(prepared, Seq(args))
    row      <- session.selectOne(select, profileName = "oltp")
  } yield row
  
  job.provideCustomLayer(CassandraSession.make(config).toLayer)

```
