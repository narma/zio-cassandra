[![Maven][mvnImg]][mvnLink]

[mvnImg]: https://img.shields.io/maven-central/v/io.github.jsfwa/zio-cassandra_2.13.svg
[mvnLink]: https://mvnrepository.com/artifact/io.github.jsfwa/zio-cassandra

#### zio-cassandra

```text
scala = 2.13.3
cassandra-driver = 4.8.0
zio = 1.0.1
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
