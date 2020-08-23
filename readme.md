[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots]

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/io/github/jsfwa/zio-cassandra_2.13/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/io/github/jsfwa/zio-cassandra_2.13/ "Sonatype Snapshots"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/io.github.jsfwa/zio-cassandra_2.13.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/io.github.jsfwa/zio-cassandra_2.13.svg "Sonatype Snapshots"

#### zio-cassandra

```text
scala = 2.13.3
cassandra-driver = 4.8.0
zio = 1.0.1
```

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)


#### Usage

Dependency:
```scala
libraryDependencies += "io.github.jsfwa" %% "zio-cassandra" % "1.0.0"
```

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
