# zio-cassandra

[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots]

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/st/alzo/zio-cassandra_2.13/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/st/alzo/zio-cassandra_2.13/ "Sonatype Snapshots"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/st.alzo/zio-cassandra_2.13.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/st.alzo/zio-cassandra_2.13.svg "Sonatype Snapshots"

This is lightweight ZIO wrapper for latest datastax 4.x driver.

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)
CQL ported from [ringcentral/cassandra4io](https://github.com/ringcentral/cassandra4io)


## Usage

#### Dependency:
For ZIO 1.x
```scala
libraryDependencies += "st.alzo" %% "zio-cassandra" % "1.2.0"
```

### Create a connection to Cassandra
```scala
import zio.cassandra.CassandraSession

import com.datastax.oss.driver.api.core.CqlSession

import java.net.InetSocketAddress

val builder = CqlSession
      .builder()
      .addContactPoint(InetSocketAddress.createUnresolved("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .withKeyspace("awesome") 

val session = CassandraSession.make(builder)
```

## Work with CQL interpolator

Gently ported from [cassadnra4io](https://github.com/ringcentral/cassandra4io) cql
package `zio.cassandra.cql` introduces typed way to deal with cql queries:

### Simple syntax

This syntax reuse implicit driver prepared statements cache

```scala
import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.cassandra.CassandraSession
import zio.cassandra.cql._
import zio._

case class Model(id: Int, data: String)

trait Service {
  def put(value: Model): Task[Unit]
  def get(id: Int): Task[Option[Model]]
}

class ServiceImpl(session: CassandraSession) extends Service {

  private def insertQuery(value: Model) =
    cql"insert into table (id, data) values (${value.id}, ${value.data})"
      .config(_.setConsistencyLevel(ConsistencyLevel.ALL))

  private def selectQuery(id: Int) =
    cql"select id, data from table where id = $id".as[Model]
  
  override def put(value: Model) = insertQuery(value).execute(session).unit
  override def get(id: Int) = selectQuery(id).selectOne(session)
}
```

### Templated syntax

When you want control your prepared statements manually.

```scala
import zio.duration._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.cassandra.CassandraSession
import zio.cassandra.cql._
import zio.stream._
    
case class Model(id: Int, data: String)
  
trait Service {
  def put(value: Model): Task[Unit]
  def get(id: Int): Task[Option[Model]]
  def getAll(): Stream[Throwable, Model]
}
    
object Service {
  
  private val insertQuery = cqlt"insert into table (id, data) values (${Put[Int]}, ${Put[String]})"
    .config(_.setTimeout(1.second))
  private val selectQuery = cqlt"select id, data from table where id = ${Put[Int]}".as[Model]
  private val selectAllQuery = cqlt"select id, data from table".as[Model]

  def apply(session: CassandraSession) = for {
    insert <- insertQuery.prepare(session)
    select <- selectQuery.prepare(session)      
    selectAll <- selectAllQuery.prepare(session)
  } yield new Service {
    override def put(value: Model) = insert(value.id, value.data).execute.unit
    override def get(id: Int) = select(id).config(_.setExecutionProfileName("default")).selectOne
    override def getAll() = selectAll().config(_.setExecutionProfileName("default")).select
  } 
} 
```

## User Defined Type (UDT) support

zio.cassandra.cql provides support for Cassandra's User Defined Type (UDT) values.
For example, given the following Cassandra schema:

```cql
create type basic_info(
    weight double,
    height text,
    datapoints frozen<set<int>>
);

create table person_attributes(
    person_id int,
    info frozen<basic_info>,
    PRIMARY KEY (person_id)
);
```

**Note:** `frozen` means immutable

Here is how to insert and select data from the `person_attributes` table:

```scala
final case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])
object BasicInfo {
  implicit val cqlReads: Reads[BasicInfo]   = FromUdtValue.deriveReads[BasicInfo]
  implicit val cqlBinder: Binder[BasicInfo] = ToUdtValue.deriveBinder[BasicInfo]
}

final case class PersonAttribute(personId: Int, info: BasicInfo)
```

We provide a set of typeclasses (`FromUdtValue` and `ToUDtValue`) under the hood that automatically convert your Scala
types into types that Cassandra can understand without having to manually convert your data-types into Datastax Java
driver's `UdtValue`s.

```scala
import zio.stream.Stream

class UDTUsageExample(session: CassandraSession) {
  val data = PersonAttribute(1, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
  val insert: F[Boolean] =
    cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
            .execute(session)

  val retrieve: Stream[Throwable, PersonAttribute] = 
    cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
            .as[PersonAttribute]
            .select(session)
}
```

### More control over the transformation process of `UdtValue`s

If you wanted to have additional control into how you map data-types to and from Cassandra rather than using `FromUdtValue`
& `ToUdtValue`, we expose the Datastax Java driver API to you for full control. Here is an example using `BasicInfo`:

```scala
object BasicInfo {
  implicit val cqlReads: Reads[BasicInfo] = Reads[UdtValue].map { udtValue =>
    BasicInfo(
      weight = udtValue.getDouble("weight"),
      height = udtValue.getString("height"),
      datapoints = udtValue
        .getSet[java.lang.Integer]("datapoints", classOf[java.lang.Integer])
        .asScala
        .toSet
        .map { int: java.lang.Integer => Int.unbox(int) }
    )
  }

  implicit val cqlBinder: Binder[BasicInfo] = Binder[UdtValue].contramapUDT { (info, constructor) =>
    constructor
      .newValue()
      .setDouble("weight", info.weight)
      .setString("height", info.height)
      .setSet("datapoints", info.datapoints.map(Int.box).asJava, classOf[java.lang.Integer])
  }
}
```

Please note that we recommend using `FromUdtValue` and `ToUdtValue` to automatically derive this hand-written (and error-prone)
code.


### Raw API without cql
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


## References
- [Datastax Java driver](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/)