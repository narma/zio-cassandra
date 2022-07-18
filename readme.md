# zio-cassandra

[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots]

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/st/alzo/zio-cassandra_2.13/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/st/alzo/zio-cassandra_2.13/ "Sonatype Snapshots"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/s01.oss.sonatype.org/st.alzo/zio-cassandra_2.13.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/st.alzo/zio-cassandra_2.13.svg "Sonatype Snapshots"

This is lightweight ZIO wrapper for latest datastax 4.x driver.

Inspired by [akka/alpakka-cassandra](https://doc.akka.io/docs/alpakka/current/cassandra.html)
CQL ported from [ringcentral/cassandra4io](https://github.com/ringcentral/cassandra4io)


## Usage

#### Dependency:
```scala
libraryDependencies += "st.alzo" %% "zio-cassandra" % zioCassandra
```

### Create a connection to Cassandra
```scala
import zio.cassandra.session.Session

import com.datastax.oss.driver.api.core.CqlSession

import java.net.InetSocketAddress

val builder = CqlSession
      .builder()
      .addContactPoint(InetSocketAddress.createUnresolved("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .withKeyspace("awesome") 

val session = Session.make(builder)
```

## Work with CQL interpolator

Gently ported from [cassadnra4io](https://github.com/ringcentral/cassandra4io) cql
package `zio.cassandra.session.cql` introduces typed way to deal with cql queries:

### Simple syntax

This syntax reuse implicit driver prepared statements cache

```scala
import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.cassandra.session.Session
import zio.cassandra.session.cql._
import zio._

case class Model(id: Int, data: String)

trait Service {
  def put(value: Model): Task[Unit]
  def get(id: Int): Task[Option[Model]]
}

class ServiceImpl(session: Session) extends Service {

  private def insertQuery(value: Model) =
    cql"insert into table (id, data) values (${value.id}, ${value.data})"
      .config(_.setConsistencyLevel(ConsistencyLevel.ALL))

  private def selectQuery(id: Int) =
    cql"select id, data from table where id = $id".as[Model]
  
  override def put(value: Model) = insertQuery(value).execute.unit.provide(session)
  override def get(id: Int) = selectQuery(id).selectFirst.provideSome(session)
}
```

### Templated syntax

When you want control your prepared statements manually.

```scala
import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.cassandra.session.Session
import zio.cassandra.session.cql._
import zio.stream._
import zio._

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

  def apply(session: Session) = for {
    insert <- insertQuery.prepare
    select <- selectQuery.prepare
    selectAll <- selectAllQuery.prepare
  } yield new Service {
    override def put(value: Model) = insert(value.id, value.data).execute.unit.provide(session)

    override def get(id: Int) = select(id).config(_.setExecutionProfileName("default")).selectFirst

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
import zio._
import zio.cassandra.session.Session
import zio.cassandra.session.cql._
import zio.stream._


final case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])
final case class PersonAttribute(personId: Int, info: BasicInfo)


class UDTUsageExample {
  val data = PersonAttribute(1, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
  val insert: RIO[Session, Boolean] =
    cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
            .execute

  val retrieve: ZStream[Session, Throwable, PersonAttribute] = 
    cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
            .as[PersonAttribute]
            .select
}
```

Please note that we recommend using `FromUdtValue` and `ToUdtValue` to automatically derive this hand-written (and error-prone)
code.


### Raw API without cql
```scala
  import zio._
  import zio.cassandra.session.Session

// Use:
  val job = for {
    session  <- ZIO.service[Session]
    _        <- session.execute("insert ...")
    prepared <- session.prepare("select ...")
    select   <- ZIO.attempt(prepared.bind(1, 2))
    row      <- session.selectFirst(select)
  } yield row
  
  job.provideCustomLayer(Session.live)

```


## References
- [Datastax Java driver](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/)