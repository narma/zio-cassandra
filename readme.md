# zio-cassandra

[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots]

![Scala 3, 2.13](https://img.shields.io/badge/Scala-3%2C%202.13-green)

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/st/alzo/zio-cassandra_3/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://s01.oss.sonatype.org/content/repositories/snapshots/st/alzo/zio-cassandra_3/ "Sonatype Snapshots"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/s01.oss.sonatype.org/st.alzo/zio-cassandra_3.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/st.alzo/zio-cassandra_3.svg "Sonatype Snapshots"

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

  override def put(value: Model) = insertQuery(value).execute.unit.provide(ZLayer.succeed(session))
  override def get(id: Int) = selectQuery(id).selectFirst.provideSome(ZLayer.succeed(session))

  // alternatively, to avoid providing environment each time
  def insert(value: Model) = session.execute(insertQuery(value)).unit
  def select(id: Int) = session.selectFirst(selectQuery(id))

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

### More control over the transformation process of `UdtValue`s

If you wanted to have additional control into how you map data-types to and from Cassandra rather than using `UdtReads`
& `UdtWrites`, we expose the Datastax Java driver API to you for full control. Here is an example using `BasicInfo`:

```scala
import com.datastax.oss.driver.api.core.data.UdtValue
import zio.cassandra.session.cql.Binder
import zio.cassandra.session.cql.codec.{ UdtReads, UdtWrites }

import scala.jdk.CollectionConverters.{ SetHasAsJava, SetHasAsScala }

final case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])

object BasicInfo {

  implicit val basicInfoUdtReads: UdtReads[BasicInfo] = UdtReads.instance { udtValue =>
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

  implicit val basicInfoUdtWrites: UdtWrites[BasicInfo] = UdtWrites.instance { (info, constructor) =>
    constructor
      .setDouble("weight", info.weight)
      .setString("height", info.height)
      .setSet("datapoints", info.datapoints.map(Int.box).asJava, classOf[java.lang.Integer])
  }

  // just an example, will be derived automatically from UdtWrites[BasicInfo]
  implicit val cqlBinder: Binder[BasicInfo] = Binder[UdtValue].contramapUDT { (info, constructor) =>
    constructor
      .newValue()
      .setDouble("weight", info.weight)
      .setString("height", info.height)
      .setSet("datapoints", info.datapoints.map(Int.box).asJava, classOf[java.lang.Integer])
  }

}

final case class CompactedInfo(weight: Double, height: String)

object CompactedInfo {

  implicit val compactedInfoUdtReads: UdtReads[CompactedInfo] =
    UdtReads[BasicInfo].map(basicInfo => CompactedInfo(basicInfo.weight, basicInfo.height))

  implicit val compactedInfoUdtWrites: UdtWrites[CompactedInfo] =
    UdtWrites[BasicInfo].contramap(compactedInfo => BasicInfo(compactedInfo.weight, compactedInfo.height, Set.empty))

}
```


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

  job.provideLayer(ZLayer.scoped(Session.live))

```


## References
- [Datastax Java driver](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/)
