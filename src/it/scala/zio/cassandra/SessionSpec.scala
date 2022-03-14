package zio.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, PreparedStatement}
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.dimafeng.testcontainers.CassandraContainer
import com.typesafe.config.ConfigFactory
import wvlet.log.{LogLevel, LogSupport, Logger}
import zio.blocking.Blocking
import zio.cassandra.session.Session
import zio.container.ZTestContainer
import zio.test.Assertion._
import zio.test._
import zio.{blocking => _, test => _, _}

import java.net.InetSocketAddress
import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsJava

object TestHelpers {
  final implicit class toJavaInt(private val i: Int) extends AnyVal {
    def asJava: Integer = i.asInstanceOf[java.lang.Integer]
  }
}

import zio.cassandra.TestHelpers._

object SessionSpec extends DefaultRunnableSpec with LogSupport with Fixtures {
  Logger.setDefaultLogLevel(LogLevel.INFO)

  override def spec =
    suite("Cassandra session")(
      testM("complete scenario with multiple queries")(
        for {
          table            <- ZIO.succeed("test_data")
          session          <- ZIO.service[Session]
          _                <- session.execute(tableQuery(table))
          insert           <- session.prepare(insertQuery(table))
          update           <- session.prepare(updateQuery(table))
          delete           <- session.prepare(deleteQuery(table))
          select           <- session.prepare(selectQuery(table))
          emptyResult      <- bind(select, Seq("user1")) >>= session.selectFirst
          preparedBatchSeq <-
            ZIO.foreach(0.until(10).toList)(i => bind(insert, Seq("user1", i.asJava, i.toString, Instant.now())))
          _                <- executeBatch(preparedBatchSeq)
          _                <- bind(insert, Seq("user1", 11.asJava, 21.toString, Instant.now())) >>= session.execute
          _                <- bind(insert, Seq("user1", 12.asJava, 22.toString, Instant.now()), "slow") >>= session.execute
          _                <- bind(update, Seq("nope", "user1", 2.asJava)) >>= session.execute
          _                <- bind(delete, Seq("user1", 1.asJava)) >>= session.execute
          selectAll        <- for {
                                stmt <- bind(select, Seq("user1"))
                                rows <- session.select(stmt).runCollect
                              } yield rows
        } yield assert(emptyResult)(isNone) &&
          assertTrue(selectAll.size == 11) &&
          assertTrue(
            selectAll
              .find(r => r.getInt("seq_nr") == 2)
              .map(_.getString("data"))
              .get == "nope"
          )
      ),
      testM("selectAll should be reference transparent")(
        for {
          session      <- ZIO.service[Session]
          select       <- session.prepare(selectQuery("prepared_data"))
          effect        = for {
                            b <- bind(select, Seq("user1"))
                            v <- session.select(b).runCollect.map(_.map(_.getString(0)))
                          } yield v
          resultOne    <- effect
          resultSecond <- effect
        } yield assertTrue(resultOne == resultSecond)
      ),
      testM("select should be reference transparent")(for {
        session      <- ZIO.service[Session]
        select       <- session.prepare(selectQuery("prepared_data"))
        statement    <- bind(select, Seq("user1"))
        stream        = session.select(statement).map(_.getString(0))
        resultOne    <- stream.runCollect
        resultSecond <- stream.runCollect
      } yield assertTrue(resultOne == resultSecond)),
      testM("select should emit chunks sized equal to statement pageSize")(for {
        session    <- ZIO.service[Session]
        select     <- session.prepare(selectQuery("prepared_data"))
        statement  <- bind(select, Seq("user1"))
        stream      = session.select(statement.setPageSize(2)).map(_.getString(0))
        chunkSizes <- stream.mapChunks(ch => Chunk.single(ch.size)).runCollect
      } yield assert(chunkSizes)(forall(equalTo(2))) && assertTrue(chunkSizes.size == 5)),
      testM("should return error on invalid query")(
        for {
          session <- ZIO.service[Session]
          select  <- session.prepare(s"select column_not_exists from $keyspace.prepared_data where user_id = 1").either
        } yield assert(select)(isLeft(hasMessage(equalTo("Undefined column name column_not_exists")))) &&
          assert(select)(isLeft(isSubtype[InvalidQueryException](Assertion.anything)))
      )
    ).provideCustomLayerShared(layer)
}

trait Fixtures {
  val keyspace = "test_keyspace"

  val keyspaceQuery =
    s"""CREATE KEYSPACE IF NOT EXISTS $keyspace
       |  WITH REPLICATION =
       |    {'class' : 'SimpleStrategy', 'replication_factor' : 1}""".stripMargin

  def tableQuery(table: String) =
    s"""CREATE TABLE IF NOT EXISTS $keyspace.$table(
       |user_id text,
       |seq_nr int,
       |data text,
       |created_at timestamp,
       |PRIMARY KEY (user_id, seq_nr))""".stripMargin

  def insertQuery(table: String) =
    s"""
       |INSERT INTO $keyspace.$table (user_id, seq_nr, data, created_at) values (?, ?, ?, ?)
       |""".stripMargin

  def updateQuery(table: String) =
    s"""
       |UPDATE $keyspace.$table SET data = ? WHERE user_id = ? and seq_nr = ?
       |""".stripMargin

  def deleteQuery(table: String) =
    s"""
       |DELETE FROM $keyspace.$table WHERE user_id = ? and seq_nr = ?
       |""".stripMargin

  def selectQuery(table: String) =
    s"""
       |SELECT user_id, seq_nr, data, created_at FROM $keyspace.$table WHERE user_id = ?
       |""".stripMargin

  val layaerCassandra = ZTestContainer.cassandra

  val layerSession = (for {
    cassandra <- ZTestContainer[CassandraContainer].toManaged_
    session   <- {
      val address = new InetSocketAddress(cassandra.containerIpAddress, cassandra.mappedPort(9042))
      val config  = ConfigFactory.load().getConfig("cassandra.test-driver")
      val builder = CqlSession
        .builder()
        .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
        .addContactPoints(Seq(address).asJavaCollection)

      Session.make(builder)
    }
    _         <- prepareTestSession(session).toManaged_
  } yield session).toLayer.mapError(TestFailure.die)

  val layer: ZLayer[Blocking, TestFailure[Nothing], Has[Session]] = layaerCassandra >>> layerSession

  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
    Task(stmt.bind(bindValues: _*))

  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement] =
    bind(stmt, bindValues).map(_.setExecutionProfileName(profileName))

  def prepareTestSession(session: Session): Task[Unit] =
    for {
      table            <- ZIO.succeed("prepared_data")
      _                <- session.execute(keyspaceQuery)
      _                <- session.execute(tableQuery(table))
      insert           <- session.prepare(insertQuery(table))
      preparedBatchSeq <- ZIO.foreach(0.until(10).toList) { i =>
                            bind(insert, Seq("user1", i.asJava, i.toString, Instant.now()))
                          }
      batch             = BatchStatement
                            .builder(DefaultBatchType.LOGGED)
                            .addStatements(preparedBatchSeq: _*)
                            .build()
      _                <- session.execute(batch)
    } yield ()

  def withSession[R](f: Session => Task[R]): ZIO[Has[Session], Throwable, R] = ZIO.accessM[Has[Session]] { session =>
    f(session.get)
  }

  def executeBatch(seq: Seq[BoundStatement]): RIO[Has[Session], Unit] = withSession { s =>
    val batch = BatchStatement
      .builder(DefaultBatchType.LOGGED)
      .addStatements(seq: _*)
      .build()
    s.execute(batch).unit
  }

}
