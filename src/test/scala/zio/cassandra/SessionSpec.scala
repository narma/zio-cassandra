package zio.cassandra

import java.net.InetSocketAddress
import java.time.Instant

import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BoundStatement, DefaultBatchType }
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.dimafeng.testcontainers.CassandraContainer
import com.typesafe.config.ConfigFactory
import wvlet.log.{ LogLevel, LogSupport, Logger }

import zio.cassandra.service.CassandraSession
import zio.container.ZTestContainer
import zio.test.Assertion._
import zio.test._
import zio.{ blocking => _, test => _, _ }

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
          table       <- ZIO.succeed("test_data")
          session     <- ZIO.service[service.CassandraSession]
          _           <- session.execute(tableQuery(table))
          insert      <- session.prepare(insertQuery(table))
          update      <- session.prepare(updateQuery(table))
          delete      <- session.prepare(deleteQuery(table))
          select      <- session.prepare(selectQuery(table))
          emptyResult <- session.bind(select, Seq("user1")) >>= session.selectOne
          preparedBatchSeq <- ZIO.collectAll(
                               0.until(10) map (i =>
                                 session.bind(insert, Seq("user1", i.asJava, i.toString, Instant.now()))
                               )
                             )
          _         <- executeBatch(preparedBatchSeq)
          _         <- session.bindAndExecute(insert, Seq("user1", 11.asJava, 21.toString, Instant.now()))
          _         <- session.bindAndExecute(insert, Seq("user1", 12.asJava, 22.toString, Instant.now()), "slow")
          _         <- session.bind(update, Seq("nope", "user1", 2.asJava)) >>= session.execute
          _         <- session.bind(delete, Seq("user1", 1.asJava)) >>= session.execute
          selectAll <- session.bind(select, Seq("user1")) >>= session.selectAll
        } yield {
          assert(emptyResult)(isNone) &&
          assert(selectAll.size)(equalTo(11)) &&
          assert(
            selectAll
              .find(r => r.getInt("seq_nr") == 2)
              .map(_.getString("data"))
          )(isSome(equalTo("nope")))
        }
      ),
      testM("selectAll should be reference transparent")(
        for {
          session      <- ZIO.service[service.CassandraSession]
          select       <- session.prepare(selectQuery("prepared_data"))
          effect       = (session.bind(select, Seq("user1")) >>= session.selectAll).map(_.map(_.getString(0)))
          resultOne    <- effect
          resultSecond <- effect
        } yield assert(resultOne)(equalTo(resultSecond))
      ),
      testM("select should be reference transparent")(for {
        session      <- ZIO.service[service.CassandraSession]
        select       <- session.prepare(selectQuery("prepared_data"))
        statement    <- session.bind(select, Seq("user1"))
        stream       = session.select(statement).map(_.getString(0))
        resultOne    <- stream.runCollect
        resultSecond <- stream.runCollect
      } yield assert(resultOne)(equalTo(resultSecond))),
      testM("select should emit chunks sized equal to statement pageSize")(for {
        session    <- ZIO.service[service.CassandraSession]
        select     <- session.prepare(selectQuery("prepared_data"))
        statement  <- session.bind(select, Seq("user1"))
        stream     = session.select(statement.setPageSize(2)).map(_.getString(0))
        chunkSizes <- stream.mapChunks(ch => Chunk.single(ch.size)).runCollect
      } yield assert(chunkSizes)(forall(equalTo(2))) && assert(chunkSizes.size)(equalTo(5))),
      testM("should return error on invalid query")(
        for {
          session <- ZIO.service[service.CassandraSession]
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
    session <- {
      val address = new InetSocketAddress(cassandra.containerIpAddress, cassandra.mappedPort(9042))
      val config  = ConfigFactory.load().getConfig("cassandra.test-driver")
      CassandraSession.make(config, Seq(address))
    }
    _ <- prepareTestSession(session).toManaged_
  } yield session).toLayer.mapError(TestFailure.die)

  val layer = layaerCassandra >+> layerSession

  def prepareTestSession(session: CassandraSession): Task[Unit] =
    for {
      table  <- ZIO.succeed("prepared_data")
      _      <- session.execute(keyspaceQuery)
      _      <- session.execute(tableQuery(table))
      insert <- session.prepare(insertQuery(table))
      preparedBatchSeq <- ZIO.foreach(0.until(10).toList) { i =>
                           session.bind(insert, Seq("user1", i.asJava, i.toString, Instant.now()))
                         }
      batch = BatchStatement
        .builder(DefaultBatchType.LOGGED)
        .addStatements(preparedBatchSeq: _*)
        .build()
      _ <- session.execute(batch)
    } yield ()

  def withSession[R](f: service.CassandraSession => Task[R]): ZIO[Session, Throwable, R] = ZIO.accessM[Session] {
    session => f(session.get)
  }

  def executeBatch(seq: Seq[BoundStatement]): RIO[Session, Unit] = withSession { s =>
    val batch = BatchStatement
      .builder(DefaultBatchType.LOGGED)
      .addStatements(seq: _*)
      .build()
    s.execute(batch).unit
  }

}
