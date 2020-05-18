package cassandra

import java.net.InetSocketAddress
import java.time.Instant

import cassandra.embedded.EmbeddedCassandra
import cassandra.embedded.EmbeddedCassandra.Embedded
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, PreparedStatement}
import com.typesafe.config.ConfigFactory
import wvlet.log.{Logger, LogLevel, LogSupport}
import zio.blocking._
import zio.cassandra.config.CassandraConnectionConfig
import zio.cassandra.session.Session
import zio.cassandra.session.Session.CassandraSession
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, ZSpec, _}
import zio.{Task, ZIO, ZLayer}

abstract class CassandraSessionSpec extends DefaultRunnableSpec with LogSupport {
  Logger.setDefaultLogLevel(LogLevel.ERROR)

  import CassandraSessionSpec._

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Work with cassandra session - complete scenario")(
      testM("Just create correct service and run queries")(
        (for {
          _ <- blocking(
                ZIO
                  .accessM[Embedded] { s =>
                    ZIO {
                      val host = s.get.getAddress
                      val port = s.get.getPort
                      info(s"Checking cassandra status: ${host.getHostName}:$port")
                      port
                    }
                  }
              ).doWhile(_ == -1)
          _           <- withSession(_.executeDDL(keyspaceQuery))
          _           <- withSession(_.executeDDL(tableQuery))
          insert      <- prepareStatement(insertQuery)
          update      <- prepareStatement(updateQuery)
          delete      <- prepareStatement(deleteQuery)
          select      <- prepareStatement(selectQuery)
          emptyResult <- withSession(s => s.bind(select, Seq("user1")).flatMap(s.selectOne))
          preparedBatchSeq <- withSession { s =>
                               ZIO.collectAllSuccesses(0.until(10).map { i =>
                                 s.bind(insert, Seq("user1", i.asJava, i.toString, Instant.now()))
                               })
                             }
          _         <- executeBatch(preparedBatchSeq)
          _         <- withSession(s => s.bind(update, Seq("nope", "user1", 2.asJava)).flatMap(s.executeWrite))
          _         <- withSession(s => s.bind(delete, Seq("user1", 1.asJava)).flatMap(s.executeWrite))
          selectAll <- withSession(s => s.bind(select, Seq("user1")).flatMap(s.selectAll))
        } yield {
          assert(emptyResult)(isNone) &&
          assert(selectAll.size)(equalTo(9)) &&
          assert(
            selectAll
              .find(r => r.getInt("seq_nr") == 2)
              .map(_.getString("data"))
          )(isSome(equalTo("nope")))
        }).provideCustomLayer(layers ++ goodSession)
      ),
      test("Invalid contact-points") {
        assert(
          CassandraConnectionConfig(badConfig)
        )(isLeft)
      },
      test("Valid contact-points") {
        assert(CassandraConnectionConfig(goodConfig))(
          isRight(equalTo(
            CassandraConnectionConfig(List(new InetSocketAddress("1.2.3.4", 9042), new InetSocketAddress("5.6.7.8", 9)),
                                      Some("good_user"),
                                      Some("cassandra"),
                                      Some("fast"))
          ))
        )
      }
    )
}

object CassandraSessionSpec {
  val keyspace = "test_keyspace"

  val keyspaceQuery =
    s"""CREATE KEYSPACE $keyspace
       |  WITH REPLICATION =
       |    {'class' : 'SimpleStrategy', 'replication_factor' : 1}""".stripMargin

  val table = "test_table"

  val tableQuery =
    s"""CREATE TABLE $keyspace.$table(
       |user_id text,
       |seq_nr int,
       |data text,
       |created_at timestamp,
       |PRIMARY KEY (user_id, seq_nr))""".stripMargin

  val insertQuery =
    s"""
       |INSERT INTO $keyspace.$table (user_id, seq_nr, data, created_at) values (?, ?, ?, ?)
       |""".stripMargin

  val updateQuery =
    s"""
       |UPDATE $keyspace.$table SET data = ? WHERE user_id = ? and seq_nr = ?
       |""".stripMargin

  val deleteQuery =
    s"""
       |DELETE FROM $keyspace.$table WHERE user_id = ? and seq_nr = ?
       |""".stripMargin

  val selectQuery =
    s"""
       |SELECT user_id, seq_nr, data, created_at FROM $keyspace.$table WHERE user_id = ?
       |""".stripMargin

  val layers: ZLayer[Any, Throwable, _root_.zio.test.environment.TestEnvironment with Embedded] =
    zio.test.environment.testEnvironment ++
      EmbeddedCassandra.createInstance(9042)

  val goodSession = Session.create(ConfigFactory.load().getConfig("test-driver"))

  val badConfig = ConfigFactory.load("bad.conf")

  val goodConfig = ConfigFactory.load("good.conf")

  val badSession = Session.create(badConfig.getConfig("test-driver"))

  def withSession[R](f: CassandraSession => Task[R]): ZIO[Session, Throwable, R] = ZIO.accessM[Session] { session =>
    f(session.get)
  }

  def prepareStatement(stmt: String): ZIO[Session, Throwable, PreparedStatement] = withSession(_.prepare(stmt))

  def executeBatch(seq: Seq[BoundStatement]): ZIO[Session, Throwable, Unit] = withSession { s =>
    val batch = BatchStatement.newInstance(DefaultBatchType.LOGGED)
    batch.addAll(seq: _*)
    s.executeWriteBatch(batch)
  }

  implicit class toJavaInt(val i: Int) extends AnyVal {
    def asJava: Integer = i.asInstanceOf[java.lang.Integer]
  }
}
