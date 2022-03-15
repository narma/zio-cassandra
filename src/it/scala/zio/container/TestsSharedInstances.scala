package zio.container

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.dimafeng.testcontainers.CassandraContainer
import com.typesafe.config.ConfigFactory
import org.testcontainers.utility.DockerImageName
import zio._
import zio.blocking.Blocking
import zio.cassandra.session.Session
import zio.stream._
import zio.test.{ AbstractRunnableSpec, TestFailure }

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters.IterableHasAsJava

trait TestsSharedInstances { self: AbstractRunnableSpec =>

  val keyspace  = "tests"
  val container = CassandraContainer(DockerImageName.parse("cassandra:3.11.11"))

  def migrateSession(session: Session): RIO[Blocking, Unit] = {
    val migrations = Stream
      .fromResource("migration/1__test_tables.cql")
      .transduce(ZTransducer.utf8Decode >>> ZTransducer.splitLines)
      .filterNot { line =>
        val l = line.stripLeading()
        l.startsWith("//") || l.startsWith("--")
      }
      .runCollect
      .map { chunk =>
        chunk.mkString("").split(';').toList.map(_.strip())
      }

    for {
      _          <- ZIO.debug("start migrations")
      _          <- session.execute(s"use $keyspace")
      migrations <- migrations

      _ <- ZIO.foreach_(migrations) { migration =>
             val st = SimpleStatement.newInstance(migration)
             session.execute(st)
           }
      _ <- ZIO.debug("migration done")
    } yield ()
  }

  def ensureKeyspaceExists(builder: CqlSessionBuilder): Task[Unit] =
    for {
      session <- Task.fromCompletionStage(builder.withKeyspace(Option.empty[String].orNull).buildAsync())
      _       <-
        Task
          .fromCompletionStage(
            session.executeAsync(
              s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
            )
          )
          .unless(session.getMetadata.getKeyspace(keyspace).isPresent)
      _       <- Task.fromCompletionStage(session.closeAsync())
    } yield ()

  val layerCassandra = ZTestContainer.cassandra

  val layerSession = (for {
    cassandra <- ZManaged.service[CassandraContainer]
    address    = new InetSocketAddress(cassandra.containerIpAddress, cassandra.mappedPort(9042))
    config    <- Task(ConfigFactory.load().getConfig("cassandra.test-driver")).toManaged_
    builder    = CqlSession
                   .builder()
                   .addContactPoints(Seq(address).asJavaCollection)
                   .withLocalDatacenter("datacenter1")
                   .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
                   .withKeyspace(keyspace)
    _         <- ensureKeyspaceExists(builder).toManaged_

    session <- Session.make(builder)
    _       <- migrateSession(session).toManaged_
  } yield session).toLayer.mapError(TestFailure.die)

  val layer: ZLayer[Blocking, TestFailure[Nothing], Has[Session] with Has[CassandraContainer]] =
    ZLayer.requires[Blocking] >+> layerCassandra >+> layerSession
}
