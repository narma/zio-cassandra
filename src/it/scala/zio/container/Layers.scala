package zio.container

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.dimafeng.testcontainers.CassandraContainer
import com.typesafe.config.ConfigFactory
import zio._
import zio.cassandra.session.Session
import zio.stream._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters.IterableHasAsJava

object Layers {

  private val keyspace = "tests"

  private def migrateSession(session: Session): Task[Unit] = {
    val migrations = ZStream
      .fromResource("migration/1__test_tables.cql")
      .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
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
      migrations <- migrations

      _ <- ZIO.foreachDiscard(migrations) { migration =>
             val st = SimpleStatement.newInstance(migration)
             session.execute(st)
           }
      _ <- ZIO.debug("migration done")
    } yield ()
  }

  private def ensureKeyspaceExists(builder: CqlSessionBuilder): Task[Unit] =
    for {
      session <- ZIO.fromCompletionStage(builder.withKeyspace(Option.empty[String].orNull).buildAsync())
      _       <-
        ZIO
          .fromCompletionStage(
            session.executeAsync(
              s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
            )
          )
          .unless(session.getMetadata.getKeyspace(keyspace).isPresent)
      _       <- ZIO.fromCompletionStage(session.closeAsync())
    } yield ()

  val layerCassandra: ZLayer[Any, Throwable, CassandraContainer] = ZTestContainer.cassandra

  val layerSession: ZLayer[CassandraContainer, Throwable, Session] =
    ZLayer.scoped {
      for {
        cassandra <- ZIO.service[CassandraContainer]
        address    = new InetSocketAddress(cassandra.containerIpAddress, cassandra.mappedPort(9042))
        config    <- ZIO.attempt(ConfigFactory.load().getConfig("cassandra.test-driver"))
        builder    = CqlSession
                       .builder()
                       .addContactPoints(Seq(address).asJavaCollection)
                       .withLocalDatacenter("datacenter1")
                       .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
                       .withKeyspace(keyspace)
        _         <- ensureKeyspaceExists(builder)

        session <- Session.make(builder)
        _       <- migrateSession(session)
      } yield session
    }

  val layer: ZLayer[Any, Throwable, CassandraContainer with Session] =
    layerCassandra >+> layerSession

}
