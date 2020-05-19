package zio.cassandra

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.typesafe.config.Config
import zio.{ Task, TaskLayer, TaskManaged }
import zio.interop.reactivestreams._
import zio.stream.Stream

import scala.jdk.CollectionConverters._

object Session {
  import Task.{ fromCompletionStage => fromJavaAsync }
  class Live protected (underlying: CqlSession) extends service.Session {
    override def prepare(stmt: String): Task[PreparedStatement] =
      fromJavaAsync(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(stmt))

    override def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
      Task(stmt.bind(bindValues: _*))

    override def select(stmt: Statement[_]): Stream[Throwable, ReactiveRow] =
      underlying.executeReactive(stmt).toStream(qSize = 256)

    override def execute(query: String): Task[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(query))
  }

  object Live {
    def open(builder: CqlSessionBuilder): TaskManaged[service.Session] =
      make(builder.buildAsync())

    def open(config: Config): TaskManaged[service.Session] =
      open(
        CqlSession
          .builder()
          .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
      )

    def open(
      config: Config,
      contactPoints: Seq[InetSocketAddress],
      auth: Option[(String, String)] = None
    ): TaskManaged[service.Session] = {
      val builder = CqlSession
        .builder()
        .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
        .addContactPoints(contactPoints.asJavaCollection)

      open(auth.fold(builder) {
        case (username, password) =>
          builder.withAuthCredentials(username, password)
      })
    }

    private def make(session: => CompletionStage[CqlSession]): TaskManaged[service.Session] =
      fromJavaAsync(session).toManaged(session => fromJavaAsync(session.closeAsync()).orDie).map(new Live(_))
  }

  def live(builder: CqlSessionBuilder): TaskLayer[Session] =
    Live.open(builder).toLayer

  def live(config: Config): TaskLayer[Session] =
    Live.open(config).toLayer

  def live(
    config: Config,
    contactPoints: Seq[InetSocketAddress],
    auth: Option[(String, String)] = None
  ): TaskLayer[Session] =
    Live.open(config, contactPoints, auth).toLayer

}
