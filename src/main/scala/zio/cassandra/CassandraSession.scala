package zio.cassandra

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.typesafe.config.Config
import zio.{ Task, TaskManaged }
import zio.interop.reactivestreams._
import zio.stream.Stream

import scala.jdk.CollectionConverters._

object CassandraSession {
  import Task.{ fromCompletionStage => fromJavaAsync }
  class Live(underlying: CqlSession) extends service.CassandraSession {
    override def prepare(stmt: String): Task[PreparedStatement] =
      fromJavaAsync(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(stmt))

    override def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
      Task(stmt.bind(bindValues: _*))

    override def select(stmt: Statement[_]): Stream[Throwable, ReactiveRow] =
      Stream.fromEffect(Task(underlying.executeReactive(stmt).toStream(qSize = 256))).flatten

    override def execute(query: String): Task[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(query))
  }

  def make(builder: CqlSessionBuilder): TaskManaged[service.CassandraSession] =
    make(builder.buildAsync())

  def make(config: Config): TaskManaged[service.CassandraSession] =
    make(
      CqlSession
        .builder()
        .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
    )

  def make(
    config: Config,
    contactPoints: Seq[InetSocketAddress],
    auth: Option[(String, String)] = None
  ): TaskManaged[service.CassandraSession] = {
    val builder = CqlSession
      .builder()
      .withConfigLoader(new DefaultDriverConfigLoader(() => config, false))
      .addContactPoints(contactPoints.asJavaCollection)

    make(auth.fold(builder) {
      case (username, password) =>
        builder.withAuthCredentials(username, password)
    })
  }

  private def make(session: => CompletionStage[CqlSession]): TaskManaged[service.CassandraSession] =
    fromJavaAsync(session).toManaged(session => fromJavaAsync(session.closeAsync()).orDie).map(new Live(_))

}
