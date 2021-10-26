package zio.cassandra

import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.typesafe.config.Config
import zio.stream.Stream
import zio.stream.ZStream.Pull
import zio.{Chunk, Ref, Task, TaskManaged, ZIO}

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage
import scala.jdk.CollectionConverters._

object CassandraSession {
  import Task.{fromCompletionStage => fromJavaAsync}
  class Live(underlying: CqlSession) extends service.CassandraSession {
    override def prepare(stmt: String): Task[PreparedStatement] =
      fromJavaAsync(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(stmt))

    override def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
      Task(stmt.bind(bindValues: _*))

    override def select(stmt: Statement[_]): Stream[Throwable, Row] =  {
      def pull(ref: Ref[ZIO[Any, Option[Throwable], AsyncResultSet]]) =
        for {
          io <- ref.get
          rs <- io
          _ <- rs match {
            case _ if rs.hasMorePages =>
              ref.set(fromJavaAsync(rs.fetchNextPage()).mapError(Option(_)))
            case _ if rs.currentPage().iterator().hasNext => ref.set(Pull.end)
            case _                                        => Pull.end
          }
        } yield Chunk.fromArray(rs.currentPage().asScala.toArray)

      Stream {
        for {
          ref <- Ref.make(execute(stmt).mapError(Option(_))).toManaged_
        } yield pull(ref)
      }
    }

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
