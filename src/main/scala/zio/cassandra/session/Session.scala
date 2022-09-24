package zio.cassandra.session

import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metrics.Metrics
import com.datastax.oss.driver.api.core.{ CqlIdentifier, CqlSession, CqlSessionBuilder }
import shapeless.HList
import zio._
import zio.cassandra.session.cql.query.{ ParameterizedQuery, PreparedQuery, Query, QueryTemplate }
import zio.macros.accessible
import zio.stream.Stream
import zio.stream.ZStream.Pull

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.OptionConverters.RichOptional

@accessible
trait Session {

  def prepare(stmt: String): Task[PreparedStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  // short-cuts
  def selectFirst(stmt: Statement[_]): Task[Option[Row]]

  final def prepare[V <: HList, R](query: QueryTemplate[V, R]): Task[PreparedQuery[V, R]] = {
    import query.{ binder, reads }
    prepare(query.query).map(new PreparedQuery[V, R](this, _, query.config))
  }

  final def prepare[V <: HList, R](query: ParameterizedQuery[V, R]): Task[Query[R]] =
    prepare(query.template).map(_.applyProduct(query.values))

  final def execute[V <: HList](query: ParameterizedQuery[V, _]): Task[Boolean] =
    prepare(query).flatMap(_.execute)

  final def select[V <: HList, R](query: ParameterizedQuery[V, R]): Stream[Throwable, R] =
    Stream.fromEffect(prepare(query)).flatMap(_.select)

  final def selectFirst[V <: HList, R](query: ParameterizedQuery[V, R]): Task[Option[R]] =
    prepare(query).flatMap(_.selectFirst)

  // other methods
  def metrics: Option[Metrics]
  def name: String
  def refreshSchema: Task[Metadata]
  def setSchemaMetadataEnabled(newValue: Boolean): Task[Metadata]
  def isSchemaMetadataEnabled: Boolean
  def checkSchemaAgreement: Task[Boolean]

  def context: DriverContext
  def keyspace: Option[CqlIdentifier]

}

object Session {

  private final case class Live(private val underlying: CqlSession) extends Session {
    override def prepare(stmt: String): Task[PreparedStatement] =
      Task.fromCompletionStage(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      Task.fromCompletionStage(underlying.executeAsync(stmt))

    override def execute(query: String): Task[AsyncResultSet] =
      Task.fromCompletionStage(underlying.executeAsync(query))

    override def select(stmt: Statement[_]): Stream[Throwable, Row] = {
      def pull(ref: Ref[ZIO[Any, Option[Throwable], AsyncResultSet]]): ZIO[Any, Option[Throwable], Chunk[Row]] =
        for {
          io <- ref.get
          rs <- io
          _  <- rs match {
                  case _ if rs.hasMorePages                     =>
                    ref.set(Task.fromCompletionStage(rs.fetchNextPage()).mapError(Option(_)))
                  case _ if rs.currentPage().iterator().hasNext => ref.set(Pull.end)
                  case _                                        => Pull.end
                }
        } yield Chunk.fromIterable(rs.currentPage().asScala)

      Stream {
        for {
          ref <- Ref.make(execute(stmt).mapError(Option(_))).toManaged_
        } yield pull(ref)
      }
    }

    override def selectFirst(stmt: Statement[_]): Task[Option[Row]] =
      execute(stmt).map(rs => Option(rs.one()))

    override def metrics: Option[Metrics] =
      underlying.getMetrics.toScala

    override def name: String = underlying.getName

    override def refreshSchema: Task[Metadata] =
      Task.fromCompletionStage(underlying.refreshSchemaAsync())

    override def setSchemaMetadataEnabled(newValue: Boolean): Task[Metadata] =
      Task.fromCompletionStage(underlying.setSchemaMetadataEnabled(newValue))

    override def isSchemaMetadataEnabled: Boolean = underlying.isSchemaMetadataEnabled

    override def checkSchemaAgreement: Task[Boolean] =
      Task
        .fromCompletionStage(underlying.checkSchemaAgreementAsync())
        .map(Boolean.unbox)

    override def context: DriverContext = underlying.getContext

    override def keyspace: Option[CqlIdentifier] = underlying.getKeyspace.toScala
  }

  def live: ZManaged[Has[CqlSessionBuilder], Throwable, Session] =
    ZManaged.serviceWithManaged[CqlSessionBuilder] { cqlSessionBuilder =>
      make(cqlSessionBuilder)
    }

  def make(builder: => CqlSessionBuilder): TaskManaged[Session] =
    ZManaged
      .make(Task.fromCompletionStage(builder.buildAsync())) { session =>
        Task.fromCompletionStage(session.closeAsync()).orDie
      }
      .map(Live)

  def existing(session: CqlSession): Session =
    Live(session)
}
