package zio.cassandra.session

import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metrics.Metrics
import com.datastax.oss.driver.api.core.{ CqlIdentifier, CqlSession, CqlSessionBuilder }
import zio._
import zio.cassandra.session.cql.query.{ Batch, PreparedQuery, QueryTemplate }
import zio.stream.ZStream.Pull
import zio.stream.{ Stream, ZStream }

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.OptionConverters.RichOptional

trait Session {

  def prepare(stmt: String): Task[PreparedStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(stmt: Task[Statement[_]]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  // Use it for continuous quering
  // Example: key ((p_id, p_nr), seq_nr) - to fetch all records for composite key `p_id`
  // we have to first call `select(p_id, p_nr_0)` then `select(p_id, p_nr_1)` & etc
  // Using the Task[Statement] instead of Statement allow us to
  // fetch all the records as a single stream
  def select(stmt: Task[Statement[_]]): Stream[Throwable, Row]

  // short-cuts
  def selectFirst(stmt: Statement[_]): Task[Option[Row]]

  final def prepare[R](query: QueryTemplate[R]): Task[PreparedQuery[R]] = {
    import query.reads
    prepare(query.query).map(PreparedQuery[R](this, _, query.config))
  }

  final def execute(template: QueryTemplate[_]): Task[Boolean] =
    prepare(template).flatMap(_.execute)

  final def execute(batch: Batch): Task[Boolean] =
    execute(batch.build).map(_.wasApplied)

  final def select[R](template: QueryTemplate[R]): Stream[Throwable, R] =
    ZStream.fromZIO(prepare(template)).flatMap(_.select)

  final def selectFirst[R](template: QueryTemplate[R]): Task[Option[R]] =
    prepare(template).flatMap(_.selectFirst)

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
      ZIO.fromCompletionStage(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      ZIO.fromCompletionStage(underlying.executeAsync(stmt))

    override def execute(query: String): Task[AsyncResultSet] =
      ZIO.fromCompletionStage(underlying.executeAsync(query))

    override def execute(stmt: Task[Statement[_]]): Task[AsyncResultSet] = for {
      st  <- stmt
      res <- execute(st)
    } yield res

    private def select(stmt: Task[Statement[_]], continuous: Boolean): Stream[Throwable, Row] = {
      def pull(ref: Ref[ZIO[Any, Option[Throwable], AsyncResultSet]]): ZIO[Any, Option[Throwable], Chunk[Row]] =
        for {
          io <- ref.get
          rs <- io
          _  <- rs match {
                  case _ if rs.hasMorePages                     =>
                    ref.set(ZIO.fromCompletionStage(rs.fetchNextPage()).mapError(Option(_)))
                  case _ if rs.currentPage().iterator().hasNext =>
                    ref.set(if (continuous) execute(stmt).mapError(Option(_)) else Pull.end)
                  case _                                        =>
                    Pull.end
                }
        } yield Chunk.fromIterable(rs.currentPage().asScala)

      ZStream.fromPull {
        for {
          ref <- Ref.make(execute(stmt).mapError(Option(_)))
        } yield pull(ref)
      }
    }

    override def select[R](stmt: ZIO[R, Throwable, Statement[_]]): ZStream[R, Throwable, Row] =
      select(stmt, continuous = true)

    override def select(stmt: Statement[_]): Stream[Throwable, Row] =
      select(ZIO.succeed(stmt), continuous = false)

    override def selectFirst(stmt: Statement[_]): Task[Option[Row]] =
      execute(stmt).map(rs => Option(rs.one()))

    override def metrics: Option[Metrics] =
      underlying.getMetrics.toScala

    override def name: String = underlying.getName

    override def refreshSchema: Task[Metadata] =
      ZIO.fromCompletionStage(underlying.refreshSchemaAsync())

    override def setSchemaMetadataEnabled(newValue: Boolean): Task[Metadata] =
      ZIO.fromCompletionStage(underlying.setSchemaMetadataEnabled(newValue))

    override def isSchemaMetadataEnabled: Boolean = underlying.isSchemaMetadataEnabled

    override def checkSchemaAgreement: Task[Boolean] =
      ZIO
        .fromCompletionStage(underlying.checkSchemaAgreementAsync())
        .map(Boolean.unbox)

    override def context: DriverContext = underlying.getContext

    override def keyspace: Option[CqlIdentifier] = underlying.getKeyspace.toScala
  }

  val live: RIO[Scope with CqlSessionBuilder, Session] =
    ZIO.serviceWithZIO[CqlSessionBuilder](cqlSessionBuilder => make(cqlSessionBuilder))

  def make(builder: => CqlSessionBuilder): RIO[Scope, Session] =
    ZIO
      .acquireRelease(ZIO.fromCompletionStage(builder.buildAsync())) { session =>
        ZIO.fromCompletionStage(session.closeAsync()).orDie
      }
      .map(Live(_))

  def existing(session: CqlSession): Session =
    Live(session)
}
