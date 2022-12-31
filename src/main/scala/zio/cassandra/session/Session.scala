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
import scala.language.existentials

trait Session {

  def prepare(stmt: String): Task[PreparedStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  /*
   * Continuously quering the effect, until empty response returned.
   * Meaning that effect should provide new statement on each materialization.
   */
  def repeatZIO[R](stmt: ZIO[R, Throwable, Statement[_]]): ZStream[R, Throwable, Row]

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

    private def repeatZIO[R](
      stmt: ZIO[R, Throwable, Statement[_]],
      continuous: Boolean
    ): ZStream[R, Throwable, Row] = {
      val executeOpt                                                                                       = stmt.flatMap(execute).mapError(Option(_))
      def pull(ref: Ref[ZIO[R, Option[Throwable], AsyncResultSet]]): ZIO[R, Option[Throwable], Chunk[Row]] =
        for {
          io <- ref.get
          rs <- io
          _  <- rs match {
                  case _ if rs.hasMorePages                     =>
                    ref.set(ZIO.fromCompletionStage(rs.fetchNextPage()).mapError(Option(_)))
                  case _ if rs.currentPage().iterator().hasNext =>
                    ref.set(if (continuous) executeOpt else Pull.end)
                  case _                                        =>
                    Pull.end
                }
        } yield Chunk.fromIterable(rs.currentPage().asScala)

      ZStream.fromPull {
        for {
          ref <- Ref.make(executeOpt)
        } yield pull(ref)
      }
    }

    override def repeatZIO[R](stmt: ZIO[R, Throwable, Statement[_]]): ZStream[R, Throwable, Row] =
      repeatZIO(stmt, continuous = true)

    override def select(stmt: Statement[_]): Stream[Throwable, Row] =
      repeatZIO(ZIO.succeed(stmt), continuous = false)

    override def selectFirst(stmt: Statement[_]): Task[Option[Row]] = {
      // setPageSize returns T <: Statement[T] for any T, but Scala can't figure it out without clues that will spoil library API
      val single = stmt.setPageSize(1).asInstanceOf[Statement[_]]
      execute(single).map(rs => Option(rs.one()))
    }

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
