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
import zio.cassandra.session.cql.codec.Reads

trait Session {

  def prepare(stmt: String): Task[PreparedStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  /** Continuously querying the effect, until empty response returned. Meaning that effect should provide new statement
    * on each materialization.
    */
  def repeatZIO[R, O: Reads](stmt: ZIO[R, Throwable, Statement[_]]): ZStream[R, Throwable, O]

  // short-cuts
  def selectFirst(stmt: Statement[_]): Task[Option[Row]]

  /** Same as `repeatZIO`, but allows to use queries at higher level. Has different name only because otherwise type
    * erasure won't allow method overloading
    */
  def repeatTemplateZIO[R, A](template: ZIO[R, Throwable, QueryTemplate[A]]): ZStream[R, Throwable, A]

  final def prepare[A](query: QueryTemplate[A]): Task[PreparedQuery[A]] = {
    import query.reads
    prepare(query.query).map(PreparedQuery[A](this, _, query.config))
  }

  final def execute(template: QueryTemplate[_]): Task[Boolean] =
    prepare(template).flatMap(_.execute)

  final def execute(batch: Batch): Task[Boolean] =
    execute(batch.build).map(_.wasApplied)

  final def select[A](template: QueryTemplate[A]): Stream[Throwable, A] =
    ZStream.fromZIO(prepare(template)).flatMap(_.select)

  final def selectFirst[A](template: QueryTemplate[A]): Task[Option[A]] =
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

    private def repeatZIO[R, O](
      stmt: ZIO[R, Throwable, (Statement[_], Row => O)],
      continuous: Boolean
    ): ZStream[R, Throwable, O] = {
      val executeOpt = stmt.flatMap { case (s, o) =>
        execute(s).map(_ -> o)
      }.mapError(Option(_))

      def pull(
        ref: Ref[Function0[ZIO[R, Option[Throwable], (AsyncResultSet, Row => O)]]]
      ): ZIO[R, Option[Throwable], Chunk[O]] =
        for {
          io      <- ref.get
          tp      <- io()
          (rs, fn) = tp
          _       <- rs match {
                       case _ if rs.hasMorePages                     =>
                         ref.set(() => ZIO.fromCompletionStage(rs.fetchNextPage()).map(_ -> fn).mapError(Option(_)))
                       case _ if rs.currentPage().iterator().hasNext =>
                         ref.set(if (continuous) () => executeOpt else () => Pull.end)
                       case _                                        =>
                         Pull.end
                     }
        } yield Chunk.fromIterable(rs.currentPage().asScala).map(fn)

      ZStream.fromPull {
        for {
          ref <- Ref.make(() => executeOpt)
        } yield pull(ref)
      }
    }

    override def select(stmt: Statement[_]): Stream[Throwable, Row] =
      repeatZIO(ZIO.succeed(stmt -> identity), false)

    override def repeatZIO[R, O](stmt: ZIO[R, Throwable, Statement[_]])(implicit
      rds: Reads[O]
    ): ZStream[R, Throwable, O] =
      repeatZIO(stmt.map(_ -> rds.read _), true)

    override def selectFirst(stmt: Statement[_]): Task[Option[Row]] = {
      // setPageSize returns T <: Statement[T] for any T, but Scala can't figure it out without clues that will spoil library API
      val single = stmt.setPageSize(1).asInstanceOf[Statement[_]]
      execute(single).map(rs => Option(rs.one()))
    }

    override def repeatTemplateZIO[R, A](
      template: ZIO[R, Throwable, QueryTemplate[A]]
    ): ZStream[R, Throwable, A] = {
      val io = template.flatMap { tm =>
        prepare(tm.query).map(ps => tm.config(ps.bind()) -> (tm.reads.read _))
      }
      repeatZIO(io, true)
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

  def make(builder: => CqlSessionBuilder): RIO[Scope, Session] = ZIO
    .acquireRelease(ZIO.fromCompletionStage(builder.buildAsync())) { session =>
      ZIO.fromCompletionStage(session.closeAsync()).orDie
    }
    .map(Live(_))

  def existing(session: CqlSession): Session =
    Live(session)

}
