package zio.cassandra.session

import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metrics.Metrics
import com.datastax.oss.driver.api.core.{ CqlIdentifier, CqlSession, CqlSessionBuilder }
import zio._
import zio.cassandra.session.cql.cache.AsyncCache
import zio.cassandra.session.cql.codec.Reads
import zio.cassandra.session.cql.query.{ Batch, QueryTemplate }
import zio.stream.ZStream.Pull
import zio.stream.{ Stream, ZStream }

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.OptionConverters.RichOptional
import scala.language.existentials
import zio.stream.ZChannel

trait Session {

  def prepare(stmt: String): Task[PreparedStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  /** Continuously querying the effect, until empty response returned. Meaning that effect should provide new statement
    * on each materialization, otherwise it might produce an infinite stream.
    */
  def repeatZIO[R, A: Reads](stmt: ZIO[R, Throwable, Statement[_]]): ZStream[R, Throwable, A]

  /** Same as `repeatZIO(ZIO[R, Throwable, Statement[_]])`, but allows to use high-level query.
    */
  def repeatZIO[R, A](template: ZIO[R, Throwable, QueryTemplate[A]]): ZStream[R, Throwable, A]

  // short-cuts
  def selectFirst(stmt: Statement[_]): Task[Option[Row]]

  final def prepare[A](query: QueryTemplate[A]): Task[BoundStatement] =
    prepare(query.query).map(st => query.config(st.bind()))

  final def execute(template: QueryTemplate[_]): Task[Boolean] = for {
    st  <- prepare(template)
    res <- execute(st)
  } yield res.wasApplied

  final def execute(batch: Batch): Task[Boolean] =
    execute(batch.build).map(_.wasApplied)

  final def select[A](template: QueryTemplate[A]): Stream[Throwable, A] =
    ZStream.fromZIO(prepare(template)).flatMap { st =>
      select(st).mapChunksZIO(chunk => ZIO.attempt(chunk.map(template.reads.read)))
    }

  final def selectFirst[A](template: QueryTemplate[A]): Task[Option[A]] = for {
    st    <- prepare(template)
    first <- selectFirst(st)
    res   <- ZIO.foreach(first)(first => ZIO.attempt(template.reads.read(first)))
  } yield res

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

  private val DEFAULT_CAPACITY = 256L

  private final case class Live(private val underlying: CqlSession, capacity: Long = DEFAULT_CAPACITY) extends Session {
    private val preparedStatementCache =
      new AsyncCache[String, PreparedStatement](underlying.prepareAsync(_).toCompletableFuture)(capacity)

    override def prepare(stmt: String): Task[PreparedStatement] = preparedStatementCache.get(stmt)

    override def execute(stmt: Statement[_]): Task[AsyncResultSet] =
      ZIO.fromCompletionStage(underlying.executeAsync(stmt))

    override def execute(query: String): Task[AsyncResultSet] =
      ZIO.fromCompletionStage(underlying.executeAsync(query))

    private def write[R, A](
      in: java.lang.Iterable[Row],
      fn: Row => A
    ): ZChannel[R, Any, Any, Any, Throwable, Chunk[A], Any] = {
      def toChunk(it: java.util.Iterator[Row]) = {
        val builder = ChunkBuilder.make[A]()
        while (it.hasNext()) {
          val a = it.next()
          builder += fn(a)
        }
        builder.result()
      }

      ZChannel.fromZIO(ZIO.attempt(toChunk(in.iterator()))).flatMap(ZChannel.write)
    }

    private def loop[R, A](
      io: ZIO[R, Throwable, QueryTemplate[A]],
      continuous: Boolean
    ): ZChannel[R, Any, Any, Any, Throwable, Chunk[A], Any] =
      ZChannel.fromZIO(io).flatMap { qt =>
        loop(prepare(qt).flatMap(execute), qt.reads.read, continuous, loop(io, continuous))
      }

    private def loop[R, A](
      io: ZIO[R, Throwable, Statement[_]],
      fn: Row => A,
      continuous: Boolean
    ): ZChannel[R, Any, Any, Any, Throwable, Chunk[A], Any] =
      loop(io.flatMap(execute), fn, continuous, loop(io, fn, continuous))

    private def loop[R, A, B](
      ch: ZIO[R, Throwable, AsyncResultSet],
      fn: Row => A,
      continuous: Boolean,
      next: => ZChannel[R, Any, Any, Any, Throwable, Chunk[A], Any]
    ): ZChannel[R, Any, Any, Any, Throwable, Chunk[A], Any] =
      ZChannel.fromZIO(ch).flatMap {
        case rs if rs.hasMorePages                     =>
          write(rs.currentPage(), fn) *> loop(
            ZIO.fromCompletionStage(rs.fetchNextPage()),
            fn,
            continuous,
            next
          )
        case rs if rs.currentPage().iterator().hasNext =>
          if (continuous) {
            write(rs.currentPage(), fn) *> next
          } else write(rs.currentPage(), fn)
        case _                                         => ZChannel.unit
      }

    override def select(stmt: Statement[_]): Stream[Throwable, Row] =
      ZStream.fromChannel(loop(ZIO.succeed(stmt), identity, continuous = false))

    override def repeatZIO[R, O](stmt: ZIO[R, Throwable, Statement[_]])(implicit
      rds: Reads[O]
    ): ZStream[R, Throwable, O] =
      ZStream.fromChannel(loop(stmt, rds.read, continuous = true))

    override def selectFirst(stmt: Statement[_]): Task[Option[Row]] = {
      // setPageSize returns T <: Statement[T] for any T, but Scala can't figure it out without clues that will spoil library API
      val single = stmt.setPageSize(1).asInstanceOf[Statement[_]]
      execute(single).map(rs => Option(rs.one()))
    }

    override def repeatZIO[R, A](template: ZIO[R, Throwable, QueryTemplate[A]]): ZStream[R, Throwable, A] =
      ZStream.fromChannel(loop(template, continuous = true))

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

  def live(cacheCapacity: Long = DEFAULT_CAPACITY): RIO[Scope with CqlSessionBuilder, Session] =
    ZIO.serviceWithZIO[CqlSessionBuilder](cqlSessionBuilder => make(cqlSessionBuilder, cacheCapacity))

  def make(builder: => CqlSessionBuilder, cacheCapacity: Long = DEFAULT_CAPACITY): RIO[Scope, Session] = ZIO
    .acquireRelease(ZIO.fromCompletionStage(builder.buildAsync())) { session =>
      ZIO.fromCompletionStage(session.closeAsync()).orDie
    }
    .map(Live(_, cacheCapacity))

  def existing(session: CqlSession, cacheCapacity: Long = DEFAULT_CAPACITY): Session =
    Live(session, cacheCapacity)

}
