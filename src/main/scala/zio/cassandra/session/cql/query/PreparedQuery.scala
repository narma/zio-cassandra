package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.{ BoundStatement, PreparedStatement }
import zio.cassandra.session.Session
import zio.cassandra.session.cql.codec.Reads
import zio.stream.Stream
import zio.{ Task, ZIO }

class PreparedQuery[R: Reads] private[cql] (
  session: Session,
  private[cql] val statement: BoundStatement
) {

  def config(statement: BoundStatement => BoundStatement) = new PreparedQuery[R](session, statement(this.statement))

  def select: Stream[Throwable, R] = session.select(statement).mapChunksZIO { chunk =>
    chunk.mapZIO(row => ZIO.attempt(Reads[R].read(row)))
  }

  def selectFirst: Task[Option[R]] = session.selectFirst(statement).flatMap {
    case None      => ZIO.none
    case Some(row) => ZIO.attempt(Reads[R].read(row)).map(Some(_))
  }

  def execute: Task[Boolean] = session.execute(statement).map(_.wasApplied)

}

object PreparedQuery {

  private[session] def apply[R: Reads](
    session: Session,
    statement: PreparedStatement,
    config: BoundStatement => BoundStatement
  ): PreparedQuery[R] =
    new PreparedQuery[R](session, config(statement.bind()))

}
