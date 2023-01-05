package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import zio.cassandra.session.Session
import zio.cassandra.session.cql.codec.Reads
import zio.stream.Stream
import zio.{ Task, ZIO }
import zio.cassandra.session.cql.CqlValue
import com.datastax.oss.driver.api.core.cql.BoundStatement
import zio.cassandra.session.cql.Query.bind

class PreparedQuery[R: Reads] private[cql] (
  session: Session,
  val statement: PreparedStatement,
  val bound: BoundStatement
) {
  def apply(values: CqlValue*) = new PreparedQuery[R](session, statement, bind(statement.bind(), values))

  def config(fn: BoundStatement => BoundStatement) = new PreparedQuery[R](session, statement, fn(this.bound))

  def select: Stream[Throwable, R] = session.select(bound).mapChunksZIO { chunk =>
    chunk.mapZIO(row => ZIO.attempt(Reads[R].read(row)))
  }

  def selectFirst: Task[Option[R]] = session.selectFirst(bound).flatMap {
    case None      => ZIO.none
    case Some(row) => ZIO.attempt(Reads[R].read(row)).map(Some(_))
  }

  def execute: Task[Boolean] = session.execute(bound).map(_.wasApplied)
}

object PreparedQuery {

  private[session] def apply[R: Reads](
    session: Session,
    statement: PreparedStatement,
    config: BoundStatement => BoundStatement
  ): PreparedQuery[R] =
    new PreparedQuery[R](session, statement, config(statement.bind()))

}
