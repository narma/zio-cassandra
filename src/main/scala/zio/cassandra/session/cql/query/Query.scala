package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.BoundStatement
import zio.{ Task, ZIO }
import zio.stream.Stream
import zio.cassandra.session.cql.Reads
import zio.cassandra.session.Session

class Query[R: Reads] private[cql] (
  session: Session,
  private[cql] val statement: BoundStatement
) {
  def config(statement: BoundStatement => BoundStatement) = new Query[R](session, statement(this.statement))

  def select: Stream[Throwable, R]                        = session.select(statement).mapChunksM { chunk =>
    chunk.mapM(Reads[R].read(_, 0))
  }

  def selectFirst: Task[Option[R]]                        = session.selectFirst(statement).flatMap {
    case None      => ZIO.none
    case Some(row) =>
      Reads[R].read(row, 0).map(Some(_))
  }

  def execute: Task[Boolean]                              = session.execute(statement).map(_.wasApplied)
}
