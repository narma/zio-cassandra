package zio.cassandra.session.cql.query

import zio.cassandra.session.Session
import zio.cassandra.session.cql.codec.Reads
import zio.stream.ZStream
import zio.{ RIO, ZIO }
import com.datastax.oss.driver.api.core.cql.BoundStatement

case class QueryTemplate[R] private[cql] (
  query: String,
  config: BoundStatement => BoundStatement
)(implicit val reads: Reads[R]) {
  def +(that: String): QueryTemplate[R] = QueryTemplate[R](this.query + that, config)
  def as[R1: Reads]: QueryTemplate[R1]  = QueryTemplate[R1](query, config)

  def prepare: RIO[Session, BoundStatement] = ZIO.serviceWithZIO(_.prepare(this))

  def select: ZStream[Session, Throwable, R] = ZStream.serviceWithStream(_.select(this))

  def selectFirst: RIO[Session, Option[R]] = ZIO.serviceWithZIO(_.selectFirst(this))

  def execute: RIO[Session, Boolean] = ZIO.serviceWithZIO(_.execute(this))

  def config(config: BoundStatement => BoundStatement): QueryTemplate[R] =
    QueryTemplate[R](this.query, this.config andThen config)

  def stripMargin: QueryTemplate[R] = QueryTemplate[R](this.query.stripMargin, this.config)

  def ++(that: QueryTemplate[R]): QueryTemplate[R] = concat(that)

  def concat(that: QueryTemplate[R]): QueryTemplate[R] =
    QueryTemplate[R](
      this.query + that.query,
      statement => (this.config andThen that.config)(statement)
    )
}
