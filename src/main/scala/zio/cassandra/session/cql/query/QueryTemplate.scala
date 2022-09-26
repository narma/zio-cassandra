package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.BoundStatement
import shapeless.HList
import shapeless.ops.hlist.Prepend
import zio.cassandra.session.Session
import zio.cassandra.session.cql.Binder
import zio.cassandra.session.cql.codec.Reads
import zio.{ RIO, ZIO }

case class QueryTemplate[V <: HList, R] private[cql] (
  query: String,
  config: BoundStatement => BoundStatement
)(implicit val binder: Binder[V], val reads: Reads[R]) {
  def +(that: String): QueryTemplate[V, R] = QueryTemplate[V, R](this.query + that, config)
  def as[R1: Reads]: QueryTemplate[V, R1]  = QueryTemplate[V, R1](query, config)

  def prepare: RIO[Session, PreparedQuery[V, R]] = ZIO.serviceWithZIO(_.prepare(this))

  def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R] =
    QueryTemplate[V, R](this.query, this.config andThen config)

  def stripMargin: QueryTemplate[V, R] = QueryTemplate[V, R](this.query.stripMargin, this.config)

  def ++[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): QueryTemplate[Out, R] = concat(that)

  def concat[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): QueryTemplate[Out, R] = QueryTemplate[Out, R](
    this.query + that.query,
    statement => (this.config andThen that.config)(statement)
  )

}
