package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.BoundStatement
import shapeless.HList
import shapeless.ops.hlist.Prepend
import zio.cassandra.session.Session
import zio.cassandra.session.cql.{Binder}
import zio.cassandra.session.cql.codec.Reads
import zio.{Has, RIO, ZIO}

import scala.annotation.nowarn

case class QueryTemplate[V <: HList: Binder, R: Reads] private[cql] (
  query: String,
  config: BoundStatement => BoundStatement
) {
  def +(that: String): QueryTemplate[V, R] = QueryTemplate[V, R](this.query + that, config)
  def as[R1: Reads]: QueryTemplate[V, R1]  = QueryTemplate[V, R1](query, config)

  def prepare: RIO[Has[Session], PreparedQuery[V, R]] =
    ZIO.accessM[Has[Session]] { session =>
      session.get.prepare(query).map(new PreparedQuery(session.get, _, config))
    }

  def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R] =
    QueryTemplate[V, R](this.query, this.config andThen config)

  def stripMargin: QueryTemplate[V, R]                                      = QueryTemplate[V, R](this.query.stripMargin, this.config)

  def ++[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): QueryTemplate[Out, R] = concat(that)

  @nowarn("msg=is never used")
  def concat[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): QueryTemplate[Out, R] = QueryTemplate[Out, R](
    this.query + that.query,
    statement => (this.config andThen that.config)(statement)
  )

}
