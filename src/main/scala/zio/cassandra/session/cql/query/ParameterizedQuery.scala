package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.BoundStatement
import shapeless.HList
import shapeless.ops.hlist.Prepend
import zio.Task
import zio.cassandra.session.cql.{ Binder, Reads }
import zio.cassandra.session.Session
import zio.stream.Stream

case class ParameterizedQuery[V <: HList: Binder, R: Reads] private (template: QueryTemplate[V, R], values: V) {
  def +(that: String): ParameterizedQuery[V, R]      = ParameterizedQuery[V, R](this.template + that, this.values)
  def as[R1: Reads]: ParameterizedQuery[V, R1]       = ParameterizedQuery[V, R1](template.as[R1], values)
  def select(session: Session): Stream[Throwable, R] =
    Stream.unwrap(template.prepare(session).map(_.applyProduct(values).select))

  def selectFirst(session: Session): Task[Option[R]]                             =
    template.prepare(session).flatMap(_.applyProduct(values).selectFirst)
  def execute(session: Session): Task[Boolean]                                   =
    template.prepare(session).map(_.applyProduct(values)).flatMap(_.execute)
  def config(config: BoundStatement => BoundStatement): ParameterizedQuery[V, R] =
    ParameterizedQuery[V, R](template.config(config), values)
  def stripMargin: ParameterizedQuery[V, R]                                      = ParameterizedQuery[V, R](this.template.stripMargin, values)

  def ++[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): ParameterizedQuery[Out, R] = concat(that)

  def concat[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
    prepend: Prepend.Aux[V, W, Out],
    binderForW: Binder[W],
    binderForOut: Binder[Out]
  ): ParameterizedQuery[Out, R] =
    ParameterizedQuery[Out, R](this.template ++ that.template, prepend(this.values, that.values))
}
