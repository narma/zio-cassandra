package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.{ BoundStatement, Row }
import shapeless.{ ::, HList, HNil, ProductArgs }
import zio.cassandra.session.cql.query.{ ParameterizedQuery, QueryTemplate }

import scala.annotation.{ nowarn, tailrec }

class CqlTemplateStringInterpolator(ctx: StringContext) extends ProductArgs {
  import CqlTemplateStringInterpolator._
  @nowarn("msg=is never used")
  def applyProduct[P <: HList, V <: HList](params: P)(implicit
    bb: BindableBuilder.Aux[P, V]
  ): QueryTemplate[V, Row] = {
    implicit val binder: Binder[V] = bb.binder
    QueryTemplate[V, Row](ctx.parts.mkString("?"), identity)
  }
}

object CqlTemplateStringInterpolator {

  trait BindableBuilder[P] {
    type Repr <: HList
    def binder: Binder[Repr]
  }

  object BindableBuilder {
    type Aux[P, Repr0] = BindableBuilder[P] { type Repr = Repr0 }
    def apply[P](implicit builder: BindableBuilder[P]): BindableBuilder.Aux[P, builder.Repr] = builder
    implicit def hNilBindableBuilder: BindableBuilder.Aux[HNil, HNil]                        = new BindableBuilder[HNil] {
      override type Repr = HNil
      override def binder: Binder[HNil] = Binder[HNil]
    }
    implicit def hConsBindableBuilder[PH <: Put[_], T: Binder, PT <: HList, RT <: HList](implicit
      f: BindableBuilder.Aux[PT, RT]
    ): BindableBuilder.Aux[Put[T] :: PT, T :: RT] = new BindableBuilder[Put[T] :: PT] {
      override type Repr = T :: RT
      override def binder: Binder[T :: RT] = {
        implicit val tBinder: Binder[RT] = f.binder
        Binder[T :: RT]
      }
    }
  }
}

/** BoundValue is used to capture the value inside the cql interpolated string along with evidence of its Binder so that
  * a ParameterizedQuery can be built and the values can be bound to the BoundStatement internally
  */
private[cql] final case class BoundValue[A](value: A, ev: Binder[A])

object BoundValue {
  // This implicit conversion automatically captures the value and evidence of the Binder in a cql interpolated string
  implicit def aToBoundValue[A](a: A)(implicit ev: Binder[A]): BoundValue[A] =
    BoundValue(a, ev)
}

class CqlStringInterpolator(ctx: StringContext) {
  @tailrec
  private def replaceValuesWithQuestionMark(
    strings: Iterator[String],
    expressions: Iterator[BoundValue[_]],
    acc: String
  ): String =
    if (strings.hasNext && expressions.hasNext) {
      val str = strings.next()
      val _   = expressions.next()
      replaceValuesWithQuestionMark(
        strings = strings,
        expressions = expressions,
        acc = acc + s"$str?"
      )
    } else if (strings.hasNext && !expressions.hasNext) {
      val str = strings.next()
      replaceValuesWithQuestionMark(
        strings = strings,
        expressions = expressions,
        acc + str
      )
    } else acc

  def apply(values: BoundValue[_]*): SimpleQuery[Row] = {
    val queryWithQuestionMark                                     = replaceValuesWithQuestionMark(ctx.parts.iterator, values.iterator, "")
    val assignValuesToStatement: BoundStatement => BoundStatement = { in: BoundStatement =>
      val (configuredBoundStatement, _) =
        values.foldLeft((in, 0)) { case ((current, index), bv: BoundValue[a]) =>
          val binder: Binder[a] = bv.ev
          val value: a          = bv.value
          binder.bind(current, index, value)
        }
      configuredBoundStatement
    }
    ParameterizedQuery(QueryTemplate[HNil, Row](queryWithQuestionMark, assignValuesToStatement), HNil)
  }
}
