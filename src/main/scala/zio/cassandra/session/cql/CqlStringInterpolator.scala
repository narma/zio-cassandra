package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.{ BoundStatement, Row }
import shapeless.ops.hlist.ToList
import shapeless.{ ::, HList, HNil, ProductArgs }
import zio.cassandra.session.cql.query.{ ParameterizedQuery, QueryTemplate }

import scala.annotation.tailrec

sealed abstract class CqlStringInterpolatorBase {

  @tailrec
  final protected def assembleQuery(
    strings: Iterator[String],
    expressions: Iterator[HasPlaceholder],
    acc: String
  ): String =
    if (strings.hasNext && expressions.hasNext) {
      val str  = strings.next()
      val expr = expressions.next()

      assembleQuery(
        strings = strings,
        expressions = expressions,
        acc = acc + s"$str${expr.placeholder}"
      )
    } else if (strings.hasNext && !expressions.hasNext) {
      val str = strings.next()
      assembleQuery(
        strings = strings,
        expressions = expressions,
        acc + str
      )
    } else acc

}

class CqlTemplateStringInterpolator(ctx: StringContext) extends CqlStringInterpolatorBase with ProductArgs {
  import CqlTemplateStringInterpolator._
  def applyProduct[P <: HList, V <: HList](
    params: P
  )(implicit bb: BindableBuilder.Aux[P, V], toList: ToList[P, CqltValue]): QueryTemplate[V, Row] = {
    implicit val binder: Binder[V] = bb.binder
    val queryWithQuestionMarks     = assembleQuery(ctx.parts.iterator, params.toList[CqltValue].iterator, "")
    QueryTemplate[V, Row](queryWithQuestionMarks, identity)
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

    implicit def hNilBindableBuilder: BindableBuilder.Aux[HNil, HNil] = new BindableBuilder[HNil] {
      override type Repr = HNil
      override def binder: Binder[HNil] = Binder[HNil]
    }

    implicit def hConsPutBindableBuilder[T: Binder, PT <: HList, RT <: HList](implicit
      f: BindableBuilder.Aux[PT, RT]
    ): BindableBuilder.Aux[Put[T] :: PT, T :: RT] = new BindableBuilder[Put[T] :: PT] {
      override type Repr = T :: RT
      override def binder: Binder[T :: RT] = {
        implicit val tBinder: Binder[RT] = f.binder
        Binder[T :: RT]
      }
    }

    implicit def hConsLiftedValueBindableBuilder[PT <: HList, RT <: HList](implicit
      f: BindableBuilder.Aux[PT, RT]
    ): BindableBuilder.Aux[LiftedValue :: PT, RT] = new BindableBuilder[LiftedValue :: PT] {
      override type Repr = RT
      override def binder: Binder[RT] = f.binder
    }

  }
}

final class CqlStringInterpolator(ctx: StringContext) extends CqlStringInterpolatorBase {

  def apply(values: CqlValue*): SimpleQuery[Row] = {
    val queryWithQuestionMarks = assembleQuery(ctx.parts.iterator, values.iterator, "")

    val assignValuesToStatement: BoundStatement => BoundStatement = { in: BoundStatement =>
      val (configuredBoundStatement, _) =
        values.foldLeft((in, 0)) { case ((current, index), qv: CqlValue) =>
          qv match {
            case _: LiftedValue            => (current, index)
            case BoundValue(value, binder) =>
              val statement = binder.bind(current, index, value)
              val nextIndex = binder.nextIndex(index)
              (statement, nextIndex)
          }
        }
      configuredBoundStatement
    }
    ParameterizedQuery(QueryTemplate[HNil, Row](queryWithQuestionMarks, assignValuesToStatement), HNil)
  }
}
