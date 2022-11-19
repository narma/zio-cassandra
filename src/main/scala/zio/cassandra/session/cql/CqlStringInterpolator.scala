package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.{ BoundStatement, Row }
import zio.cassandra.session.cql.query.QueryTemplate

import scala.annotation.tailrec

sealed abstract class CqlStringInterpolatorBase {

  @tailrec
  final protected def assembleQuery(
    strings: Iterator[String],
    expressions: Iterator[CqlValue],
    acc: String
  ): String =
    if (strings.hasNext && expressions.hasNext) {
      val str  = strings.next()
      val expr = expressions.next()

      val placeholder = expr match {
        case l: LiftedValue   => l.toString
        case _: BoundValue[_] => "?"
      }

      assembleQuery(
        strings = strings,
        expressions = expressions,
        acc = acc + s"$str$placeholder"
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

final class CqlStringInterpolator(ctx: StringContext) extends CqlStringInterpolatorBase {

  def apply(values: CqlValue*): QueryTemplate[Row] = {
    val queryWithQuestionMarks = assembleQuery(ctx.parts.iterator, values.iterator, "")

    val assignValuesToStatement: BoundStatement => BoundStatement = { (in: BoundStatement) =>
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
    QueryTemplate[Row](queryWithQuestionMarks, assignValuesToStatement)
  }

}
