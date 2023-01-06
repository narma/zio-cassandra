package zio.cassandra.session.cql

import zio.cassandra.session.cql.query.QueryTemplate
import com.datastax.oss.driver.api.core.cql.Row

sealed abstract class CqlStringInterpolatorBase {

  final def assembleQuery(
    strings: Iterator[String],
    expressions: Iterator[CqlValue],
    acc: String
  ): String = {
    var temp: String = acc

    while (strings.hasNext) {
      val str = strings.next()
      if (expressions.hasNext) {
        val expr = expressions.next()

        val placeholder = expr match {
          case l: LiftedValue   => l.toString
          case _: BoundValue[_] => "?"
        }
        temp += s"$str$placeholder"
      } else
        temp += str
    }

    temp
  }
}

final class CqlStringInterpolator(ctx: StringContext) extends CqlStringInterpolatorBase {

  def apply(values: CqlValue*): QueryTemplate[Row] = {
    val queryWithQuestionMarks = assembleQuery(ctx.parts.iterator, values.iterator, "")

    QueryTemplate[Row](queryWithQuestionMarks, Query.bind(_, values))
  }
}
