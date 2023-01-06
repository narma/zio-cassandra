package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.BoundStatement

object Query {
  def bind(bs: BoundStatement, values: Seq[CqlValue]) = {
    val (configuredBoundStatement, _) =
      values.foldLeft((bs, 0)) { case ((current, index), qv: CqlValue) =>
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
}
