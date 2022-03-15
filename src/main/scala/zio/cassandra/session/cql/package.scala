package zio.cassandra.session

import shapeless.HNil
import zio.cassandra.session.cql.query.ParameterizedQuery

package object cql {

  type SimpleQuery[Output] = ParameterizedQuery[HNil, Output]

  final implicit class CqlStringContext(private val ctx: StringContext) {
    val cqlt     = new CqlTemplateStringInterpolator(ctx)
    val cql      = new CqlStringInterpolator(ctx)
    val cqlConst = new CqlConstInterpolator(ctx)
  }
}
