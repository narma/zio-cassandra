package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless.HNil
import zio.cassandra.session.cql.query.{ ParameterizedQuery, QueryTemplate }

/** Provides a way to lift arbitrary strings into CQL so you can parameterize on values that are not valid CQL
  * parameters Please note that this is not escaped so do not use this with user-supplied input for your application
  * (only use cqlConst for input that you as the application author control)
  */
class CqlConstInterpolator(ctx: StringContext) {
  def apply(args: Any*): ParameterizedQuery[HNil, Row] =
    ParameterizedQuery(QueryTemplate(ctx.s(args: _*), identity), HNil)
}
