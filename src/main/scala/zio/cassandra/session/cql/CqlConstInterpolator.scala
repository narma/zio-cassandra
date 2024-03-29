package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.Row
import zio.cassandra.session.cql.query.QueryTemplate

/** Provides a way to lift arbitrary strings into CQL so you can parameterize on values that are not valid CQL
  * parameters <br> Please note that this is not escaped so do not use this with user-supplied input for your
  * application (only use cqlConst for input that you as the application author control)
  */
class CqlConstInterpolator(ctx: StringContext) {
  def apply(args: Any*): QueryTemplate[Row] =
    QueryTemplate(ctx.s(args: _*), identity)
}
