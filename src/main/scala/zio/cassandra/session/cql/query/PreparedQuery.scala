package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.{ BoundStatement, PreparedStatement }
import shapeless.{ HList, ProductArgs }
import zio.cassandra.session.cql.Binder
import zio.cassandra.session.Session
import zio.cassandra.session.cql.codec.Reads

class PreparedQuery[V <: HList: Binder, R: Reads] private[session] (
  session: Session,
  statement: PreparedStatement,
  config: BoundStatement => BoundStatement
) extends ProductArgs {
  def applyProduct(values: V) = new Query[R](session, Binder[V].bind(config(statement.bind()), 0, values))
}
