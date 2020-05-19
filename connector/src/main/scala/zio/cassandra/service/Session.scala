package zio.cassandra.service

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.cql._
import zio._
import zio.stream.Stream

trait Session {
  def prepare(stmt: String): Task[PreparedStatement]

  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, ReactiveRow]

  // short-cuts
  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement] =
    bind(stmt, bindValues).map(_.setExecutionProfileName(profileName))

  def selectOne(stmt: Statement[_]): Task[Option[Row]] =
    execute(stmt).map(rs => Option(rs.one()))

  def selectAll(stmt: Statement[_]): Task[Seq[Row]] =
    select(stmt).runCollect
}
