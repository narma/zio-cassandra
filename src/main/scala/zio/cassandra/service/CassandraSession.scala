package zio.cassandra.service

import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metrics.Metrics

import zio._
import zio.stream.{ Stream, ZStream }

trait CassandraSession {
  def prepare(stmt: String): Task[PreparedStatement]

  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

  def execute(stmt: Statement[_]): Task[AsyncResultSet]

  def execute(query: String): Task[AsyncResultSet]

  def select(stmt: Statement[_]): Stream[Throwable, Row]

  def getMetrics: Task[Option[Metrics]]

  // short-cuts
  def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement] =
    bind(stmt, bindValues).map(_.setExecutionProfileName(profileName))

  def bindAndExecute(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[AsyncResultSet] =
    for {
      bound <- bind(stmt, bindValues)
      res   <- execute(bound.setExecutionProfileName(profileName))
    } yield res

  def bindAndExecute(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[AsyncResultSet] =
    for {
      bound <- bind(stmt, bindValues)
      res   <- execute(bound)
    } yield res

  def bindAndSelect(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Stream[Throwable, Row] =
    ZStream.fromEffect(bind(stmt, bindValues)).flatMap(select)

  def bindAndSelect(
    stmt: PreparedStatement,
    bindValues: Seq[AnyRef],
    profileName: String
  ): Stream[Throwable, Row] =
    ZStream.fromEffect(bind(stmt, bindValues, profileName)).flatMap(select)

  def bindAndSelectAll(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[Seq[Row]] =
    for {
      bound <- bind(stmt, bindValues)
      res   <- selectAll(bound)
    } yield res

  def bindAndSelectAll(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[Seq[Row]] =
    for {
      bound <- bind(stmt, bindValues, profileName)
      res   <- selectAll(bound)
    } yield res

  def executeBatch(seq: Seq[BoundStatement], batchType: DefaultBatchType): Task[AsyncResultSet] = {
    val batch = BatchStatement
      .builder(batchType)
      .addStatements(seq: _*)
      .build()
    execute(batch)
  }

  def selectOne(stmt: Statement[_]): Task[Option[Row]] =
    execute(stmt).map(rs => Option(rs.one()))

  def selectAll(stmt: Statement[_]): Task[Seq[Row]] =
    select(stmt).runCollect
}
