package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BatchStatementBuilder, BatchType }
import zio.cassandra.session.Session
import zio.{ RIO, ZIO }
import com.datastax.oss.driver.api.core.cql.BoundStatement
import zio.Chunk

class Batch(batchStatementBuilder: BatchStatementBuilder) {

  def add(queries: Seq[BoundStatement]): this.type = {
    batchStatementBuilder.addStatements(queries: _*)
    this
  }

  def add(queries: QueryTemplate[_]*): RIO[Session, Batch] =
    Chunk.fromIterable(queries).mapZIOPar(_.prepare).map { sts =>
      batchStatementBuilder.addStatements(sts: _*)
      this
    }

  def build: BatchStatement = batchStatementBuilder.build()

  def execute: RIO[Session, Boolean] =
    ZIO.serviceWithZIO(_.execute(this))

  def config(config: BatchStatementBuilder => BatchStatementBuilder): Batch =
    new Batch(config(batchStatementBuilder))
}

object Batch {
  def logged   = new Batch(new BatchStatementBuilder(BatchType.LOGGED))
  def unlogged = new Batch(new BatchStatementBuilder(BatchType.UNLOGGED))
}
