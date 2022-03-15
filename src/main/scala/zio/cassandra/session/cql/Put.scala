package zio.cassandra.session.cql

import scala.annotation.nowarn

trait Put[T]
object Put {
  @nowarn("msg=is never used")
  def apply[T: Binder]: Put[T] = new Put[T] {}
}
