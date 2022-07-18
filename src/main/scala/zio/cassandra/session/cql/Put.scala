package zio.cassandra.session.cql

trait Put[T]
object Put {
  def apply[T: Binder]: Put[T] = new Put[T] {}
}
