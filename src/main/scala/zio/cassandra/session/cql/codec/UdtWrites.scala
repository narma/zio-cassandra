package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue

/** Serializer created specifically for UDT values.<br> Note that this reader can be (is) recursive, so each instance of
  * [[zio.cassandra.session.cql.codec.UdtWrites]] can be seen as an instance of
  * [[zio.cassandra.session.cql.codec.CellWrites]], while at the same time it might need
  * [[zio.cassandra.session.cql.codec.CellWrites]] instances to work.<br>
  *
  * The reason why it needs `structure: UdtValue` param is because we cannot create an `UdtValue` out of thin air, we
  * can only fill it with values. The only one who can properly create an UdtValue is java driver, so it's up to a
  * caller to ask the driver to create a dummy UdtValue, which we'll use.
  */
trait UdtWrites[T] {

  def write(t: T, structure: UdtValue): UdtValue

}

object UdtWrites extends UdtWritesInstances1 {

  def apply[T](implicit writes: UdtWrites[T]): UdtWrites[T] = writes

  def instance[T](f: (T, UdtValue) => UdtValue): UdtWrites[T] =
    (t: T, udtValue: UdtValue) => f(t, udtValue)

  final implicit class UdtWritesOps[T](private val writes: UdtWrites[T]) extends AnyVal {

    def contramap[V](f: V => T): UdtWrites[V] = instance((t, structure) => writes.write(f(t), structure))

  }

}
