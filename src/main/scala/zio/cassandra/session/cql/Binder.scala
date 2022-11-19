package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.data.{ SettableByIndex, UdtValue }
import zio.cassandra.session.cql.codec.CellWrites

import scala.annotation.implicitNotFound

@implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
trait Binder[T] { self =>

  def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: T): S

  def nextIndex(index: Int): Int = index + 1

}

object Binder extends BinderLowerPriority {

  def apply[T](implicit binder: Binder[T]): Binder[T] = binder

  final implicit class UdtValueBinderOps(private val udtBinder: Binder[UdtValue]) extends AnyVal {

    /** This is necessary for UDT values as you are not allowed to safely create a UDT value, instead you use the
      * prepared statement's variable definitions to retrieve a UserDefinedType that can be used as a constructor for a
      * UdtValue
      *
      * @param f
      *   is a function that accepts the input value A along with a constructor that you use to build the UdtValue that
      *   gets sent to Cassandra
      * @tparam A
      * @return
      */
    def contramapUDT[A](f: (A, UserDefinedType) => UdtValue): Binder[A] = new Binder[A] {
      override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: A): S = {
        val udtValue = f(
          value,
          statement.getType(index).asInstanceOf[UserDefinedType]
        )
        udtBinder.bind(statement, index, udtValue)
      }
    }
  }

  final implicit class BinderOps[T](private val binder: Binder[T]) extends AnyVal {

    def contramap[U](f: U => T): Binder[U] = new Binder[U] {
      override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: U): S =
        binder.bind(statement, index, f(value))
    }

  }

}

trait BinderLowerPriority {

  /** This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
    * @param writes
    *   is evidence that a typeclass instance of CellWrites exists for A
    * @tparam T
    *   is the Scala datatype that needs to be written to Cassandra
    * @return
    */

  implicit def deriveBinderFromCellWrites[T](implicit writes: CellWrites[T]): Binder[T] = new Binder[T] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: T): S = {
      val protocol = statement.protocolVersion()
      val dataType = statement.getType(index)
      val bytes    = writes.write(value, protocol, dataType)
      statement.setBytesUnsafe(index, bytes)
    }
  }

}
