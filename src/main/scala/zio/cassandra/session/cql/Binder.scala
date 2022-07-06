package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.data.{ SettableByIndex, UdtValue }
import shapeless.{ ::, HList, HNil, Widen }
import zio.cassandra.session.cql.codec.RawWrites

import scala.annotation.implicitNotFound

@implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
trait Binder[T] { self =>

  def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: T): S

  def nextIndex(index: Int): Int = index + 1

}

object Binder extends BinderLowerPriority with BinderLowestPriority {

  def apply[T](implicit binder: Binder[T]): Binder[T] = binder

  implicit def widenBinder[T: Binder, X <: T](implicit wd: Widen.Aux[X, T]): Binder[X] = new Binder[X] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: X): S =
      Binder[T].bind(statement, index, wd.apply(value))
  }

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
    *   is evidence that a typeclass instance of RawWrites exists for A
    * @tparam T
    *   is the Scala datatype that needs to be written to Cassandra
    * @return
    */

  implicit def deriveBinderFromRawWrites[T](implicit writes: RawWrites[T]): Binder[T] = new Binder[T] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: T): S = {
      val protocol = statement.protocolVersion()
      val dataType = statement.getType(index)
      val bytes    = writes.write(value, protocol, dataType)
      statement.setBytesUnsafe(index, bytes)
    }
  }

}

trait BinderLowestPriority {
  implicit val hNilBinder: Binder[HNil] = new Binder[HNil] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: HNil): S = statement
  }

  implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: H :: T): S = {
      val applied   = Binder[H].bind(statement, index, value.head)
      val nextIndex = Binder[H].nextIndex(index)
      Binder[T].bind(applied, nextIndex, value.tail)
    }
  }
}
