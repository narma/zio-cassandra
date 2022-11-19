package zio.cassandra.session.cql

sealed trait CqlValue

object CqlValue {

  // This implicit conversion automatically captures the value and evidence of the Binder in a cql interpolated string
  implicit def aToBoundValue[A](a: A)(implicit ev: Binder[A]): BoundValue[A] =
    BoundValue(a, ev)

}

/** BoundValue is used to capture the value inside the cql interpolated string along with evidence of its Binder so that
  * a ParameterizedQuery can be built and the values can be bound to the BoundStatement internally
  */
final case class BoundValue[A](value: A, ev: Binder[A]) extends CqlValue

/** LiftedValue is useful when you want to inject a value into cql query as is without escaping (similar to
  * [[zio.cassandra.session.cql.CqlConstInterpolator]], but on a lower lever). <br> Please only use LiftedValue for
  * input that you as the application author control.
  */
final case class LiftedValue(value: Any) extends CqlValue {

  override def toString: String = value.toString // to keep things simple with cqlConst

}
