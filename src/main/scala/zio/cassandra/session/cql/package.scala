package zio.cassandra.session

package object cql {
  final implicit class CqlStringContext(private val ctx: StringContext) {
    val cqlt = new CqlTemplateStringInterpolator(ctx)
    val cql  = new CqlStringInterpolator(ctx)
  }

  type FromUdtValue[Scala] = udt.FromUdtValue[Scala]
  val FromUdtValue: udt.FromUdtValue.type = udt.FromUdtValue

  type ToUdtValue[-Scala] = udt.ToUdtValue[Scala]
  val ToUdtValue: udt.ToUdtValue.type = udt.ToUdtValue
}
