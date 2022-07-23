package zio.cassandra.session

import shapeless.HNil
import zio.cassandra.session.cql.query.ParameterizedQuery

package object cql {

  type SimpleQuery[Output] = ParameterizedQuery[HNil, Output]

  final implicit class CqlStringContext(private val ctx: StringContext) {
    val cqlt     = new CqlTemplateStringInterpolator(ctx)
    val cql      = new CqlStringInterpolator(ctx)
    val cqlConst = new CqlConstInterpolator(ctx)
  }

  object unsafe {

    /** lifting a value is useful when you want to inject a value into cql query as is without escaping (similar to
      * cqlConst interpolator, but on a lower lever). <br> Please only use `lift()` for input that you as the
      * application author control. Example:
      * {{{
      * import unsafe._
      *
      * private val tableName = "my_table"
      * def selectById(ids: Seq[Long) = cql"select id from ${lift(tableName)} where id in $ids".as[Int]
      * }}}
      * instead of
      * {{{
      * private val tableName = "my_table"
      * def selectById(ids: Seq[Long) = (cqlConst"select id from $tableName" ++  cql"where id in $ids").as[Int]
      * }}}
      */
    def lift(value: Any): LiftedValue = LiftedValue(value)

  }

}
