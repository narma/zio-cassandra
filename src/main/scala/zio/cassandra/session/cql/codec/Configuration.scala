package zio.cassandra.session.cql.codec

import java.util.regex.Pattern

final case class Configuration(transformFieldNames: String => String)

object Configuration {

  implicit lazy val defaultConfiguration: Configuration = Configuration(snakeCaseTransformation)

  private val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
  private val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")

  val snakeCaseTransformation: String => String = s => {
    val partial = basePattern.matcher(s).replaceAll("$1_$2")
    swapPattern.matcher(partial).replaceAll("$1_$2").toLowerCase
  }

}
