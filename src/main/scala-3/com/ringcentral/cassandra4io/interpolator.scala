package com.ringcentral.cassandra4io

import com.ringcentral.cassandra4io.cql.*
import com.ringcentral.cassandra4io.query.*
import com.datastax.oss.driver.api.core.cql.Row
import scala.annotation.tailrec
import com.datastax.oss.driver.api.core.cql.BoundStatement

object interpolator {

  class CqlStringInterpolator(ctx: StringContext) {
    @tailrec
    private def replaceValuesWithQuestionMark(
      strings: Iterator[String],
      expressions: Iterator[BoundValue[_]],
      acc: String
    ): String =
      if (strings.hasNext && expressions.hasNext) {
        val str = strings.next()
        val _   = expressions.next()
        replaceValuesWithQuestionMark(
          strings = strings,
          expressions = expressions,
          acc = acc + s"$str?"
        )
      } else if (strings.hasNext && !expressions.hasNext) {
        val str = strings.next()
        replaceValuesWithQuestionMark(
          strings = strings,
          expressions = expressions,
          acc + str
        )
      } else acc

    def apply(values: BoundValue[_]*): ParameterizedQuery[EmptyTuple, Row] = {
      val queryWithQuestionMark = replaceValuesWithQuestionMark(ctx.parts.iterator, values.iterator, "")
      val assignValuesToStatement: BoundStatement => BoundStatement = { in =>
        val (configuredBoundStatement, _) =
          values.foldLeft((in, 0)) { case ((current, index), bv: BoundValue[a]) =>
            val binder: Binder[a] = bv.ev
            val value: a          = bv.value
            binder.bind(current, index, value)
          }
        configuredBoundStatement
      }
      ParameterizedQuery(QueryTemplate[EmptyTuple, Row](queryWithQuestionMark, assignValuesToStatement), EmptyTuple)
    }
  }

  /**
   * Provides a way to lift arbitrary strings into CQL so you can parameterize on values that are not valid CQL parameters
   * Please note that this is not escaped so do not use this with user-supplied input for your application (only use
   * cqlConst for input that you as the application author control)
   */
  class CqlConstInterpolator(ctx: StringContext) {
    def apply(args: Any*): ParameterizedQuery[EmptyTuple, Row] =
      ParameterizedQuery(QueryTemplate[EmptyTuple, Row](ctx.s(args: _*), identity), EmptyTuple)
  }

  implicit class CqlStringContext(val ctx: StringContext) extends AnyVal {
    def cql      = new CqlStringInterpolator(ctx)
    def cqlConst = new CqlConstInterpolator(ctx)
  }

}
