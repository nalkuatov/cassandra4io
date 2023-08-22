package com.ringcentral.cassandra4io

import shapeless._
import com.ringcentral.cassandra4io.cql._
import com.ringcentral.cassandra4io.query._
import com.datastax.oss.driver.api.core.cql.{Row, BoundStatement}
import scala.annotation.tailrec
import com.ringcentral.cassandra4io.query

object interpolator {

  class CqlTemplateStringInterpolator(ctx: StringContext) extends ProductArgs {
    import CqlTemplateStringInterpolator._
    def applyProduct[P <: HList, V <: HList](params: P)(implicit
      bb: BindableBuilder.Aux[P, V]
    ): QueryTemplate[V, Row] = {
      implicit val binder: Binder[V] = bb.binder
      QueryTemplate[V, Row](ctx.parts.mkString("?"), identity)
    }
  }

  object CqlTemplateStringInterpolator {

    trait BindableBuilder[P] {
      type Repr <: HList
      def binder: Binder[Repr]
    }

    private object BindableBuilder {
      type Aux[P, Repr0] = BindableBuilder[P] { type Repr = Repr0 }
      def apply[P](implicit builder: BindableBuilder[P]): BindableBuilder.Aux[P, builder.Repr] = builder
      implicit def hNilBindableBuilder: BindableBuilder.Aux[HNil, HNil]                        = new BindableBuilder[HNil] {
        override type Repr = HNil
        override def binder: Binder[HNil] = Binder[HNil]
      }
      implicit def hConsBindableBuilder[PH <: Put[_], T: Binder, PT <: HList, RT <: HList](implicit
        f: BindableBuilder.Aux[PT, RT]
      ): BindableBuilder.Aux[Put[T] :: PT, T :: RT]                                            = new BindableBuilder[Put[T] :: PT] {
        override type Repr = T :: RT
        override def binder: Binder[T :: RT] = {
          implicit val tBinder: Binder[RT] = f.binder
          Binder[T :: RT]
        }
      }
    }
  }

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

    def apply(values: BoundValue[_]*): ParameterizedQuery[HNil, Row] = {
      val queryWithQuestionMark = replaceValuesWithQuestionMark(ctx.parts.iterator, values.iterator, "")
      val assignValuesToStatement: BoundStatement => BoundStatement = { in: BoundStatement =>
        val (configuredBoundStatement, _) =
          values.foldLeft((in, 0)) { case ((current, index), bv: BoundValue[a]) =>
            val binder: Binder[a] = bv.ev
            val value: a          = bv.value
            binder.bind(current, index, value)
          }
        configuredBoundStatement
      }
      ParameterizedQuery(QueryTemplate[HNil, Row](queryWithQuestionMark, assignValuesToStatement), HNil)
    }
  }

  /**
   * Provides a way to lift arbitrary strings into CQL so you can parameterize on values that are not valid CQL parameters
   * Please note that this is not escaped so do not use this with user-supplied input for your application (only use
   * cqlConst for input that you as the application author control)
   */
  class CqlConstInterpolator(ctx: StringContext) {
    def apply(args: Any*): ParameterizedQuery[HNil, Row] =
      ParameterizedQuery(QueryTemplate(ctx.s(args: _*), identity), HNil)
  }

  implicit class CqlStringContext(val ctx: StringContext) extends AnyVal {
    def cqlt     = new CqlTemplateStringInterpolator(ctx)
    def cql      = new CqlStringInterpolator(ctx)
    def cqlConst = new CqlConstInterpolator(ctx)
  }

}
