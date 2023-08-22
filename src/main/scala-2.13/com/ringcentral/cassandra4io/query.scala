package com.ringcentral.cassandra4io

import cats.{Functor, Monad}
import cats.data.OptionT
import cats.syntax.functor._
import cats.syntax.flatMap._
import fs2.Stream
import shapeless._
import com.ringcentral.cassandra4io.cql._
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import shapeless.ops.hlist.Prepend

object query {
  case class QueryTemplate[V <: HList: Binder, R: Reads] private[query] (
    query: String,
    config: BoundStatement => BoundStatement
  ) {
    def +(that: String): QueryTemplate[V, R] = QueryTemplate[V, R](this.query + that, config)

    def ++[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): QueryTemplate[Out, R] = concat(that)

    def concat[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): QueryTemplate[Out, R] = QueryTemplate[Out, R](
      this.query + that.query,
      statement => (this.config andThen that.config)(statement)
    )

    def as[R1: Reads]: QueryTemplate[V, R1] = QueryTemplate[V, R1](query, config)

    def prepare[F[_]: Functor](session: CassandraSession[F]): F[PreparedQuery[F, V, R]] =
      session.prepare(query).map(new PreparedQuery(session, _, config))

    def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R] =
      QueryTemplate[V, R](this.query, this.config andThen config)

    def stripMargin: QueryTemplate[V, R] = QueryTemplate[V, R](this.query.stripMargin, this.config)
  }

  case class ParameterizedQuery[V <: HList: Binder, R: Reads] private (template: QueryTemplate[V, R], values: V) {
    def +(that: String): ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template + that, this.values)

    def ++[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): ParameterizedQuery[Out, R] = concat(that)

    def concat[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): ParameterizedQuery[Out, R] =
      ParameterizedQuery[Out, R](this.template ++ that.template, prepend(this.values, that.values))

    def as[R1: Reads]: ParameterizedQuery[V, R1] = ParameterizedQuery[V, R1](template.as[R1], values)

    def select[F[_]: Functor](session: CassandraSession[F]): Stream[F, R] =
      Stream.force(template.prepare(session).map(_.applyProduct(values).select))

    def selectFirst[F[_]: Monad](session: CassandraSession[F]): F[Option[R]] =
      template.prepare(session).flatMap(_.applyProduct(values).selectFirst)

    def execute[F[_]: Monad](session: CassandraSession[F]): F[Boolean] =
      template.prepare(session).map(_.applyProduct(values)).flatMap(_.execute)

    def config(config: BoundStatement => BoundStatement): ParameterizedQuery[V, R] =
      ParameterizedQuery[V, R](template.config(config), values)

    def stripMargin: ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template.stripMargin, values)
  }

  class PreparedQuery[F[_]: Functor, V <: HList: Binder, R: Reads] private[query] (
    session: CassandraSession[F],
    statement: PreparedStatement,
    config: BoundStatement => BoundStatement
  ) extends ProductArgs {
    def applyProduct(values: V) = new Query[F, R](session, Binder[V].bind(config(statement.bind()), 0, values)._1)
  }

  class Query[F[_]: Functor, R: Reads] private[query] (
    session: CassandraSession[F],
    private[cassandra4io] val statement: BoundStatement
  ) {
    def config(statement: BoundStatement => BoundStatement) = new Query[F, R](session, statement(this.statement))
    def select: Stream[F, R]                                = session.select(statement).map(Reads[R].read(_, 0))
    def selectFirst: F[Option[R]]                           = OptionT(session.selectFirst(statement)).map(Reads[R].read(_, 0)).value
    def execute: F[Boolean]                                 = session.execute(statement).map(_.wasApplied)
  }
}