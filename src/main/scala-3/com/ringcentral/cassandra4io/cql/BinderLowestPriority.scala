package com.ringcentral.cassandra4io.cql

import shapeless3.deriving.*
import com.datastax.oss.driver.api.core.cql.BoundStatement

trait BinderLowestPriority:
  given binderGen[A <: Product](
    using pInst: K0.ProductInstances[Binder, A]
  ): Binder[A] = (statement, index, value) =>
    pInst.foldLeft(value)((statement, index))(
      [t] => (acc: (BoundStatement, Int), binder: Binder[t], value: t) =>
        binder.bind(acc._1, index, value)
    )
