package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless3.deriving.*

trait ReadsLowestPriority {
  inline given reads[A](
    using inst: K0.ProductInstances[Reads, A]
  ): Reads[A] with

    def readNullable(row: Row, index: Int): A = inst.construct(
      [t] => (r: Reads[t]) => r.readNullable(row, index)
    )
}
