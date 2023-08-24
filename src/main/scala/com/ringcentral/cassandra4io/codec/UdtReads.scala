package com.ringcentral.cassandra4io.codec

import com.datastax.oss.driver.api.core.data.UdtValue

trait UdtReads[T] {
  def read(udt: UdtValue): T
}

object UdtReads extends UdtReadsInstances {
  def apply[T](implicit udtReads: UdtReads[T]): UdtReads[T] = udtReads

  def instance[T](f: UdtValue => T): UdtReads[T] = (udtValue: UdtValue) => f(udtValue)

  final implicit class UdtReadsOps[A](private val reads: UdtReads[A]) extends AnyVal {
    def map[B](f: A => B): UdtReads[B] =
      instance(udtValue => f(reads.read(udtValue)))
  }
}
