/*
 * =========================================================================================
 * Copyright © 2013-2017 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.jaeger

import java.nio.ByteBuffer
import java.util

import com.typesafe.config.Config
import com.uber.jaeger.thriftjava.{Log, Process, Tag, TagType, Span => JaegerSpan}
import com.uber.jaeger.senders.{HttpSender, ThriftSender, UdpSender}
import kamon.trace.IdentityProvider.Identifier
import kamon.trace.Span
import kamon.util.Clock
import kamon.{Kamon, SpanReporter}

import scala.util.Try

class JaegerReporter extends SpanReporter {

  @volatile private var jaegerClient:JaegerClient = _

  reconfigure(Kamon.config())

  override def reconfigure(newConfig: Config):Unit = {
    val jaegerConfig = newConfig.getConfig("kamon.jaeger")
    val host = jaegerConfig.getString("host")
    val port = jaegerConfig.getInt("port")

    val jaegerSender = jaegerConfig.getString("sender")

    jaegerSender.toLowerCase match {
      case "udp" =>
        val maxPacketSize = jaegerConfig.getInt("udp.max-packet-size")
        jaegerClient = new JaegerClient(new UdpSender(host, port, maxPacketSize))

      case "http" =>
        val endpoint = s"http://$host:$port/api/traces"
        jaegerClient = new JaegerClient(new HttpSender(endpoint))

      case sender =>
        sys.error(s"Unknown jaeger sender configured: $sender" )
    }

  }

  override def start(): Unit = {}
  override def stop(): Unit = {}

  override def reportSpans(spans: Seq[Span.FinishedSpan]): Unit = {
    jaegerClient.sendSpans(spans)
  }
}

class JaegerClient(sender: ThriftSender) {
  import scala.collection.JavaConverters._

  val process = new Process(Kamon.environment.service)

  def sendSpans(spans: Seq[Span.FinishedSpan]): Unit = {
    val convertedSpans = spans.map(convertSpan).asJava
    try {
      sender.send(process, convertedSpans)
    } finally {
      sender.flush()
    }
  }

  private def convertSpan(span: Span.FinishedSpan): JaegerSpan = {
    val from = Clock.toEpochMicros(span.from)
    val duration = Clock.toEpochMicros(span.to) - from

    val convertedSpan = new JaegerSpan(
      convertIdentifier(span.context.traceID),
      0L,
      convertIdentifier(span.context.spanID),
      convertIdentifier(span.context.parentID),
      span.operationName,
      0,
      from,
      duration
    )

    convertedSpan.setTags(new util.ArrayList[Tag](span.tags.size))
    span.tags.foreach {
      case (k, v) => v match {
        case Span.TagValue.True =>
          val tag = new Tag(k, TagType.BOOL)
          tag.setVBool(true)
          convertedSpan.tags.add(tag)

        case Span.TagValue.False =>
          val tag = new Tag(k, TagType.BOOL)
          tag.setVBool(false)
          convertedSpan.tags.add(tag)

        case Span.TagValue.String(string) =>
          val tag = new Tag(k, TagType.STRING)
          tag.setVStr(string)
          convertedSpan.tags.add(tag)

        case Span.TagValue.Number(number) =>
          val tag = new Tag(k, TagType.LONG)
          tag.setVLong(number)
          convertedSpan.tags.add(tag)
      }
    }

    span.marks.foreach(mark => {
      val markTag = new Tag("event", TagType.STRING)
      markTag.setVStr(mark.key)
      convertedSpan.addToLogs(new Log(Clock.toEpochMicros(mark.instant), java.util.Collections.singletonList(markTag)))
    })

    convertedSpan
  }

  private def convertIdentifier(identifier: Identifier): Long = Try {
    // Assumes that Kamon was configured to use the default identity generator.
    ByteBuffer.wrap(identifier.bytes).getLong
  }.getOrElse(0L)
}
