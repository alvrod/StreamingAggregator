package com.alvrod.stragg

import org.apache.camel.processor.aggregate.CompletionAwareAggregationStrategy
import org.apache.camel.{ProducerTemplate, EndpointInject, Exchange}
import scala.beans.BeanProperty
import java.io.{PipedWriter, PipedReader}

class StreamingAggregator extends CompletionAwareAggregationStrategy {
  @BeanProperty var endpointUri: String = _
  @EndpointInject var producer: ProducerTemplate = _

  override def aggregate(oldExchangeNullable: Exchange, newExchange: Exchange): Exchange = {
    val oldExchangeOpt = Option(oldExchangeNullable)
    oldExchangeOpt match {
      case None => // first message
        val writer = new PipedWriter()
        val reader = new PipedReader(writer, 100000)

        writer.write(newExchange.getIn.getBody.toString)

        val createFileExchange = newExchange.copy()
        createFileExchange.getIn.setBody(reader)

        // This will "tie" the incoming content from the Messages to the content written to the File
        producer.asyncSend(endpointUri, createFileExchange)

        // Return the writer, so that on further aggregations it can be written to.
        newExchange.getIn.setBody(writer)
        newExchange

      case Some(oldExchange) =>
        // write content to stream
        val message = oldExchange.getIn
        val writer = message.getBody.asInstanceOf[PipedWriter]

        val newBody = newExchange.getIn.getBody
        writer.append(newBody.toString)

        // keep passing the writer around
        newExchange.getIn.setBody(writer)
        newExchange
    }
  }

  /* Detail on possible values on the Exchange.AGGREGATED_COMPLETED_BY property
   * "consumer" -> for the Batch Consumer
   * "forceCompletion" -> completed by a force completion request, i.e. during shutdown or
   *                      when signaled (see http://camel.apache.org/aggregator2.html
   *                      "Manually Force the Completion"
   * "interval" -> completed by interval
   * "timeout" -> completed by timeout
   * "size" -> completed by size
   * "predicate" -> completed by predicate
   */
  override def onCompletion(completed: Exchange) {
    val resultHeaderName = "AggregationResult"
    val completedBy = completed.getProperty(Exchange.AGGREGATED_COMPLETED_BY)
    val input = completed.getIn

    val writer = input.getBody.asInstanceOf[PipedWriter]
    writer.flush()
    writer.close()

    val expectedCount = input.getHeader("CamelSplitIndex").asInstanceOf[Int] + 1
    val count = completed.getProperty("CamelAggregatedSize", classOf[Long])
    input.setBody(s"Completed aggregating $count messages by $completedBy from ${completed.getFromEndpoint}")

    completedBy match {
      // Complete stream
      case "predicate" if expectedCount == count =>
        input.setHeader(resultHeaderName, "SUCCESS")

      // Something went wrong (missing messages?)
      case other: String =>
        input.setHeader(resultHeaderName, "MISSING_CONTENT")

      case other: Any =>
        input.setHeader(resultHeaderName, "UNEXPECTED")
    }
  }
}
