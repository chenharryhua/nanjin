package com.github.chenharryhua.nanjin.spark.dstream

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError}

class NJDStreamListener() extends StreamingListener {

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit =
    super.onReceiverError(receiverError)

}
