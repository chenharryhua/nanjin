package mtest.http.auth

import org.cometd.bayeux.{Channel, Message}
import org.cometd.bayeux.client.{ClientSession, ClientSessionChannel}
import org.cometd.client.BayeuxClient
//import org.cometd.client.http.jetty.JettyHttpClientTransport
import org.eclipse.jetty.client.HttpClient
import org.scalatest.funsuite.AnyFunSuite

import java.util

class JettyTest extends AnyFunSuite {
  test("jetty") {
    val httpClient = new HttpClient()
    // Here configure Jetty's HttpClient.
    // httpClient.setMaxConnectionsPerDestination(2);
    httpClient.start

    // Prepare the transport.
    // val options      = new util.HashMap[String, Object]()
    // val transport    = new JettyHttpClientTransport(options, httpClient)
    // val bayeuxClient = new BayeuxClient("https://tabcorp--TABUAT.my.salesforce.com/cometd/45.0", transport)

    // bayeuxClient.getChannel("/event/Case_Decision__e").subscribe { (channel: ClientSessionChannel, message: Message) =>
    //   println(message)
    // }
    // bayeuxClient.handshake(new ClientSession.MessageListener {
    //   override def onMessage(message: Message): Unit = {
    //     bayeuxClient.getChannel(Channel.META_CONNECT).subscribe { (channel: ClientSessionChannel, message: Message) =>
    //       println(message)
    //     } 
    //     ()
    //   }
    // })

    // Thread.sleep(30000)

  }

}
