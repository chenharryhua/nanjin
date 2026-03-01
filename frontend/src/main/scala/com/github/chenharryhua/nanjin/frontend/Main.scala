package com.github.chenharryhua.nanjin.frontend
import org.scalajs.dom
import org.scalajs.dom.{CloseEvent, Event, MessageEvent, WebSocket}

object Main {
  def main(args: Array[String]): Unit = {
    // Connect to backend WebSocket
    val ws = new WebSocket("ws://localhost:1026/ws")

    // When the connection opens
    ws.onopen = { (_: Event) =>
      dom.console.log("WebSocket connected")
      ws.send("Hello from Scala.js frontend!")
    }

    // When a message is received from the server
    ws.onmessage = { (e: MessageEvent) =>
      dom.console.log(s"Received: ${e.data.toString}")
      // For demo: display message in the DOM
      val p = dom.document.createElement("p")
      p.textContent = e.data.toString
      dom.document.body.appendChild(p)
    }

    // Handle WebSocket close
    ws.onclose = { (e: CloseEvent) =>
      dom.console.log(s"WebSocket closed: code=${e.code} reason=${e.reason}")
    }

    // Handle errors
    ws.onerror = { (e: Event) =>
      dom.console.error("WebSocket error", e)
    }
  }
}
