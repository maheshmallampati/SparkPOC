package akka.examples

import akka.actor._
 
case object PingMessage
case object PongMessage
case object StartMessage
case object StopMessage
 
/**
 * An Akka Actor example written by Alvin Alexander of
 * <a href="http://devdaily.com" title="http://devdaily.com">http://devdaily.com</a>
 *
 * Shared here under the terms of the Creative Commons
 * Attribution Share-Alike License: <a href="http://creativecommons.org/licenses/by-sa/2.5/" title="http://creativecommons.org/licenses/by-sa/2.5/">http://creativecommons.org/licenses/by-sa/2.5/</a>
 * 
 * more akka info: <a href="http://doc.akka.io/docs/akka/snapshot/scala/actors.html" title="http://doc.akka.io/docs/akka/snapshot/scala/actors.html">http://doc.akka.io/docs/akka/snapshot/scala/actors.html</a>
 */
class Ping(pong: ActorRef) extends Actor {
  override def preStart { println("kenny: preStart") }
    override def postStop { println("kenny: postStop") }
    override def preRestart(reason: Throwable, message: Option[Any]) {
        println("Called only when exception occurs")
        println("kenny: preRestart")
        println(s" MESSAGE: ${message.getOrElse("")}")
        println(s" REASON: ${reason.getMessage}")
        super.preRestart(reason, message)
    }
    override def postRestart(reason: Throwable) {
        println("Called only when exception occurs")
        println("kenny: postRestart")
        println(s" REASON: ${reason.getMessage}")
        super.postRestart(reason)
    }
    
  var count = 0
  def incrementAndPrint { count += 1; println("ping") }
  def receive = {
    case StartMessage =>
        incrementAndPrint
        pong ! PingMessage // Pong Object sending PingMessage to ping object . ! is used for sending messages.
        //println("Ponging")
    case PongMessage => 
        incrementAndPrint
        if (count > 2) {
          sender ! StopMessage
          println("ping stopped")
          context.stop(self)
        } else {
          sender ! PingMessage
        }
  }
}
 
class Pong extends Actor {
  override def preStart { println("kenny: preStart") }
    override def postStop { println("kenny: postStop") }
    override def preRestart(reason: Throwable, message: Option[Any]) {
        println("kenny: preRestart")
        println(s" MESSAGE: ${message.getOrElse("")}")
        println(s" REASON: ${reason.getMessage}")
        super.preRestart(reason, message)
    }
    override def postRestart(reason: Throwable) {
        println("kenny: postRestart")
        println(s" REASON: ${reason.getMessage}")
        super.postRestart(reason)
    }
    
  def receive = {
    case PingMessage =>
        println("  pong")
        sender ! PongMessage
    case StopMessage =>
        println("pong stopped")
        context.stop(self)
        //context.system.shutdown()
        //http://alvinalexander.com/scala/scala-akka-actors-stop-shut-down-shutdown-receive -- see this to stop actor completely and implement context.system.shutdown()
        
  }
}
 
object PingPongTest extends App {
  val system = ActorSystem("PingPongSystem")
  val pong = system.actorOf(Props[Pong], name = "pong")
  val ping = system.actorOf(Props(new Ping(pong)), name = "ping")
  // start them going
  ping ! StartMessage
  println("Actual shutdown can happen by calling context.system.shutdown() inside reference methods after actor completes everything")
  //shutdown actorsystem
  system.terminate()
}