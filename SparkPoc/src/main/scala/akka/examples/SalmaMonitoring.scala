package akka.examples


import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Terminated }


class Ares(athena: ActorRef) extends Actor {

  override def preStart() = {
    context.watch(athena)
  }

  override def postStop() = {
    println("Ares postStop...")
  }

  def receive = {
    
    case Terminated => 
      context.stop(self)
      //println("Ares received the message")
  }
}

class Athena extends Actor {

  def receive = {
    case msg1 => 
      println(s"Athena received ${msg1}")
      context.stop(self)
  }

}

object SalmaMonitoring extends App {

 // Create the 'monitoring' actor system
 val system = ActorSystem("monitoring")

 val athena = system.actorOf(Props[Athena], "athena")  // Child

 val ares = system.actorOf(Props(classOf[Ares], athena), "ares") //Parent actor is created by passing Child reference here.

 athena ! "Sam" // calling child receive method and stopping. since child is stopped, parent is also stopped by calling watch method on pre-start as it is monitoring the child through watch method. 

 system.terminate()


}