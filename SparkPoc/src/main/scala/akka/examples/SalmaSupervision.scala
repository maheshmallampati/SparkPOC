package akka.examples

import scala.language.postfixOps
import scala.concurrent.duration._
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import akka.actor.{ ActorRef, ActorSystem, Props, Actor }

class Aphrodite extends Actor {
  import Aphrodite._

  override def preStart() = {
    println("Aphrodite preStart hook....")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    println("Aphrodite preRestart hook...")
    super.preRestart(reason, message)
  }

   override def postRestart(reason: Throwable) = {
    println("Aphrodite postRestart hook...")
    super.postRestart(reason)
  }

  override def postStop() = {
    println("Aphrodite postStop...")
  }

  def receive = {
    case "Resume" => 
      throw ResumeException
    case "Stop" => 
      throw StopException
    case "Restart" => 
      throw RestartException
    case _ => 
      throw new Exception
  }
} 

object Aphrodite {
  case object ResumeException extends Exception
  case object StopException extends Exception
  case object RestartException extends Exception
}

class Hera extends Actor {
  import Aphrodite._

  var childRef: ActorRef = _

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 second) {
      case ResumeException      => Resume
      case RestartException     => Restart
      case StopException        => Stop
      case _: Exception            => Escalate
  }


  override def preStart() = {
    // Create Aphrodite Actor
    childRef = context.actorOf(Props[Aphrodite], "Aphrodite")
    Thread.sleep(100)
  }

  def receive = {
    case msg123 => 
      println(s"Hera received ${msg123}")
      childRef ! msg123
      Thread.sleep(100)
  }
} 

object SalmaSupervision extends App {
  
/*  Supervision: Hera is parent of Aphrodite. Lets add supervision strategy in hera actor. 
We will override supervision strategy, override val supervisorStrategy. Child actor is created in child itself inside prestart method.
When hera receives stop exception, it will stop the child actor since we overriden the strategy, it will go to child to get the appropriate message and excepton. from exception it calls library like stop, resume or restart of scala
When hera receives resume exception, it will stop the child actor since we overriden the strategy, it will go to child to get the appropriate message and excepton. from exception it calls library like stop, resume or restart of scala*/



 // Create the 'supervision' actor system
 val system = ActorSystem("supervision")

 // Create Hera Actor
 val hera = system.actorOf(Props[Hera], "hera")

 //   hera ! "Resume"
 //   Thread.sleep(1000)
 //   println()

 //  hera ! "Restart"
 //  Thread.sleep(1000)
 //  println()

 hera ! "Stop"  // Asmath --> not sure how the msg is read when you are sending stop as message. This first goes to hera and since we overriden the strategy, it will go to child to get the appropriate message and excepton. from exception it calls library like stop, resume or restart of scala
 Thread.sleep(1000)
 println()


 system.terminate()

}