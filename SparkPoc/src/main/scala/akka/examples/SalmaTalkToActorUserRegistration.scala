package akka.examples

import scala.language.postfixOps
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.{ ActorRef, ActorSystem, Props, Actor }
import Storage._
import Recorder._
import Checker._


case class User(username: String, email: String)


object Recorder {
  sealed trait RecorderMsg
  // Recorder Messages
  case class NewUser(user: User) extends RecorderMsg

  def props(checker: ActorRef, storage: ActorRef) =
    Props(new Recorder(checker, storage))

}


object Checker {
  sealed trait CheckerMsg
  // Checker Messages
  case class CheckUser(user: User) extends CheckerMsg

  sealed trait CheckerResponse
  //Checker Responses
  case class BlackUser(user: User) extends CheckerResponse
  case class WhiteUser(user: User) extends CheckerResponse

}


object Storage {
  sealed trait StorageMsg
  // Storage Messages
  case class AddUser(user: User) extends StorageMsg
}


class Storage extends Actor {

  var users = List.empty[User]

  def receive = {
    case AddUser(user) =>
      println(s"Storage: $user added")
      users = user :: users
  }
}

class Checker extends Actor {

  val blackList = List(
    User("Adam", "adam@mail.com")
  )

  def receive = {
    case CheckUser(user) if blackList.contains(user) =>
      println(s"Checker: $user in the blacklist")
      sender() ! BlackUser(user)
    case CheckUser(user) =>
      println(s"Checker: $user not in the blacklist")
      sender() ! WhiteUser(user)
  }
}

class Recorder(checker: ActorRef, storage: ActorRef) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = Timeout(5 seconds)

  def receive = {
    case NewUser(user) =>
      checker ? CheckUser(user) map {
        case WhiteUser(user) =>
          storage ! AddUser(user)
        case BlackUser(user) =>
          println(s"Recorder: $user in the blacklist")
      }
  }

}

object SalmaTalkToActorUserRegistration extends App {

  // Create the 'talk-to-actor' actor system
  val system = ActorSystem("talk-to-actor")

  // Create the 'checker' actor
  val checker = system.actorOf(Props[Checker], "checker")

  // Create the 'storage' actor
  val storage = system.actorOf(Props[Storage], "storage")

  // Create the 'recorder' actor
  val recorder = system.actorOf(Recorder.props(checker, storage), "recorder")

  //send NewUser Message to Recorder
  recorder ! Recorder.NewUser(User("Jon", "jon@packt.com"))  // Asmath --> val user=User("Jon", "jon@packt.com");

  Thread.sleep(100)

  //shutdown system
  system.terminate()
  
  // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  
  
  
/*
 *   Three actors here .. Recorder, Checker and Storage
Recorder gets messages from outside world to add new user.. 
Records talks to checker and see if the user is backlist or not, if not backlist then white user is send. if not then add that user to storage.
In our case, recorder sends deals two types, check if user is present and it not present add that to storage.
Source Code: SalmaKhater/AkkaExamples.
1.	Define case class representing username and email.
2.	Now define, our messages -- New user inside recorder, check user under recorder and add user under storage.
3.	Now define, responses from the checker like white user and black user. It has separate trait like responses inside the checker.
Now Define actors….
1.	Storage actor:  When he receives actor, he adds that to the users list.
var users = List.empty[User]
	

	  def receive = {
	    case AddUser(user) =>
	      println(s"Storage: $user added")
	      users = user :: users
	  }

2.	Checker actor  this actor contains list that are backlisted. When checker receives messages to check the user, 
val blackList = List(
	    User("Adam", "adam@mail.com")
	  )

def receive = {
	    case CheckUser(user) if blackList.contains(user) =>
	      println(s"Checker: $user in the blacklist")
	      sender() ! BlackUser(user)
	    case CheckUser(user) =>
	      println(s"Checker: $user not in the blacklist")
	      sender() ! WhiteUser(user)
	  }

When this actor receives to check user, if checks that user with the backlisted user, if present then it sends that black user object to sender else it will send white user object to the sender.

3.	Now let’s define Recorder actor, when it receives new user message, recorder actors asks checker actor to send message if that user is backlisted or whitelisted user. Note: if it asking checker not sending the message so we use ? not!
? is not terniary operator it is asking message
If user is not backlist, we are going to add that user so we are using ! in this case.
def receive = {
	    case NewUser(user) =>
	      checker ? CheckUser(user) map {
	        case WhiteUser(user) =>
	          storage ! AddUser(user)
	        case BlackUser(user) =>
	          println(s"Recorder: $user in the blacklist")
	      }
	  }*/

  
}

