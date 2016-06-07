package akka.examples

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Stash }
import UserStorage._


case class User1(username: String, email: String)

object UserStorage {

 trait DBOperation
 object DBOperation {
   case object Create extends DBOperation
   case object Update extends DBOperation
   case object Read extends DBOperation
   case object Delete extends DBOperation
 }

 case object Connect
 case object Disconnect
 case class Operation(dBOperation: DBOperation, user: Option[User])

}

class UserStorage extends Actor with Stash{

 def receive = 
   {
   println("Inside Receive ")
   disconnected // calling disconnected first which internally calls connected. First it calls default _, that is stash, and then calls connect.
   }

 def connected: Actor.Receive = {
   case Disconnect =>
     println("User Storage Disconnect from DB")
     context.unbecome()
   case Operation(op, user) =>
     println(s"User Storage receive ${op} to do in user: ${user}")

 }

 def disconnected: Actor.Receive = {
   case Connect =>
     println(s"User Storage connected to DB")
     unstashAll()
     println("Calling become connected")
     context.become(connected)
   case _ =>
     println("Inside stash")
     stash()
 }
}

object SalmaBecomeUnbecomeDBStorage08 extends App {
 import UserStorage._

 val system = ActorSystem("Hotswap-Become")

 val userStorage = system.actorOf(Props[UserStorage], "userStorage")

println("Asmath --> I am not sure why receive is not called multiple times here ")
 userStorage ! Operation(DBOperation.Create, Some(User("Admin", "admin@packt.com"))) 
 // This particular statement will call receive, from receive it goes to disconnect method. disconnect to Stash and saves the message in stash (FIFO)
 // Message saved in stash is Operation(DBOperation.Create, Some(User("Admin", "admin@packt.com")) . It is not executed untill you call unstash.
 Thread.sleep(5000)
 println("------------------------------------------------")
 userStorage ! Connect 
 Thread.sleep(5000)
 println("------------------------------------------------")
 // When connect is called, it calls disconnected, from there it goes to case Connect.. Prints connected. now untash is called, with Operation(DBOperation.Create, Some(User("Admin", "admin@packt.com"))
 // pulled from stash. that executes and puts the result.

 userStorage ! Disconnect

 Thread.sleep(5000)
 println("------------------------------------------------")
 Thread.sleep(5000)
 system.terminate()
}