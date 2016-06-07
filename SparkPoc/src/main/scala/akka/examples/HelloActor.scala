package akka.examples

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging

//http://alvinalexander.com/scala/simple-scala-akka-actor-examples-hello-world-actors
//http://doc.akka.io/docs/akka/2.0/scala/actors.html

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Actor with DefaultConstructor %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class HelloActorWithoutConstructor extends Actor {
  val log = Logging(context.system, this)
  override def preStart() = {
    //... // initialization code
    println("Initialization code of HelloActorWithoutConstructor can be written here")
    log.info("Initialization code of HelloActorWithoutConstructor can be written here")
  }
  /*
   * Actor Trait. You should define override method receive to receive messages and send messages to actors.
   * Also have to use pattern matching.
   *  */

  def receive = {
    case "hello" => println("hello back at you") // Pattern Matching in receive
    case _       => println("huh?") // Have to include default else you will get errors.
  }
}

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Actor with ParameterisedConstructor %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class HelloActorWithConstructor(myName: String) extends Actor {
  val log = Logging(context.system, this)
  override def preStart() = {
    //... // initialization code
    println("Initialization code of HelloActorWithConstructor can be written here")
    log.info("Initialization code of HelloActorWithConstructor can be written here")
  }
  def receive = {
    // (2) changed these println statements
    case "hello"                  => println("hello from %s".format(myName))
    case "hello With Constructor" => println("hello from %s".format(myName))
    case _                        => println("'huh?', said %s".format(myName))
  }
}

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Object Creations of Actors %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
object HelloActor extends App {

  val system = ActorSystem("HelloSystem")

  println("Creating actor from system with default constructor ***********************************")
  // default Actor constructor
  val helloActor = system.actorOf(Props[HelloActorWithoutConstructor], name = "helloactor") // Creating actor with System then it will become parent. You can create actos with multiple ways.
  val helloActorWithoutConstOtherWay = system.actorOf(Props(new HelloActorWithoutConstructor()), name = "helloActorWithoutConstOtherWay") // Creating actor with System then it will become parent. You can create actos with multiple ways.
  helloActor ! "hello"
  helloActor ! "hello"
  helloActor ! "buenos dias"
  

  println("Creating actor from system with Constructor ***********************************")
  val helloActorWithConst = system.actorOf(Props(new HelloActorWithConstructor("Asmath")), name = "helloactorwithconstructor") // Creating actor with System then it will become parent. You can create actos with multiple ways.
  helloActorWithConst ! "hello With Constructor" // Pattern is not found in this case so always default
  helloActorWithConst ! "buenos dias with Constructor"
  
  println("Actual shutdown can happen by calling context.system.shutdown() inside reference methods after actor completes everything")
  
  println("Below comment has list of ways you can create actor instance with props")
  //val helloActorWithDiffProps = system.actorOf(Props(new HelloActorWithoutConstructor()).withDispatcher("my-dispatcher"), name = "helloActorWithProps") // Creating actor with System then it will become parent. You can create actos with multiple ways.
         
         /* val props1 = Props()
          val props2 = Props[MyActor]
          val props3 = Props(new MyActor)
          val props4 = Props(
            creator = { () ⇒ new MyActor },
            dispatcher = "my-dispatcher")
          val props5 = props1.withCreator(new MyActor)
          val props6 = props5.withDispatcher("my-dispatcher")
          
          * */
  //shutdown actorsystem
  system.terminate()
          
}