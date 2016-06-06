package akka.examples.actorpaths

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, PoisonPill}


object ActorPath extends App {

  val system = ActorSystem("Actor-Paths")

  val counter1 = system.actorOf(Props[Counter], "counter")
  
  println(s"Actor reference for counter1:${counter1}")
  
  val counterSelection1 = system.actorSelection("counter") // This is path or actor name --> Here we gave actor name.
  
  println(s"Actor reference for counterSelection1:${counterSelection1}")
  
  counter1!PoisonPill //Actor Selections are same but actor references are different. We can kill actor selection only through posionpill and communicate through identity

  
  
  
  Thread.sleep(100)

  val counter2 = system.actorOf(Props[Counter], "counter")
  
  println(s"Actor reference for counter1:${counter2}")
  
  val counterSelection2 = system.actorSelection("counter") // This is path or actor name --> Here we gave actor name.
  
  println(s"Actor reference for counterSelection2:${counterSelection2}")
  
  counter2!PoisonPill
  
  println("Actor Selections are same but actor references are different. We can kill actor selection only through posionpill and communicate through identity")
	//system.terminate()
	
	
	
	//$$$$$$$$$$$$$$$$$$ USING WATCHER TO CALL ACTOR PATHS /user/counter $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
	val counter3 = system.actorOf(Props[Counter], "counter")
	val watcher = system.actorOf(Props[Watcher], "watcher")
	println("we are getting instance of counter from watcher using path inside the actor class <<<<<<<< ---> val counter2 = system.actorOf(Props[Counter], counter")
  println()
	Thread.sleep(1000)

	system.terminate()
	
	
}