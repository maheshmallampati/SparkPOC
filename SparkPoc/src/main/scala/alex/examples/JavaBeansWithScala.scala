package alex.examples

import scala.beans.BeanProperty


class Person(@BeanProperty var firstName: String, 
             @BeanProperty var lastName: String, 
             @BeanProperty var age: Int) {
    //override def toString: String = return format("%s, %s, %d", firstName, lastName, age)
  
    override def toString = s"first=$firstName, last=$lastName, address=$age"
}

class EmailAccount {
    @BeanProperty var accountName: String = null
    @BeanProperty var username: String = null
    @BeanProperty var password: String = null
    @BeanProperty var mailbox: String = null

    
   def this(accountName:String)
  {
    this(); // calls primary constructor
    this.accountName=accountName;    
  }
    
    def this(accountName: String,username: String)
  {
    this(accountName); // calls primary constructor
    this.username=username;    
  }
    
     def this(accountName: String,username: String,password: String)
  {
    this(accountName,username); // calls primary constructor
    this.password=password;    
  }
     
      def this(accountName: String,username: String,password: String,mailbox: String)
  {
    this(accountName,username,password); // calls primary constructor
    this.mailbox=mailbox;    
  }
      
     
    
   
}

class AuxillaryConstructor04
{
  var size=0; //class variables should be initialized in scala
  var age=0; //class variables should be initialized in scala
  
  def this(size:Int)
  {
    this(); // calls primary constructor
    this.size=size;    
  }
  
  def this(size:Int,age:Int)
  {
    this(size); // calls Auxillary constructor
    //this();
    this.age=age;    
  }
}


object JavaBeansWithScala extends App{
  
  
  println("Check Json parsig example (Sunil Patil) also to know more about passing values in scala")
  
  val obj = new Person("Khaja","Mohammed",25);
  println (obj.toString());
  
  val firstName=obj.getFirstName();
  val lastName=obj.getAge();
  println(firstName+"-----"+lastName)
  
  
 // val obj1 = new Person(); -->**** Not Enough Arguments for constructor parameter.  *****
  
    
  val emailObj=new EmailAccount();
  emailObj.setUsername("khajaasmath786");
  val acctName=emailObj.getAccountName();
  println(acctName+ "-----"+emailObj.getUsername() )
  
  
  
  var obj1=new AuxillaryConstructor04;
  var objaux=new AuxillaryConstructor04(5);
  var objau=new AuxillaryConstructor04(5,10);
  
  println(obj1.size,obj1.age); // obj.size  here is accessor method. also println is functon returning tuples here with , see output
  println(objaux.size,objaux.age); // obj.size  here is accessor method. also println is functon returning tuples here with , see output
  println(objau.size,objau.age); // obj.size  here is accessor method. also println is functon returning tuples here with , see output
  
  
  
}