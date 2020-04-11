package p1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  val aBoolean: Boolean = false

  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  val theUnit = println(anIfExpression)

  def myFunc(x: Int) = 42

  class Animal

  class Dog extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = ???
  }

  object MySingleton

  object Carnivore

  trait MyList[A]

  val x = 1 + 2

  val incrementer: Int => Int = x => x + 1
  val incremented: Int = incrementer(22)

  val processedList = List(1, 2, 3).map(incrementer)

  val unknown: Any = 45

  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "none"
  }

  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some err"
    case _ => "something"
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I have failed: $meaningOfLife")
    case Failure(exception) => ???
  }

  case class Person(name: String)
  implicit def fromStringToPerson(name: String) = Person(name)
}
