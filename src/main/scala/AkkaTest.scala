import scala.collection.mutable.{Map => MutMap}
import scala.collection.parallel.immutable.ParMap

import scala.concurrent.duration._  
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor._
import akka.util.Timeout
import akka.dispatch.ExecutionContexts._

object AkkaTest extends App {  
    var startTime = System.currentTimeMillis()
    implicit val fileUrl = "/Users/wulicheng/Documents/workspace/AkkaTest/TLOR.txt"
    val system = ActorSystem("System") 
    val resultHandler = system.actorOf(Props[ResultHandler], "ResultHandler")
    val router = system.actorOf(Props(new WatchActor(fileUrl, resultHandler, startTime)), "Router")
    router ! StartProcessFileMsg()
}

case class StartProcessFileMsg()
case class ProcessStringMsg(string: String)  
case class StringProcessedMsg(words: ParMap[String, Int]) 
case class WCResult(result: List[Tuple2[String, Int]], duration: Duration) 

class WatchActor(fileUrl: String, resultHandler: ActorRef, startTime: Long) extends Actor with ActorLogging {
  import scala.io.Source._  

  private val file = fromFile(fileUrl).getLines.toList.par.filterNot(""==)
  private lazy val fileIter = file.toIterator
  private val childs = (1 to scala.sys.runtime.availableProcessors*2).toList
    .map(i => context.actorOf(Props[ActionActor], s"worker-$i"))
  private var linesProcessed = 0
  private var result = MutMap[String, Int]()
  private var running = false
  private lazy val totalLines = file.length
  context.setReceiveTimeout(10 seconds)

  def receive = {
    case StartProcessFileMsg()     =>
      if (running) log.info("Warning: duplicate start message received")
      else {
        running = true
        log.info(s"Router Started")
        childs.foreach(_ ! ProcessStringMsg(fileIter.next))
      }
    case StringProcessedMsg(words) => {
      linesProcessed += 1
      words.foreach{case (k, v) => result.update(k, v + result.getOrElse(k, 0))}
      if (linesProcessed == totalLines && !fileIter.hasNext) {
        log.info(s"Processed lines : ${linesProcessed}\t Total lines : ${totalLines}\tResult :")
        resultHandler ! 
          WCResult(replaceRec(result.toList.sortWith((a, b) => a._2 > b._2).take(40)).take(10),
            (System.currentTimeMillis - startTime).millis)
      }
      else if (fileIter.hasNext) sender ! ProcessStringMsg(fileIter.next)
    }
    case _                         => log.info(s"message not recognized!")
  }

  def replaceRec(str: List[Tuple2[String, Int]], 
      list: List[String] = List("the", "and", "i", "to", "of", "a", "in", "was", "that", "had", "he", "you", "his", "my", "it", "as", "with", "her", "for", "on", "is", "are", "s", "re", "")
      ): List[Tuple2[String, Int]] =
      list match {
        case Nil => str
        case head :: tail => replaceRec(str.filterNot(head==_._1), tail)
      }
}

class ActionActor extends Actor with ActorLogging {
  def receive = {
    case ProcessStringMsg(string) => {
      val wordsCountResult = cleanSymbol(string).toLowerCase
        .split(' ').par
        .groupBy(s => s)
        .map{case (k, v) => (k, v.length)}
      sender ! StringProcessedMsg(wordsCountResult)
    }
    case _                        => log.info("Error: message not recognized")
  }

  def cleanSymbol(str: String) = {
    def replaceRec(str: String, 
      list: List[String] 
        = List("!", ",", ".", ";", "-", "~", "?", "\"", "'", "(", ")", ":", "=")): String =
      list match {
        case Nil => str
        case head :: tail => replaceRec(str.replace(head, " "), tail)
      }
    replaceRec(str).trim.filterNot(""==)
  }
}

class ResultHandler extends Actor {
  def receive = { case WCResult(result, duration) =>
      result.foreach(println)
      println(s"Calculation time : ${duration}")
      context.system.shutdown()
  }
}