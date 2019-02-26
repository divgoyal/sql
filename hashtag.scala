package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.flume._
import scala.util.parsing.json._

object hashtag{
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: Hashtag Analyzer <hostname> <port>")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("Twitter Hashtag")
				val ssc = new StreamingContext(sparkConf, Seconds(1))

				val rawLines = FlumeUtils.createStream(ssc, args(0), args(1).toInt)

				val result = rawLines.map{record => {
					(new String(record.event.getBody().array()))
				}
		    }
 			.map{ line => {
			  val parsedJson = JSON.parseFull(line)
			  
			  parsedJson.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String]
			}
			}
 			.flatMap { tweet => {
			  tweet.split(" ")
			}
			}
			.map{ rec => (rec, 1)}
			.filter { token => token._1.startsWith("#") }
			.reduceByKeyAndWindow((a:Int, b:Int) => a+b, Seconds(120), Seconds(30))
      .transform{rec =>{
         rec.sortBy(_._2, false)
      }
	    }

			
			result.print()
			ssc.start()
			ssc.awaitTermination()
	}
}