package com.amway.magic.los.fullload.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StreamingConsumer {

    def main(args: Array[String]): Unit = {
        @transient lazy val logger = Logger.getLogger(getClass.getName)
        Logger.getLogger("org").setLevel(Level.OFF)

        val stream = args(0)
        val topic = args(1)

        val srcTopic = stream + ":" + topic
        val spark = SparkSession.builder()
                .appName("coreplus-event-dispatcher")
                .master("local[*]")
                //      .master("yarn")
                .getOrCreate()
        val brokers = "localhost:9092"
        import spark.implicits._

        val topicDF = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", false)
                //.option("subscribe", srcTopic)
                .option("subscribePattern", stream +":WWS.*")
                .option("maxOffsetsPerTrigger", 100)
                .load()

        val resultDF = topicDF.select($"topic".cast("String"), $"partition", $"offset",
            $"key".cast("String"),$"value".cast("String"),
            $"timestamp".cast("String") as "ts")
        //resultDF.show(false)

        val query = resultDF.writeStream.outputMode("append")
                .format("console")
                //.trigger(Trigger.ProcessingTime(1.seconds))
                .option("truncate", false)
                .option("numRows", 100).start()
        query.awaitTermination()

        /*val _3290Topic = resultDF
                .filter($"topic" === "/amway/magic/bonus/services/los/streams/full-load-streams/full-load:WWSMGC01.WWT03290_CUST_SPON_DTL")
        val query2 = _3290Topic.writeStream.outputMode("append")
                .format("console")
                //.trigger(Trigger.ProcessingTime(1.seconds))
                .option("truncate", false)
                .option("numRows", 1).start()
        query2.awaitTermination()*/

        //spark.streams.awaitAnyTermination()
    }

}
