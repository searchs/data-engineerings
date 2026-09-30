package examples.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Curated from the legacy KafkaStreamer repository.
 *
 * Demonstrates connecting Spark Structured Streaming to Kafka and defining
 * an invoice schema. The original example stopped after loading the Kafka
 * stream; this version makes that limitation explicit rather than implying a
 * complete processing pipeline.
 */
object KafkaSparkStreamingExample {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "invoices")

    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Kafka Spark Structured Streaming Example")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

    val invoiceSchema = StructType(
      Seq(
        StructField("InvoiceNumber", StringType),
        StructField("CreatedTime", LongType),
        StructField("StoreID", StringType),
        StructField("PosID", StringType),
        StructField("CashierID", StringType),
        StructField("CustomerType", StringType),
        StructField("CustomerCardNo", StringType),
        StructField("TotalAmount", DoubleType),
        StructField("NumberOfItems", IntegerType),
        StructField("PaymentMethod", StringType),
        StructField("CGST", DoubleType),
        StructField("SGST", DoubleType),
        StructField("CESS", DoubleType),
        StructField("DeliveryType", StringType),
        StructField(
          "DeliveryAddress",
          StructType(
            Seq(
              StructField("AddressLine", StringType),
              StructField("City", StringType),
              StructField("State", StringType),
              StructField("PinCode", StringType),
              StructField("ContactNumber", StringType)
            )
          )
        ),
        StructField(
          "InvoiceLineItems",
          ArrayType(
            StructType(
              Seq(
                StructField("ItemCode", StringType),
                StructField("ItemDescription", StringType),
                StructField("ItemPrice", DoubleType),
                StructField("ItemQty", IntegerType),
                StructField("TotalValue", DoubleType)
              )
            )
          )
        )
      )
    )

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.printSchema()
    println(s"Defined invoice schema with ${invoiceSchema.fields.length} top-level fields")

    // Next step for a complete lab:
    // cast kafkaDF.value to string, parse JSON with from_json(invoiceSchema),
    // then write the parsed stream to a real sink with checkpointing.
  }
}
