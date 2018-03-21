package com.hashmapinc.processor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object IOTDataProcessor extends App{

	private val temperature = args(0).toDouble //min temperature to consider for avg

	private implicit val spark = SparkSession.builder().appName("IOT Data Processor").getOrCreate()
	import spark.implicits._

	val ds = readJsonResourceAsDataset("iot_devices.json")

	ds.show(10)

	private val avged = ds.filter(d => d.temp > temperature).map(d => (d.temp, d.humidity, d.cca3)).groupBy($"_3").avg()

	avged.show(30)

	spark.close()


	def readJsonResource(file: String): List[String] = {
		val stream = getClass.getResourceAsStream(s"/$file")
		scala.io.Source.fromInputStream(stream)
			.getLines
			.toList
	}

	def readJsonResourceAsRDD(file: String)(implicit sparkCtxt: SparkContext): RDD[String] =
		sparkCtxt.parallelize(readJsonResource(file))

	def readJsonResourceAsDataset(file: String)(implicit sparkSession: SparkSession): Dataset[DeviceIoTData] =
		sparkSession.read.json(readJsonResourceAsRDD(file)(sparkSession.sparkContext)).as[DeviceIoTData]
}

case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String,
													cn: String, device_id: Long, device_name: String, humidity: Long,
													ip: String, latitude: Double, lcd: String, longitude: Double,
													scale:String, temp: Long, timestamp: Long)
