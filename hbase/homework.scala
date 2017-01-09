package com.hbase

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.types.{StructType, StructField, LongType};
import com.datastax.spark.connector.rdd.CassandraJoinRDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContextFunctions
import com.datastax.spark.connector._

class nani2 extends nani {

  object homework {

    case class device(
                       btid: String,
                       hbdt: String,
                       dd: devicemodel
                     )

    def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
      //--------------Session
      val sparkSession = SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.cassandra.connection.host", "master")
        .getOrCreate()
      val sc = sparkSession.sparkContext
      import sparkSession.implicits._

      def gethase1(): Configuration = {
        val conf1 = HBaseConfiguration.create()
        conf1.set("hbase.master", "master:60000")
        conf1.setInt("timeout", 120000)
        conf1.set("hbase.zookeeper.quorum", "master")
        conf1.set("zookeeper.znode.parent", "/hbase")
        d1(conf1)
        d2(conf1)
        //conf1.set(TableInputFormat.INPUT_TABLE, "deviceinfo1")
        // conf.set(TableInputFormat.INPUT_TABLE, "errors")
        conf1
      }
      def d1(c: Configuration): Unit = {
        c.set(TableInputFormat.INPUT_TABLE, "deviceinfo3")
        val dataRDD1 = sparkSession.sparkContext
          .newAPIHadoopRDD(c, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        val finaldf1 = dataRDD1.map(_._2).map(l => converttodata1(l)).toDF()
        finaldf1.registerTempTable("datatbl1")

        sparkSession.sql("select * from datatbl1").toDF().show()
      }
      def converttodata1(r: Result): homework.device = {
        val t0 = r.getRow.map(_.toChar).mkString.toString()
        val t1 = Bytes.toString(r.getValue(Bytes.toBytes("m"), Bytes.toBytes("btid")))
        val t2 = Bytes.toString(r.getValue(Bytes.toBytes("m"), Bytes.toBytes("hbdt")))
        val t3 = Bytes.toString(r.getValue(Bytes.toBytes("m"), Bytes.toBytes("dd")))
        println(t3)
        val t4 = devicemodel.todevicemodel(t3)
        device(t1, t2, t4)
      }

      def d2(c: Configuration): Unit = {
        c.set(TableInputFormat.INPUT_TABLE, "errors3")
        val dataRDD2 = sparkSession.sparkContext
          .newAPIHadoopRDD(c, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        val finaldf2 = dataRDD2.map(_._2).map(l => converttodata2(l)).toDF()
        finaldf2.registerTempTable("datatbl2")
      }

      def converttodata2(r: Result): errors = {
        val t0 = r.getRow.map(_.toChar).mkString.toString()
        val t1 = Bytes.toString(r.getValue(Bytes.toBytes("l"), Bytes.toBytes("error_dt")))
        val t2 = Bytes.toString(r.getValue(Bytes.toBytes("l"), Bytes.toBytes("gpsalt")))
        val t3 = Bytes.toString(r.getValue(Bytes.toBytes("l"), Bytes.toBytes("gps")))
        val t4 = Bytes.toString(r.getValue(Bytes.toBytes("l"), Bytes.toBytes("tripgpsspeed")))
        errors(t1, t2, t3, t4)
      }

      val conf1 = gethase1()
      val df = sparkSession.sql("select a.btid,a.hbdt,a.dd.device.model,a.dd.device.osName,a.dd.device.osVersion," +
        "a.dd.device.batteryPercentage,a.dd.device.location.latitute,a.dd.device.location.longitude,b.gpsalt " +
        "from datatbl1 a join datatbl2 b on a.hbdt = b.error_dt ")
      df.show()
      //df.write.mode("append").parquet("/user/data")
      //val conf2 = gethase2()

    }
  }

}