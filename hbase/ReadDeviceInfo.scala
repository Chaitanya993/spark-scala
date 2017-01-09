package com.hbase

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
import org.apache.hadoop.conf.Configuration;

class nani1 extends nani {

  object ReadDeviceInfo {

    case class DeviceInfoRows(
                               rowkey: String,
                               did: String,
                               dtc: String,
                               hbdt: String,
                               tpid: String,
                               btid: String,
                               dd: DeviceInfo
                             )

    def main(args: Array[String]) = {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
      //--------------Session
      val sparkSession = SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .getOrCreate()


      //----------------------------------------------------------Hbase configuration
      val conf1 = getHbaseConf1()

      val conf2 = getHbaseConf2()

      //------------------------------------------------------------Device hbase table--------------------------------
      val deviceDF = getDeviceInfo(conf1, sparkSession)

      deviceDF.show()
      deviceDF.printSchema()
      deviceDF.registerTempTable("deviceDF")
      sparkSession.sql("select rowkey," +
        "did,dtc,hbdt,dd.device.deviceId," +
        "dd.device.location.latitude " +
        "from deviceDF").show()
    }
    devicemodel.todevicemodel("fnsdk")

    //configuration
    def getHbaseConf1(): Configuration = {
      System.setProperty("user.name", "hdfs")
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val conf1 = HBaseConfiguration.create()
      conf1.set("hbase.master", "master:60000")
      conf1.setInt("timeout", 120000)
      conf1.set("hbase.zookeeper.quorum", "master")
      conf1.set("zookeeper.znode.parent", "/hbase")
      return conf1
    }

    def getHbaseConf2(): Configuration = {
      System.setProperty("user.name", "hdfs")
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val conf2 = HBaseConfiguration.create()
      conf2.set("hbase.master", "master:60000")
      conf2.setInt("timeout", 120000)
      conf2.set("hbase.zookeeper.quorum", "master")
      conf2.set("zookeeper.znode.parent", "/hbase")
      return conf2
    }

    //Get device info table
    def getDeviceInfo(conf: Configuration, sparkSession: SparkSession): DataFrame = {
      val deviceInfoTbl = "deviceinfo3"
      conf.set(TableInputFormat.INPUT_TABLE, deviceInfoTbl)
      import sparkSession.implicits._
      val deviceRDD = sparkSession
        .sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable], classOf[Result])

      val deviceResultNew = deviceRDD.map(tuple => tuple._2)
        .map(row => device_Result(row)).toDF()

      deviceResultNew.registerTempTable("DeviceInfo")
      val deviceInfo = sparkSession.sql("select * from " +
        "DeviceInfo where hbdt ='2016-01-07'").toDF()


      return deviceInfo
    }

    //
    def device_Result(row: Result): ReadDeviceInfo.DeviceInfoRows = {
      val p0 = row.getRow.map(_.toChar).mkString.toString()
      val p1 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("did")))
      val p2 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("dtc")))
      val p3 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("hbdt")))
      val p4 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("tpid")))
      val p5 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("btid")))

      val p6 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("dd")))
      val deviceinfo = DeviceInfo.toDevice(p6)

      return DeviceInfoRows(p0, p1, p2, p3, p4, p5, deviceinfo)
    }


  }

}