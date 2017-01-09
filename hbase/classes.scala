package com.hbase

/**
  * Created by chait on 10/10/2016.
  */
import java.io.StringReader

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonReader
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON._
import scala.util.parsing.json.JSON._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

trait nani {

  case class devicemodel(
                          device: devicedetails
                        )

  case class devicedetails(
                            model: String,
                            osName: String,
                            osVersion: String,
                            batteryPercentage: String,
                            location: locationinfo
                          )

  case class locationinfo(
                           latitute: String,
                           longitude: String
                         )

  case class errors(
                     error_dt: String,
                     gpsalt: String,
                     gps: String,
                     tripgpsspeed: String
                   )

  object devicemodel {
    def todevicemodel(jsonString1: String): devicemodel = {
      val json = new GsonBuilder().create()
      //val gson = new Gson();

      //val jsonstring1 = jsonString.trim()
      val a = json.fromJson(jsonString1, classOf[devicemodel])
      a
    }

  }

}

