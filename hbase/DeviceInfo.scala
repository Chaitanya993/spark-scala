package com.hbase

/**
  * Created by chait on 10/8/2016.
  */
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.rdd.RDD

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

//Note column names should match column names in JSON
case class location (latitude:String,longitude:String)
case class device (deviceId:String,
                   deviceTypeCode:String,
                   batteryPercentage:String,
                   osName:String,
                   osVersion:String,
                   model:String,
                   manufacturer:String,
                   vehicleBluetoothConnectionId:String,
                   location:location)
case class DeviceInfo (tcId:String,device:device)

object DeviceInfo {
  //get device object from device JSON string
  def  toDevice(jsonString:String): DeviceInfo ={
    val gson = new GsonBuilder().create()
    val hb = gson.fromJson(jsonString, classOf[DeviceInfo]);
    return hb
  }

}
