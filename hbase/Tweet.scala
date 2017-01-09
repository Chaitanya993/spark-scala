package com.hbase

/**
  * Created by chait on 10/8/2016.
  */

  import com.google.gson.GsonBuilder
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.util.Bytes

  case class Tweet (text:String,
                    favorited:String,
                    possibly_sensitive:String,
                    timestamp_ms:String,
                    id:String ,
                    geo:String ,
                    retweet_count:String ,
                    source:String,
                    var tweetStr:String)

  /**
    * Created by satyak on 9/20/2016.
    */
  object Tweet{

    def convertToPut(data: Tweet): (ImmutableBytesWritable, Put) = {
      val rowkey = data.id.toString()
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("Tw"), Bytes.toBytes(data.text));
      put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("retweet_count"), Bytes.toBytes(data.retweet_count));
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }


  }


