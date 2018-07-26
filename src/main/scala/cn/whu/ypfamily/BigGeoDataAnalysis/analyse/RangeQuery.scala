package cn.whu.ypfamily.BigGeoDataAnalysis.analyse

import java.util.Date

import cn.whu.ypfamily.BigGeoDataAnalysis.rdd.SpatialRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Envelope

object RangeQuery {

  def main(args: Array[String]): Unit = {

    if (args.length < 6 || args.length > 7) {
      println("input " +
        "*<hdfs path> " +
        "*<zookeeper server list> " +
        "*<table name> " +
        "*<geometry column family> " +
        "*<geometry column> " +
        "*<envelope:x1,y1,x2,y2> " +
        "<partition number>")
      return
    }

    // 开始时间
    val startTime = new Date().getTime

    // 获取参数
    val hdfsPath = args(0)
    val serverList = args(1)
    val tableName = args(2)
    val gsColumnFamily = args(3)
    val gsColumn = args(4)
    val arrEnvolope = args(5).split(",")

    // 设置Spark参数生成Spark上下文
    val sparkConf = new SparkConf().setAppName("RangeQuery")
    val sc = new SparkContext(sparkConf)

    // 设置HBase参数
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("fs.defaultFS", hdfsPath)
    hbaseConf.set("hbase.zookeeper.quorum", serverList)
    hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    var spatialRDD:SpatialRDD = null

    // 如果不进行重分区
    if (args.length < 7) {
      // 从HBase读取数据生成SpatialRDD
      spatialRDD = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, gsColumnFamily, gsColumn)
    }

    // 如果进行重分区
    if (args.length == 7) {
      // 获得参数
      val partitionNum = args(6).toInt
      // 从HBase读取数据生成SpatialRDD
      spatialRDD = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, gsColumnFamily, gsColumn, partitionNum)

    }

    // 空间范围查询
    val rs = spatialRDD.rangeQuery(
      new Envelope(
        arrEnvolope(0).toDouble,
        arrEnvolope(1).toDouble,
        arrEnvolope(2).toDouble,
        arrEnvolope(3).toDouble
      )
    ).collect()

    // 输出查询结果
    rs.foreach { data =>
      println(data._2.oid)
    }

    // 关闭Spark上下文
    sc.stop()

    // 结束时间，计算耗时
    val endTime = new Date().getTime
    println("耗时：" + (endTime - startTime) + "毫秒")
  }

}