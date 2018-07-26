package cn.whu.ypfamily.BigGeoDataAnalysis.analyse

import java.util
import java.util.Date

import cn.whu.ypfamily.BigGeoDataAnalysis.rdd.SpatialRDD
import com.google.gson.{Gson, JsonObject}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object OverlayPDT {
  def main(args: Array[String]): Unit = {
    if (args.length < 11 || args.length > 12) {
      println("input " +
        "*<hdfs path> " +
        "*<zookeeper server list> " +
        "*<DLTB table name> " +
        "*<DLTB geometry column family name> " +
        "*<DLTB geometry column name> " +
        "*<PDT table name> " +
        "*<PDT geometry column family name> " +
        "*<PDT geometry column name> " +
        "*<batch number>" +
        "*<PD tag name> " +
        "*<output path>" +
        "<partition number>")
      return
    }

    // 开始时间
    val startTime = new Date().getTime

    // 获取参数
    val hdfsPath = args(0)
    val serverList = args(1)
    val dltbTableName = args(2)
    val dltbGeoColumnFamily = args(3)
    val dltbGeoColumn = args(4)
    val pdtTableName = args(5)
    val pdtGeoColumnFamily = args(6)
    val pdtGeoColumn = args(7)
    val numBatch = args(8).toInt
    val pdTagName = args(9)
    val outputPath = args(10)

    // 设置Spark参数生成Spark上下文
    val sparkConf = new SparkConf().setAppName("OverlayPDT")
    val sc = new SparkContext(sparkConf)
    // 设置HBase参数
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("fs.defaultFS", hdfsPath)
    hbaseConf.set("hbase.zookeeper.quorum", serverList)
    hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

    // 从HBase读取DLTB和PDT数据
    var rddDLTB: SpatialRDD = null
    var rddPDT: SpatialRDD = null
    if (args.length == 11) { // 如果不进行重分区
      hbaseConf.set(TableInputFormat.INPUT_TABLE, dltbTableName)
      rddDLTB = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, dltbGeoColumnFamily, dltbGeoColumn)
      hbaseConf.set(TableInputFormat.INPUT_TABLE, pdtTableName)
      rddPDT = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, pdtGeoColumnFamily, pdtGeoColumn)
    } else if (args.length == 12) { // 如果进行重分区
      val partitionNum = args(11).toInt // 获得参数
      hbaseConf.set(TableInputFormat.INPUT_TABLE, dltbTableName)
      rddDLTB = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, dltbGeoColumnFamily, dltbGeoColumn, partitionNum)
      hbaseConf.set(TableInputFormat.INPUT_TABLE, pdtTableName)
      rddPDT = SpatialRDD.createSpatialRDDFromHBase(sc, hbaseConf, pdtGeoColumnFamily, pdtGeoColumn, partitionNum)
    }

    // 将PDT数据收集到主节点，按batchNumber个对象分批次广播
    val arrPDT = rddPDT.collect()
    val numberInBatch = Math.ceil(arrPDT.length.toDouble / numBatch).toInt
    println("每批次数据量：" + numberInBatch)

    // 遍历每个PDT表
    var rddDltbWithPdjb = rddDLTB.map(dltb => (dltb._1, (dltb._2, ("0", 0.0))))
    var rddDltbWithPdjbCache = rddDltbWithPdjb.cache()
    rddDltbWithPdjb.count() // 触发map操作
    (0 to numBatch).foreach(i => {
      // 获取该批次PDT数组进行广播
      val batchStart = i * numberInBatch
      var batchEnd = (i+1) * numberInBatch
      if (batchEnd > arrPDT.length) {
        batchEnd = arrPDT.length
      }
      val arrBatchPDT = util.Arrays.copyOfRange(arrPDT, batchStart, batchEnd)
      val bcBatchPDT = sc.broadcast(arrBatchPDT)
      // DLTB叠加PDT
      rddDltbWithPdjb = rddDltbWithPdjb.map(dltbWithPdjb => {
        var gson = new Gson()
        // 寻找最大重叠面积的坡度级别
        var pdjb = dltbWithPdjb._2._2._1
        var maxOverlayArea = dltbWithPdjb._2._2._2
        bcBatchPDT.value.foreach(pdt => {
          val valuesDltv = dltbWithPdjb._1.split("_")
          val valuesPdt = pdt._1.split("_")
          // 先判断两个是否属于相同区域
          val regionDltb = valuesDltv(1).substring(0, 6)
          val regionPdt = valuesPdt(1).substring(0, 6)
          if (regionDltb.equals(regionPdt)) {
            // 如果geohash是包含或相等关系，则两者可以判断为重叠
            val geohashDltb = valuesDltv(0)
            val geohashPdt = valuesPdt(0)
            if (geohashDltb.indexOf(geohashPdt) != -1 || geohashPdt.indexOf(geohashDltb) != -1) {
              if (dltbWithPdjb._2._1.geom.getEnvelopeInternal.intersects(pdt._2.geom.getEnvelopeInternal)) {
                val geomIntersect = pdt._2.geom.intersection(pdt._2.geom) // 多边形求交
                if (geomIntersect != null) {
                  val overlayArea = geomIntersect.getArea
                  if (overlayArea > maxOverlayArea) {
                    maxOverlayArea = overlayArea
                    pdjb = gson.fromJson(pdt._2.tags, classOf[JsonObject]).get(pdTagName).getAsString
                  }
                }
              }
            }
          }
        })
        // 输出每个DLTB对象对应的坡度等级
        (dltbWithPdjb._1, (dltbWithPdjb._2._1, (pdjb, maxOverlayArea)))
      })
      if (i < numBatch-1) {
        val rddDltbWithPdjbNewCache = rddDltbWithPdjb.cache()
        rddDltbWithPdjb.count() // 触发map操作
        rddDltbWithPdjbCache.unpersist()
        rddDltbWithPdjbCache = rddDltbWithPdjbNewCache
      }
    })

    // 输出结果到HDFS
    rddDltbWithPdjb
      .map(dltbWithPdjb => (dltbWithPdjb._2._1.oid, dltbWithPdjb._2._2._1))
      .saveAsTextFile(outputPath)
    rddDltbWithPdjb.unpersist()

    // 关闭Spark上下文
    sc.stop()

    // 结束时间，计算耗时
    val endTime = new Date().getTime
    println("耗时：" + (endTime - startTime) + "毫秒")
  }
}
