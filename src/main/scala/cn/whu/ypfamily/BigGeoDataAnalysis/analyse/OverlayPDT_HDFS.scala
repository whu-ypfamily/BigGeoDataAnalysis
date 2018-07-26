package cn.whu.ypfamily.BigGeoDataAnalysis.analyse

import java.util
import java.util.Date

import cn.whu.ypfamily.BigGeoDataAnalysis.rdd.SpatialRDD
import com.google.gson.{Gson, JsonObject}
import org.apache.spark.{SparkConf, SparkContext}

object OverlayPDT_HDFS {
  def main(args: Array[String]): Unit = {
    if (args.length < 5 || args.length > 6) {
      println("input " +
        "*<DLTB file path> " +
        "*<PDT file path> " +
        "*<batch number>" +
        "*<PD tag name> " +
        "*<output path>" +
        "<partition number>")
      return
    }

    // 开始时间
    val startTime = new Date().getTime

    // 获取参数
    val dltbFilePath = args(0)
    val pdtFilePath = args(1)
    val numBatch = args(2).toInt
    val pdTagName = args(3)
    val outputPath = args(4)

    // 设置Spark参数生成Spark上下文
    val sparkConf = new SparkConf().setAppName("OverlayPDT")
    val sc = new SparkContext(sparkConf)

    // 从HBase读取DLTB数据和PDT数据
    var rddDLTB: SpatialRDD = null
    var rddPDT: SpatialRDD = null
    if (args.length == 5) { // 如果不进行重分区
      rddDLTB = SpatialRDD.createSpatialRDDFromHDFS(sc, dltbFilePath)
      rddPDT = SpatialRDD.createSpatialRDDFromHDFS(sc, pdtFilePath)
    } else if (args.length == 6) { // 如果进行重分区
      val partitionNum = args(5).toInt // 获得参数
      rddDLTB = SpatialRDD.createSpatialRDDFromHDFS(sc, dltbFilePath, partitionNum)
      rddPDT = SpatialRDD.createSpatialRDDFromHDFS(sc, pdtFilePath, partitionNum)
    }

    // 将PDT数据收集到主节点，按numberInBatch个对象分批次广播
    val arrPDT = rddPDT.collect()
    val numberInBatch = Math.ceil(arrPDT.length.toDouble / numBatch).toInt
    println("每批次数据量：" + numberInBatch)

    // 遍历每个PDT批次
    var rddDltbWithPdjb = rddDLTB.map(dltb => (dltb._1, (dltb._2, ("0", 0.0))))
    var rddDltbWithPdjbCache = rddDltbWithPdjb.cache()
    rddDltbWithPdjb.count() // 触发map操作
    (0 until numBatch).foreach(i => {
      // 获取该批次PDT数组进行广播
      val batchStart = i * numberInBatch
      var batchEnd = (i+1) * numberInBatch
      if (batchEnd > arrPDT.length) {
        batchEnd = arrPDT.length
      }
      val arrBatchPDT = util.Arrays.copyOfRange(arrPDT, batchStart, batchEnd)
      val bcBatchPDT = sc.broadcast(arrBatchPDT)
      // DLTB叠加PDT
      rddDltbWithPdjb = rddDltbWithPdjbCache.map(dltbWithPdjb => {
        val gson = new Gson()
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
        // 更新每个DLTB对象对应的坡度等级和最大重叠面积
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

    // 关闭Spark上下文
    sc.stop()

    // 结束时间，计算耗时
    val endTime = new Date().getTime
    println("耗时：" + (endTime - startTime) + "毫秒")
  }
}
