package cn.whu.ypfamily.BigGeoDataAnalysis.analyse

import java.util.Date

import cn.whu.ypfamily.BigGeoDataAnalysis.rdd.SpatialRDD
import com.google.gson.{Gson, JsonObject}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object OverlayPDT_HDFS {
  def main(args: Array[String]): Unit = {
    if (args.length < 7 || args.length > 8) {
      println("input " +
        "*<hdfs path> " +
        "*<zookeeper server list> " +
        "*<DLTB file path> " +
        "*<PDT file path prefix> " +
        "*<PDT file number>" +
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
    val dltbFilePath = args(2)
    val pdtFilePathPrefix = args(3)
    val pdtFileNumber = args(4).toInt
    val pdTagName = args(5)
    val outputPath = args(6)

    // 设置Spark参数生成Spark上下文
    val sparkConf = new SparkConf().setAppName("OverlayPDT")
    val sc = new SparkContext(sparkConf)

    // 从HBase读取DLTB数据
    var rddDLTB: SpatialRDD = null
    if (args.length == 7) { // 如果不进行重分区
      rddDLTB = SpatialRDD.createSpatialRDDFromHDFS(sc, dltbFilePath)
    } else if (args.length == 8) { // 如果进行重分区
      val partitionNum = args(7).toInt // 获得参数
      rddDLTB = SpatialRDD.createSpatialRDDFromHDFS(sc, dltbFilePath, partitionNum)
    }

    // 遍历每个PDT文件
    var rddDltbWithPdjb = rddDLTB.map(dltb => (dltb._1, (dltb._2, ("0", 0.0))))
    var rddPDT: SpatialRDD = null
    (0 to pdtFileNumber).foreach(i => {
      val pdtFilePath = pdtFilePathPrefix + i
      // 从HDFS读取PDT数据
      if (args.length == 7) { // 如果不进行重分区
        rddPDT = SpatialRDD.createSpatialRDDFromHDFS(sc, pdtFilePath)
      } else if (args.length == 8) { // 如果进行重分区
        val partitionNum = args(7).toInt // 获得参数
        rddPDT = SpatialRDD.createSpatialRDDFromHDFS(sc, pdtFilePath, partitionNum)
      }
      // 对PDT数据进行广播
      val arrPDT = rddPDT.collect()
      val bcPDT = sc.broadcast(arrPDT)
      // DLTB叠加PDT
      rddDltbWithPdjb = rddDltbWithPdjb.map(dltbWithPdjb => {
        var pdjb = "0"
        var gson = new Gson()
        // 寻找最大重叠面积的坡度级别
        var maxOverlayArea = dltbWithPdjb._2._2._2
        bcPDT.value.foreach(pdt => {
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
      bcPDT.unpersist()
      bcPDT.destroy()
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
