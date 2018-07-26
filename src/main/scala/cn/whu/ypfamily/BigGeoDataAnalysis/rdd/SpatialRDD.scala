package cn.whu.ypfamily.BigGeoDataAnalysis.rdd

import cn.whu.ypfamily.BigGeoDataAnalysis.core.GeoObject
import cn.whu.ypfamily.BigGeoDataAnalysis.util.GsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.locationtech.jts.geom.Envelope

import scala.util.Random

/**
  * 一个用于处理空间大数据的RDD
  */
class SpatialRDD(val rddPrev: RDD[(String, GeoObject)])
  extends RDD[(String, GeoObject)](rddPrev) {

  override val partitioner: Option[Partitioner] = rddPrev.partitioner

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, GeoObject)] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)


  /**
    * 基于查询窗口的空间范围查询
    *
    * @param geoQueryWindow 空间查询窗口
    * @return 查询结果对象数组
    */
  def rangeQuery(geoQueryWindow: Envelope): SpatialRDD = {
    // 各节点进行过滤运算，筛选满足条件的地理数据
    new SpatialRDD(this.mapPartitions(partitions => {
      partitions.filter(geoObj => {
        var intersects = false
        val coordinates = geoObj._2.geom.getCoordinates
        var count = 0
        while (count < coordinates.length && !intersects) {
          if (geoQueryWindow.contains(coordinates(count))) {
            intersects = true
          }
          count = count + 1
        }
        intersects
      })
    }))
  }

  /**
    * 将SpatialRDD中的数据存储到HBase中
    *
    * @param hbaseConf      HBase参数
    * @param strTableName   要创建的表名
    * @param strGsFamily    空间数据在Hbase表中的列族名
    * @param strGsQualifier 空间数据在Hbase表中的列名
    */
  def saveAsHBaseTable(hbaseConf: Configuration,
                       strTableName: String,
                       strGsFamily: String,
                       strGsQualifier: String): Unit = {
    // 指定输出格式和输出表名
    val jobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, strTableName)

    // 将SpatialRDD中的每对数据转化成数据表中的一行
    val rddRow = this.map(data => {
      // 创建HBase每行数据
      val put = new Put(Bytes.toBytes(data._1))
      put.addColumn(Bytes.toBytes(strGsFamily), Bytes.toBytes(strGsQualifier), GsUtil.geometry2Wkb(data._2.geom))
      put.addColumn(Bytes.toBytes(strGsFamily), Bytes.toBytes("oid"), Bytes.toBytes(data._2.oid))
      put.addColumn(Bytes.toBytes(strGsFamily), Bytes.toBytes("tags"), Bytes.toBytes(data._2.tags.toString))
      // 输出格式
      (new ImmutableBytesWritable, put)
    })

    // 保存到HBase中
    rddRow.saveAsHadoopDataset(jobConf)
  }
}

object SpatialRDD {

  /**
    * 从HDFS生成SpatialRDD
    *
    * @param sparkContext Spark上下文
    * @param hdfsPath     HDFS文件路径
    * @return
    */
  def createSpatialRDDFromHDFS(sparkContext: SparkContext,
                               hdfsPath: String): SpatialRDD = {
    // 读取HDFS数据
    val rddHDFS = sparkContext.textFile(hdfsPath).filter(line => {
      val wkt = line.split("\t")(0)
      if (wkt.contains("POLYGON") || wkt.contains("LINESTRING") || wkt.contains("POINT")) {
        true
      } else {
        false
      }
    })

    // 解析空间对象
    val rddSpatialData = rddHDFS.map(line => {
      val values = line.split("\t")
      var geom = GsUtil.wkt2Geometry(values(0))
      if (geom != null) {
        if (!geom.isValid) { // 修正拓扑错误
          geom = geom.buffer(0)
        }
      }
      val strKey = GsUtil.geometry2Geohash(geom) + "_" + values(1)
      (strKey, new GeoObject(geom, values(1), values(2)))
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }

  /**
    * 从HDFS生成SpatialRDD，需设置partition数量
    *
    * @param sparkContext Spark上下文
    * @param hdfsPath     HDFS文件路径
    * @param numPartition 分区数
    * @return
    */
  def createSpatialRDDFromHDFS(sparkContext: SparkContext,
                               hdfsPath: String,
                               numPartition: Int): SpatialRDD = {
    // 读取HDFS数据
    var rddHDFSData = sparkContext.textFile(hdfsPath).filter(line => {
      val wkt = line.split("\t")(0)
      if (wkt.contains("POLYGON") || wkt.contains("LINESTRING") || wkt.contains("POINT")) {
        true
      } else {
        false
      }
    }).map(line => {
      val rand = new Random()
      val key = rand.nextInt(numPartition)
      (key, line)
    })

    // 重分区
    rddHDFSData = rddHDFSData.repartition(numPartition)

    // 解析空间对象
    val rddSpatialData = rddHDFSData.map(line => {
      val values = line._2.split("\t")
      var geom = GsUtil.wkt2Geometry(values(0))
      if (geom != null) {
        if (!geom.isValid) { // 修正拓扑错误
          geom = geom.buffer(0)
        }
      }
      val strKey = GsUtil.geometry2Geohash(geom) + "_" + values(1)
      (strKey, new GeoObject(geom, values(1), values(2)))
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }

  /**
    * 从HBase生成SpatialRDD
    *
    * @param sparkContext   Spark上下文
    * @param hbaseConf      HBase参数
    * @param strGsFamily    空间数据在Hbase表中的列族名
    * @param strGsQualifier 空间数据在Hbase表中的列名
    * @return
    */
  def createSpatialRDDFromHBase(sparkContext: SparkContext,
                                hbaseConf: Configuration,
                                strGsFamily: String,
                                strGsQualifier: String): SpatialRDD = {
    // 从HBase获取每行数据
    var rddHBaseData = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 从每行数据中解析出空间对象
    val rddSpatialData = rddHBaseData.map(data => {
      val strKey = Bytes.toString(data._2.getRow)
      val wkb = data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes(strGsQualifier))
      val oid = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("oid")))
      val tags = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("tags")))
      val geom = GsUtil.wkb2Geometry(wkb)
      (strKey, new GeoObject(geom, oid, tags))
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }

  /**
    * 从HBase生成SpatialRDD，需设置partition数量
    *
    * @param sparkContext   Spark上下文
    * @param hbaseConf      HBase参数
    * @param strGsFamily    空间数据在Hbase表中的列族名
    * @param strGsQualifier 空间数据在Hbase表中的列名
    * @param numPartition   分区数
    * @return
    */
  def createSpatialRDDFromHBase(sparkContext: SparkContext,
                                hbaseConf: Configuration,
                                strGsFamily: String,
                                strGsQualifier: String,
                                numPartition: Int): SpatialRDD = {
    // 从HBase获取每行数据
    var rddHBaseData = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(data => { // 从每行数据中解析出空间对象
        val rand = new Random()
        val key = rand.nextInt(numPartition)
        val strKey = Bytes.toString(data._2.getRow)
        val wkb = data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes(strGsQualifier))
        val oid = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("oid")))
        val tags = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("tags")))
        val geom = GsUtil.wkb2Geometry(wkb)
        (key, (strKey, new GeoObject(geom, oid, tags)))
      })

    // 重分区
    rddHBaseData = rddHBaseData.repartition(numPartition)

    // 验证几何对象
    val rddSpatialData = rddHBaseData.map(data => {
      if (!data._2._2.geom.isValid) {
        data._2._2.geom = data._2._2.geom.buffer(0)
      }
      (data._2._1, data._2._2)
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }

  /**
    * 从HBase给定Geohash范围生成SpatialRDD
    *
    * @param sparkContext   Spark上下文
    * @param hbaseConf      HBase参数
    * @param targets        目标区域Geohash编码
    * @param strGsFamily    空间数据在Hbase表中的列族名
    * @param strGsQualifier 空间数据在Hbase表中的列名
    * @return
    */
  def createSpatialRDDFromHBase(sparkContext: SparkContext,
                                hbaseConf: Configuration,
                                targets: Array[String],
                                strGsFamily: String,
                                strGsQualifier: String): SpatialRDD = {
    val rddList = new Array[RDD[(ImmutableBytesWritable, Result)]](targets.length)
    // 从HBase获取目标区域的数据
    targets.distinct.indices.foreach(i => {
      val prefix = targets(i)
      val scan = new Scan(prefix.getBytes())
      scan.addColumn(strGsFamily.getBytes, strGsQualifier.getBytes)
      scan.setFilter(new PrefixFilter(prefix.getBytes()))
      scan.setCaching(100)
      val proto = ProtobufUtil.toScan(scan)
      val strScan = Base64.encodeBytes(proto.toByteArray)
      hbaseConf.set(TableInputFormat.SCAN, strScan)
      rddList(i) = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
    })

    // 从每行数据中解析出空间对象
    val rddSpatialData = rddList.filter(_ != null).reduce(_.union(_)).map(data => {
      val strKey = Bytes.toString(data._2.getRow)
      val wkb = data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes(strGsQualifier))
      val oid = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("oid")))
      val tags = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("tags")))
      val geom = GsUtil.wkb2Geometry(wkb)
      (strKey, new GeoObject(geom, oid, tags))
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }

  /**
    * 从HBase给定Geohash范围生成SpatialRDD，需设置partition数量
    *
    * @param sparkContext   Spark上下文
    * @param hbaseConf      HBase参数
    * @param targets        目标区域Geohash编码
    * @param strGsFamily    空间数据在Hbase表中的列族名
    * @param strGsQualifier 空间数据在Hbase表中的列名
    * @param numPartition   分区数
    * @return
    */
  def createSpatialRDDFromHBase(sparkContext: SparkContext,
                                hbaseConf: Configuration,
                                targets: Array[String],
                                strGsFamily: String,
                                strGsQualifier: String,
                                numPartition: Int): SpatialRDD = {
    val rddList = new Array[RDD[(Int, Result)]](targets.length)
    // 从HBase获取目标区域的数据
    targets.distinct.indices.foreach(i => {
      val prefix = targets(i)
      val scan = new Scan(prefix.getBytes())
      scan.addColumn(strGsFamily.getBytes, strGsQualifier.getBytes)
      scan.setFilter(new PrefixFilter(prefix.getBytes()))
      val proto = ProtobufUtil.toScan(scan)
      val strScan = Base64.encodeBytes(proto.toByteArray)
      hbaseConf.set(TableInputFormat.SCAN, strScan)
      rddList(i) = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result]).coalesce(numPartition)
        .map(data => {
          val rand = new Random()
          val key = rand.nextInt(numPartition)
          (key, data._2)
        })
    })

    // 合并重分区
    var rddHBaseData = rddList.filter(_ != null).reduce(_.union(_))
    rddHBaseData = rddHBaseData.repartition(numPartition)

    // 从每行数据中解析出空间对象
    val rddSpatialData = rddHBaseData.map(data => {
      val strKey = Bytes.toString(data._2.getRow)
      val wkb = data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes(strGsQualifier))
      val oid = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("oid")))
      val tags = Bytes.toString(data._2.getValue(Bytes.toBytes(strGsFamily), Bytes.toBytes("tags")))
      val geom = GsUtil.wkb2Geometry(wkb)
      (strKey, new GeoObject(geom, oid, tags))
    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }
}