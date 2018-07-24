package cn.whu.ypfamily.BigGeoDataAnalysis.util

import ch.hsr.geohash.GeoHash
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.{WKTReader, WKTWriter}

/**
  * 地理对象工具类
  */
object GsUtil {

  /**
    * 几何对象转WKT字符串
    *
    * @param geom 几何对象
    * @return
    */
  def geometry2WKT(geom: Geometry): String = {
    new WKTWriter().write(geom)
  }

  /**
    * WKT字符串转几何对象
    *
    * @param wkt WKT字符串
    * @return
    */
  def wkt2Geometry(wkt: String): Geometry = {
    new WKTReader().read(wkt)
  }

  /**
    * 根据距离精度返回Geohash位数长度
    *
    * @param dis 距离精度
    * @return
    */
  def computeGeohashLengthByDistance(dis: Double): Int = {
    if (dis <= 0.0186) {
      12
    } else if (dis <= 0.1492) {
      11
    } else if (dis <= 0.5971) {
      10
    } else if (dis <= 4.78) {
      9
    } else if (dis <= 19.11) {
      8
    } else if (dis <= 76) {
      7
    } else if (dis <= 610) {
      6
    } else if (dis <= 2400) {
      5
    } else if (dis <= 20000) {
      4
    } else if (dis <= 78000) {
      3
    } else if (dis <= 630000) {
      2
    } else {
      1
    }
  }

  /**
    * 获得矩形框覆盖区域的GeoHash编码数组
    *
    * @param target 目标区域
    * @param ptMin  左下顶点坐标
    * @param ptMax  右上顶点坐标
    * @return
    */
  def getAllOverlayedGeoHash(target: GeoHash, ptMin: Point, ptMax: Point): Array[GeoHash] = {
    // 获得矩形框四个顶点和目标区域8个邻居的GeoHash编码
    val geoHashLength = target.toBase32.length
    val geoHashLD = GeoHash.withCharacterPrecision(ptMin.getY, ptMin.getX, geoHashLength).longValue()
    val geoHashLU = GeoHash.withCharacterPrecision(ptMax.getY, ptMin.getX, geoHashLength).longValue()
    val geoHashRU = GeoHash.withCharacterPrecision(ptMax.getY, ptMax.getX, geoHashLength).longValue()
    val geoHashRD = GeoHash.withCharacterPrecision(ptMin.getY, ptMax.getX, geoHashLength).longValue()
    val adjacents = target.getAdjacent
    // 判断并返回矩形框覆盖区域GeoHash编码
    if (geoHashLD == target.longValue() && geoHashLU == target.longValue()
      && geoHashRU == target.longValue() && geoHashRD == target.longValue()) { // 四个顶点都在目标区域
      Array(target)
    } else if (geoHashLD == adjacents(5).longValue) { // 左下顶点位于西南区域
      Array(target, adjacents(4), adjacents(5), adjacents(6))
    } else if (geoHashLD == adjacents(6).longValue()
      && geoHashLU == adjacents(6).longValue()) { // 左下和左上顶点都位于西区域
      Array(target, adjacents(6))
    } else if (geoHashLU == adjacents(7).longValue()) { //左上顶点位于西北区域
      Array(target, adjacents(0), adjacents(6), adjacents(7))
    } else if (geoHashLU == adjacents(0).longValue()
      && geoHashRU == adjacents(0).longValue()) { // 左上和右上顶点都位于北区域
      Array(target, adjacents(0))
    } else if (geoHashRU == adjacents(1).longValue()) { // 右上顶点位于东北区域
      Array(target, adjacents(0), adjacents(1), adjacents(2))
    } else if (geoHashRU == adjacents(2).longValue()
      && geoHashRD == adjacents(2).longValue()) { // 右上和右下顶点都位于东区域
      Array(target, adjacents(2))
    } else if (geoHashRD == adjacents(3).longValue()) { // 右下顶点位于东南区域
      Array(target, adjacents(2), adjacents(3), adjacents(4))
    } else if (geoHashRD == adjacents(4).longValue()
      && geoHashLD == adjacents(4).longValue()) { // 右下和左下顶点位于南区域
      Array(target, adjacents(4))
    } else {
      Array()
    }
  }

}
