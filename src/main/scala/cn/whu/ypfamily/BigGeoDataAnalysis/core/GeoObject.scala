package cn.whu.ypfamily.BigGeoDataAnalysis.core

import org.locationtech.jts.geom.Geometry


/**
  * 地理数据对象
  * @param _geom 几何对象
  * @param _oid 编号字符串
  * @param _tags 属性标签的JSON对象
  */
class GeoObject(_geom: Geometry, _oid: String, _tags: String) extends Serializable {

  var geom: Geometry = _geom
  var oid: String = _oid
  var tags: String = _tags

}
