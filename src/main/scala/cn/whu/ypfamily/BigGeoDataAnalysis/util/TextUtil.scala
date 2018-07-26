package cn.whu.ypfamily.BigGeoDataAnalysis.util

object TextUtil {
  /**
    * 寻找字符串数组最长公共前缀
    *
    * @param strings 字符串数组
    * @return
    */
  def longestCommonPrefix(strings: Array[String]): String = {
    if (strings.length == 0) {
      return ""
    }
    var prefixLen = 0
    while (prefixLen < strings(0).length) {
      val c = strings(0).charAt(prefixLen)
      var i = 1
      while (i < strings.length) {
        if (prefixLen >= strings(i).length || strings(i).charAt(prefixLen) != c) { // Mismatch found
          return strings(i).substring(0, prefixLen)
        }
        i += 1
      }
      prefixLen += 1
    }
    strings(0)
  }
}
