package geotrellis.utils

import geotrellis.raster.render.{ColorMap, ColorRamp, ColorRamps, GreaterThanOrEqualTo, RGB}

/**
 * 地图渲染颜色映射
 * 时间：2022年9月28日20:08:37
 */
object RenderColorMap {
  val rampMap: Map[String,ColorRamp] =
    Map(
      "blue-to-orange" -> ColorRamps.BlueToOrange,
      "green-to-orange" -> ColorRamps.LightYellowToOrange,
      "blue-to-red" -> ColorRamps.BlueToRed,
      "green-to-red-orange" -> ColorRamps.GreenToRedOrange,
      "light-to-dark-sunset" -> ColorRamps.LightToDarkSunset,
      "light-to-dark-green" -> ColorRamps.LightToDarkGreen,
      "yellow-to-red-heatmap" -> ColorRamps.HeatmapYellowToRed,
      "blue-to-yellow-to-red-heatmap" -> ColorRamps.HeatmapBlueToYellowToRedSpectrum,
      "dark-red-to-yellow-heatmap" -> ColorRamps.HeatmapDarkRedToYellowWhite,
      "purple-to-dark-purple-to-white-heatmap" -> ColorRamps.HeatmapLightPurpleToDarkPurpleToWhite,
      "bold-land-use-qualitative" -> ColorRamps.ClassificationBoldLandUse,
      "muted-terrain-qualitative" -> ColorRamps.ClassificationMutedTerrain
    )

  def get(string: String): Option[ColorRamp] = rampMap.get(string)

  def getOrElse(string: String,colorRamp: ColorRamp): ColorRamp = rampMap.getOrElse(string,colorRamp)

  val rgbColorMap =
    ColorMap(
      Map(
        3.5 -> RGB(0,255,0),
        7.5 -> RGB(63,255,51),
        11.5 -> RGB(102,255,102),
        15.5 -> RGB(178,255,102),
        19.5 -> RGB(255,255,0),
        23.5 -> RGB(255,255,51),
        26.5 -> RGB(255,153,51),
        31.5 -> RGB(255,128,0),
        35.0 -> RGB(255,51,51),
        40.0 -> RGB(255,0,0)
      ),
      ColorMap.Options(
        classBoundaryType = GreaterThanOrEqualTo,
        noDataColor = 0x00000000, //无值颜色，0x00表示透明
        fallbackColor = 0x00000000 //不在映射范围内颜色
      )
    )
}
