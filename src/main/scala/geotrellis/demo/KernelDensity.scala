package geotrellis.demo

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.density.RDDKernelDensity
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.LayerId
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector._
import org.apache.spark.rdd.RDD

import scala.util._


object KernelDensity extends Serializable{

  def main(args: Array[String]): Unit = {
    val path = "D:\\code\\hopechart\\geo\\GeoTrellis\\"
    """
      |首先，让我们生成一组随机点，这些点的权重在（0，32）范围内。
      |为了体现 “带权重的点” 这一概念，我们创建了一个 PointFeature [Double]（它是 Feature [Point, Double] 的别名）。
      |一般来说，要素（Features）以类型安全的方式将几何图形和属性值结合在一起。
      |""".stripMargin
    val extent = Extent(-109, 37, -102, 41) // Extent of Colorado

    def randomPointFeature(extent: Extent): PointFeature[Double] = {
      def randInRange(low: Double, high: Double): Double = {
        val x = Random.nextDouble
        low * (1 - x) + high * x
      }

      Feature(Point(randInRange(extent.xmin, extent.xmax), // the geometry
        randInRange(extent.ymin, extent.ymax)),
        Random.nextInt % 16 + 16) // the weight (attribute)
    }

    val pts = (for (i <- 1 to 10000) yield randomPointFeature(extent)).toList

    """
      |在这个例子中，范围的选择在很大程度上是任意的，但要注意这里的坐标是相对于我们通常所考虑的标准（经度，纬度）来选取的。
      |还有其他的坐标表示方式，如果你想了解更多细节，研究坐标参考系统（CRSs）可能会很有用。GeoTrellis 中的一些操作要求构建一个 CRS 对象，
      |以便将你的栅格数据置于合适的环境中。对于（经度，纬度）坐标，使用 geotrellis.proj4.CRS.fromName("EPSG:4326")
      |或者 geotrellis.proj4.LatLng 都能生成所需的 CRS。接下来，我们将创建一个包含核密度估计的瓦片：
      |""".stripMargin
    import geotrellis.raster._
    import geotrellis.raster.mapalgebra.focal.Kernel

    val kernelWidth: Int = 9

    /* Gaussian kernel with std. deviation 1.5, amplitude 25 */
    val kern: Kernel = Kernel.gaussian(kernelWidth, 1.5, 25)

    //二维像素数组，无空间参考信息	数据存储基本单元，支持各种数学运算
    val kde: Tile = pts.kernelDensity(kern, RasterExtent(extent, 700, 400))
    """
      |这会用所需的核密度估计填充一个700x400的瓦片。为了查看生成的文件，一个简单的方法是将瓦片输出为PNG或TIFF格式。
      |在下面的代码片段中，会在启动sbt的目录（工作目录）中创建一个PNG文件，其中瓦片的值被映射为从蓝色到黄色再到红色的平滑过渡颜色。
      |""".stripMargin

    val colorMap = ColorMap(
      (0 to kde.findMinMax._2 by 4).toArray,
      ColorRamps.HeatmapBlueToYellowToRedSpectrum
    )

    kde.renderPng(colorMap).write("output\\test.png")

    """
      |使用TIFF输出的优势在于，它会标记范围和坐标参考系（CRS），
      |并且生成的图像文件可以在诸如QGIS之类的查看程序中叠加到地图上。该输出由以下语句生成。
      |""".stripMargin


    import geotrellis.raster.io.geotiff._

    GeoTiff(kde, extent, LatLng).write("output\\test.tif")


    """
      |瓦片细分
      |上面的示例聚焦于一个创建小型栅格对象的简单问题。然而，GeoTrellis的目的是支持大规模数据集的处理。
      |为了处理大型栅格数据，有必要将一个区域细分为瓦片网格，这样每一部分的处理就可以由不同的处理器来完成，
      |例如，这些处理器可能位于集群中的远程机器上。本节将解释将栅格细分为一组瓦片所涉及的一些概念。
      |
      |我们仍将使用Extent对象来设定我们的栅格块在空间中的边界，
      |但现在我们必须指定该范围如何被分割成瓦片网格。这需要采用如下形式的语句：
      |""".stripMargin

    import geotrellis.spark.tiling._

    val tl = TileLayout(7, 4, 1000, 1000)
    """
      |在这里，我们指定了一个7×4的瓦片网格，每个瓦片包含100×100个单元格。
      |这最终将用于把之前的整体式700×400瓦片（kde）划分成28个大小一致的子瓦片。
      |然后，瓦片布局会与范围组合到一个LayoutDefinition对象中：
      |""".stripMargin
    val ld = LayoutDefinition(extent, tl)

    """
      |为了准备用这种结构重新实现之前的核密度估计，我们注意到pts中的每个点都位于一个核的中心，
      |该核覆盖了这些点周围一些非零区域。我们可以将每个点/核对视为以相关点为中心的小正方形范围，
      |其边长为9个像素（这是我们之前为核选择的任意宽度）。然而，每个像素都覆盖了地图的一些非零区域，
      |我们也可以将其视为一个范围，其边长由地图坐标给出。
      |像素范围的尺寸由LayoutDefinition对象的cellwidth（单元格宽度）和cellheight（单元格高度）成员给出。
      |
      |通过整合所有这些想法，我们可以创建以下函数来生成以给定点为中心的核的范围：
      |""".stripMargin

    def pointFeatureToExtent[D](kwidth: Double, ld: LayoutDefinition, ptf: PointFeature[D]): Extent = {
      val p = ptf.geom

      Extent(p.x - kwidth * ld.cellwidth / 2,
        p.y - kwidth * ld.cellheight / 2,
        p.x + kwidth * ld.cellwidth / 2,
        p.y + kwidth * ld.cellheight / 2)
    }

    def ptfToExtent[D](p: PointFeature[D]) = pointFeatureToExtent(9, ld, p)

    """
      |当我们在布局定义的背景下考虑一个点的核范围时，很明显，
      |一个核的范围可能会与布局中的多个瓦片重叠。为了找出给定的点的核范围所重叠的瓦片，L
      |ayoutDefinition提供了一个mapTransform对象。
      |mapTransform的方法中包含确定TileLayout中与给定范围重叠的子瓦片索引的功能。
      |请注意，特定布局中的瓦片是通过Spatialkey进行索引的，Spatialkey实际上是（Int，Int）对，
      |它们按如下方式给出每个瓦片的（列，行）：
      |
      |+-------+-------+-------+       +-------+
      || (0,0) | (1,0) | (2,0) | . . . | (6,0) |
      |+-------+-------+-------+       +-------+
      || (0,1) | (1,1) | (2,1) | . . . | (6,1) |
      |+-------+-------+-------+       +-------+
      |    .       .       .     .         .
      |    .       .       .       .       .
      |    .       .       .         .     .
      |+-------+-------+-------+       +-------+
      || (0,3) | (1,3) | (2,3) | . . . | (6,3) |
      |+-------+-------+-------+       +-------+
      |""".stripMargin

    """
      |具体来说，在我们的运行示例中，1d.mapTransform(ptfToExtent(Feature(Point(-108, 38), 100.0)))
      |返回的是网格边界（0, 2, 1, 3），
      |这表明列在[0,1]范围内且行在[2,3]范围内的每个单元格，
      |都与以点（-108, 38）为中心的核函数相交——也就是说，是布局左下角的2x2瓦片块。
      |
      |为了继续进行核密度估计，需要将点列表转换为（SpatialKey，List[PointFeature[Double]]）的集合，
      |该集合会收集所有对每个子瓦片有影响的点，并按它们的空间键（SpatialKey）进行索引。以下代码片段实现了这一点。
      |""".stripMargin

    import geotrellis.spark._

    def ptfToSpatialKey[D](ptf: PointFeature[D]): Seq[(SpatialKey, PointFeature[D])] = {
      val ptextent = ptfToExtent(ptf)
      val gridBounds = ld.mapTransform(ptextent)

      for {
        (c, r) <- gridBounds.coordsIter.toSeq
        if r < tl.totalRows
        if c < tl.totalCols
      } yield (SpatialKey(c, r), ptf)
    }

    val keyfeatures: Map[SpatialKey, List[PointFeature[Double]]] =
      pts
        .flatMap(ptfToSpatialKey)
        .groupBy(_._1)
        .map { case (sk, v) => (sk, v.unzip._2) }


    """
      |现在，所有的子瓦片都可以按照上述整体瓦片的方式生成。
      |""".stripMargin
    val keytiles = keyfeatures.map { case (sk, pfs) =>
      (sk, pfs.kernelDensity(
        kern,
        RasterExtent(ld.mapTransform(sk), tl.tileDimensions._1, tl.tileDimensions._2)
      ))
    }
    """
      |最后，有必要将结果结合起来。请注意，为了生成一个与更简单的非分块情况完全相同的700x400分块，
      |每个空间键都必须在地图中呈现，否则结果可能无法覆盖完整的范围。
      |只有当生成一个覆盖完整范围的分块很重要时，这一点才是必要的。
      |""".stripMargin


    val tileList =
      for {
        r <- 0 until ld.layoutRows
        c <- 0 until ld.layoutCols
      } yield {
        val k = SpatialKey(c, r)
        (k, keytiles.getOrElse(k, IntArrayTile.empty(tl.tileCols, tl.tileRows)))
      }

    val stitched = TileLayoutStitcher.stitch(tileList)._1
    stitched.renderPng(colorMap).write( "output\\dis.png")








  }

}
