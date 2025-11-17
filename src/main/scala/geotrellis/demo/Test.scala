package geotrellis.demo
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.{MutableArrayTile, isNoData}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.density.RDDKernelDensity
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.LayerId
import geotrellis.vector._
import org.apache.spark.rdd.RDD

import scala.util._

object Test {
    import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp

    /***
     * 自定义瓦片加法操作
     * 自定义瓦片合并逻辑，正确处理无数据值
     * 确保在合并瓦片时，无数据值不会影响有效数据的累加
     */
    object Adder extends LocalTileBinaryOp {
        def combine(z1: Int, z2: Int) = {
            if (isNoData(z1)) {
                z2  // 如果z1是无数据，返回z2
            } else if (isNoData(z2)) {
                z1  // 如果z2是无数据，返回z1
            } else {
                z1 + z2  // 否则相加
            }
        }

        def combine(r1: Double, r2: Double) = {
            if (isNoData(r1)) {
                r2
            } else if (isNoData(r2)) {
                r1
            } else {
                r1 + r2
            }
        }
    }

    def sumTiles(t1: MutableArrayTile, t2: MutableArrayTile): MutableArrayTile = {
        Adder(t1, t2).asInstanceOf[MutableArrayTile]
    }

    def main(args: Array[String]): Unit = {
        /**
         * 第一部分：生成带权重的随机点要素（核密度估计的输入数据）
         * 核密度估计的核心是“带权重的点”——每个点对周围区域的密度贡献由权重和核函数决定
         */
        // 定义目标区域范围：经纬度范围（xmin, ymin, xmax, ymax）
        val extent = Extent(-109, 37, -102, 41)

        /**
         * 生成单个随机点要素的工具函数
         * @param extent 点的生成范围（确保点落在目标区域内）
         * @return PointFeature[Double]：几何为点，属性为权重（Double类型）
         */
        def randomPointFeature(extent: Extent): PointFeature[Double] = {
            // 辅助函数：生成[low, high]区间内的随机Double值
            def randInRange(low: Double, high: Double): Double = {
                val randomFactor = Random.nextDouble() // 生成0~1的随机数
                low * (1 - randomFactor) + high * randomFactor // 线性插值得到目标区间随机值
            }

            // 创建点要素：几何是随机点，属性是16~31之间的随机权重（模拟不同强度的点影响）
            Feature(
                geom = Point(
                    x = randInRange(extent.xmin, extent.xmax), // 经度随机
                    y = randInRange(extent.ymin, extent.ymax)  // 纬度随机
                ),
                data = Random.nextInt % 16 + 16 // 权重计算：Random.nextInt是正负整数，取模16后+16，确保范围[16,31]
            )
        }

        // 生成1000个带权重的随机点要素，作为核密度估计的输入数据集
        val pts = (for (i <- 1 to 1000) yield randomPointFeature(extent)).toList

        /**
         * 第二部分：基础版——整体式核密度计算（适合小规模数据）
         * 直接生成一个完整的栅格瓦片，不进行分块处理
         */
        // 核函数宽度（像素数）：决定每个点的影响范围覆盖9×9个像素
        val kernelWidth: Int = 9

        // 创建高斯核函数：参数（核宽度，标准差，振幅）
        // 高斯核的特点是“中心影响强，边缘影响弱”，符合自然现象的衰减规律
        val kern: Kernel = Kernel.gaussian(
            size = kernelWidth,    // 核的像素宽度（必须与kernelWidth一致）
            sigma = 1.5,            // 标准差：越小，影响范围越集中；越大，影响越分散
            amp = 25          // 振幅：核函数的最大值，影响密度值的整体强度
        )
        import geotrellis.raster._

        // 执行整体核密度计算
        // RasterExtent(extent, 700, 400)：定义输出栅格的范围和分辨率（700列×400行像素）
        val kde: Tile = pts.kernelDensity(
            kernel = kern,
            rasterExtent = RasterExtent(extent, 700, 400)
        )

        println("kde1:" + kde.findMinMax._1)
        println("kde2:" + kde.findMinMax._2)

        /**
         * 输出基础版结果：PNG图片（可视化）和GeoTIFF（空间参考保留）
         */
        // 创建颜色映射：将密度值映射为热力图颜色（蓝→黄→红，密度递增）
        val colorMap = ColorMap(
            breaks = (0 to kde.findMinMax._2 by 4).toArray, // 颜色分割点：从0到最大密度值，每4个单位分一段
            colorRamp = ColorRamps.HeatmapBlueToYellowToRedSpectrum // 热力图配色方案
        )

        // 输出PNG图片（无空间参考，仅用于快速可视化）
        kde.renderPng(colorMap).write("output\\test.png")

        // 输出GeoTIFF文件（包含范围和CRS，可在QGIS等GIS软件中叠加显示）
         GeoTiff(
            tile = kde,
            extent = extent,
            crs = LatLng // 坐标参考系：WGS84经纬度（EPSG:4326）
        ).write("output\\test.tif")

        /**
         * 第三部分：进阶版——瓦片细分式核密度计算（适合大规模数据）
         * 核心思想：将大栅格拆分为小瓦片，并行计算每个瓦片的密度，最后拼接为完整结果
         * 解决大规模数据内存不足、计算效率低的问题
         */
        // 1. 定义瓦片布局（TileLayout）：描述大栅格如何拆分为小瓦片
        val tl = TileLayout(
            layoutCols = 7,    // 瓦片网格的列数（水平方向瓦片数）
            layoutRows = 4,    // 瓦片网格的行数（垂直方向瓦片数）
            tileCols = 100,    // 单个瓦片的列数（100像素宽）
            tileRows = 100     // 单个瓦片的行数（100像素高）
        )
        // 最终拆分结果：7×4=28个瓦片，每个100×100像素，总分辨率700×400（与基础版一致）

        // 2. 创建布局定义（LayoutDefinition）：关联瓦片布局与地理范围
        // 包含瓦片分割规则（tl）和整体地理范围（extent），是分块处理的核心配置
        val ld = LayoutDefinition(extent, tl)

        /**
         * 3. 工具函数：计算点要素的核影响范围（地理范围）
         * 核函数的影响范围是9个像素宽的正方形，需转换为对应的地理范围（与瓦片布局匹配）
         */
        def pointFeatureToExtent[D](
                                       kwidth: Double,    // 核宽度（像素数，此处固定为9）
                                       ld: LayoutDefinition, // 布局定义（提供像素的地理尺寸）
                                       ptf: PointFeature[D]  // 输入点要素
                                   ): Extent = {
            val p = ptf.geom // 获取点的地理坐标（x=经度，y=纬度）

            // 计算核范围的四个边界（以点为中心，向四周扩展）
            Extent(
                xmin = p.x - kwidth * ld.cellwidth / 2, // 左边界：点经度 - 核宽度的一半（地理距离）
                ymin = p.y - kwidth * ld.cellheight / 2, // 下边界：点纬度 - 核宽度的一半（地理距离）
                xmax = p.x + kwidth * ld.cellwidth / 2, // 右边界：点经度 + 核宽度的一半（地理距离）
                ymax = p.y + kwidth * ld.cellheight / 2  // 上边界：点纬度 + 核宽度的一半（地理距离）
            )
        }

        // 简化函数：固定核宽度为9，直接传入点要素即可获取核影响范围
        def ptfToExtent[D](p: PointFeature[D]): Extent = pointFeatureToExtent(9, ld, p)

        /**
         * 4. 工具函数：将点要素映射到其核范围所重叠的所有瓦片（SpatialKey）
         * 一个点的核范围可能覆盖多个瓦片，需明确每个点对哪些瓦片有影响
         */
        def ptfToSpatialKey[D](ptf: PointFeature[D]): Seq[(SpatialKey, PointFeature[D])] = {
            val ptextent = ptfToExtent(ptf) // 步骤1：获取点的核影响范围（地理范围）
            // 步骤2：将地理范围转换为瓦片索引范围（GridBounds）
            // ld.mapTransform：Geotrellis的核心转换工具，实现地理坐标↔瓦片索引的映射
            val gridBounds = ld.mapTransform(ptextent)

            // 步骤3：迭代所有重叠的瓦片索引，生成（SpatialKey，点要素）对
            for {
                (col, row) <- gridBounds.coordsIter.toSeq // 迭代瓦片索引（列，行）
                if row < tl.totalRows // 过滤：行索引不超过总瓦片行数（避免越界）
                if col < tl.totalCols // 过滤：列索引不超过总瓦片列数（避免越界）
            } yield (SpatialKey(col, row), ptf) // SpatialKey：瓦片的唯一索引（列，行）
        }

        /**
         * 5. 按瓦片分组：收集每个瓦片上所有受影响的点要素
         * 输出格式：Map[SpatialKey, List[PointFeature[Double]]]
         * 键：瓦片索引；值：该瓦片上所有需要参与密度计算的点要素
         */
        val keyfeatures: Map[SpatialKey, List[PointFeature[Double]]] =
            pts
                .flatMap(ptfToSpatialKey) // 展开：每个点→多个（瓦片索引，点）对（覆盖所有重叠瓦片）
                .groupBy(_._1) // 按瓦片索引（SpatialKey）分组
                .map { case (spatialKey, keyPointPairs) =>
                    // 提取每组中的点要素列表（忽略重复的瓦片索引）
                    (spatialKey, keyPointPairs.unzip._2)
                }

        /**
         * 6. 分块计算：对每个瓦片独立执行核密度估计
         * 每个瓦片的计算逻辑与基础版一致，但仅处理影响该瓦片的点要素
         */
        val keytiles: Map[SpatialKey, Tile] = keyfeatures.map { case (spatialKey, pointFeatures) =>
            (
                spatialKey,
                // 对当前瓦片的点要素执行核密度计算
                pointFeatures.kernelDensity(
                    kernel = kern,
                    // 定义当前瓦片的栅格范围：通过SpatialKey获取瓦片的地理范围，分辨率为100×100
                    rasterExtent = RasterExtent(
                        extent = ld.mapTransform(spatialKey), // 瓦片的地理范围
                        cols = tl.tileDimensions._1,         // 瓦片列数（100）
                        rows = tl.tileDimensions._2          // 瓦片行数（100）
                    )
                )
            )
        }

        /**
         * 7. 瓦片拼接：将28个分块瓦片合并为完整的700×400栅格
         * 确保所有瓦片都参与拼接（包括无点影响的空瓦片），避免范围缺失
         */
        val tileList =
            for {
                row <- 0 until ld.layoutRows // 迭代所有瓦片行（0~3）
                col <- 0 until ld.layoutCols // 迭代所有瓦片列（0~6）
            } yield {
                val spatialKey = SpatialKey(col, row) // 生成当前瓦片的索引
                // 若该瓦片无受影响的点，用空瓦片填充（避免拼接后出现空白区域）
                (spatialKey, keytiles.getOrElse(spatialKey, IntArrayTile.empty(tl.tileCols, tl.tileRows)))
            }

        // 执行瓦片拼接：TileLayoutStitcher是Geotrellis的瓦片拼接工具
        val stitched: Tile = TileLayoutStitcher.stitch(tileList)._1

        /**
         * 输出进阶版结果：拼接后的瓦片保存为PNG
         * 最终结果与基础版完全一致，但支持大规模数据的并行处理
         */
        stitched.renderPng(colorMap).write("output\\dis.png")





        //Spark
        import org.apache.spark.{SparkConf, SparkContext}

        val conf = new SparkConf().setMaster("local").setAppName("Kernel Density")
        val sc = new SparkContext(conf)

        val pointRdd = sc.parallelize(pts, 10)


        import geotrellis.raster.density.KernelStamper

        def stampPointFeature(
                               tile: MutableArrayTile,
                               tup: (SpatialKey, PointFeature[Double])
                             ): MutableArrayTile = {
            val (spatialKey, pointFeature) = tup
            val tileExtent = ld.mapTransform(spatialKey)
            val re = RasterExtent(tileExtent, tile)
            val result = tile.copy.asInstanceOf[MutableArrayTile]

            KernelStamper(result, kern)
              .stampKernelDouble(re.mapToGrid(pointFeature.geom), pointFeature.data)

            result
        }




        val function: ((MutableArrayTile, (SpatialKey, PointFeature[Double])) => MutableArrayTile, (MutableArrayTile, MutableArrayTile) => MutableArrayTile) => RDD[(SpatialKey, MutableArrayTile)] = pointRdd
          .flatMap(ptfToSpatialKey)
          .mapPartitions(
              { partition =>
                  partition.map { case (spatialKey, pointFeature) =>
                      (spatialKey, (spatialKey, pointFeature))
                  }
              }, preservesPartitioning = true)
          .aggregateByKey(ArrayTile.empty(DoubleCellType, ld.tileCols, ld.tileRows))
        val tileRdd: RDD[(SpatialKey, Tile)] =
            function(stampPointFeature, sumTiles)
              .mapValues { tile: MutableArrayTile => tile.asInstanceOf[Tile] }
        import geotrellis.layer.stitch.TileLayoutStitcher

        {
            val tuples: Array[(SpatialKey, Tile)] = tileRdd.collect()
            val stitched: Tile = TileLayoutStitcher.stitch(tuples)._1

            stitched.renderPng(colorMap).write("output\\tt.png")
            //    val value: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = RDDKernelDensity(pointRdd, ld, kern, CRS.fromEpsgCode(4326))

        }

        {
            // 注释掉的代码：使用GeoTrellis内置的核密度函数
            // 与手动实现相比，内置函数更简洁但灵活性较低
            val value: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] =
            RDDKernelDensity(pointRdd, ld, kern, CRS.fromEpsgCode(4326))
            val tuples: Array[(SpatialKey, Tile)] = value.collect()
            val stitched: Tile = TileLayoutStitcher.stitch(tuples)._1

            stitched.renderPng(colorMap).write("output\\tt1.png")
            //    val value: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = RDDKernelDensity(pointRdd, ld, kern, CRS.fromEpsgCode(4326))

        }

    }

}