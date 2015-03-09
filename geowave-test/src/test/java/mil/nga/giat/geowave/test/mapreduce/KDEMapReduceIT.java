package mil.nga.giat.geowave.test.mapreduce;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;

import mil.nga.giat.geowave.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.analytics.mapreduce.kde.KDEJobRunner;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.raster.RasterUtils;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterConfig;
import mil.nga.giat.geowave.raster.plugin.GeoWaveRasterReader;
import mil.nga.giat.geowave.raster.resize.RasterTileResizeJobRunner;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.geometry.GeneralEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.coverage.grid.GridCoverage;

public class KDEMapReduceIT extends
		MapReduceTestEnvironment
{
	private static final String TEST_COVERAGE_NAME_PREFIX = "TEST_COVERAGE";
	private static final String TEST_RESIZE_COVERAGE_NAME_PREFIX = "TEST_RESIZE";
	private static final int MIN_TILE_SIZE_POWER_OF_2 = 0;
	private static final int MAX_TILE_SIZE_POWER_OF_2 = 6;
	private static final int INCREMENT = 2;
	private static final int BASE_MIN_LEVEL = 15;
	private static final int BASE_MAX_LEVEL = 17;

	private void testIngestShp(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		// LOGGER.warn("Ingesting '" + ingestFilePath +
		// "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -t geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
	}

	@Test
	public void testKDEAndRasterResize()
			throws Exception {
		accumuloOperations.deleteAll();
		testIngestShp(
				IndexType.SPATIAL_VECTOR,
				"C:\\Users\\rfecher\\DotMatrixWorkspace\\data\\Export_Dots.shp");
		// testIngest(
		// IndexType.SPATIAL_VECTOR,
		// GENERAL_GPX_INPUT_GPX_DIR);

		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new KDEJobRunner(),
					new String[] {
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						// GpxUtils.GPX_WAYPOINT_FEATURE,

						"Export_Dots",
						new Integer(
								BASE_MIN_LEVEL - i).toString(),
						new Integer(
								BASE_MAX_LEVEL - i).toString(),
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						tileSizeCoverageName,
						hdfs,
						jobtracker,
						TEST_NAMESPACE,
						new Integer(
								(int) Math.pow(
										2,
										i)).toString()
					});
		}
		double[][][][] initialSampleValuesPerRequestSize = new double[3][][][];
		for (int i = 0; i < 2; i++) {
			initialSampleValuesPerRequestSize[i] = testSamplesMatch(
					TEST_COVERAGE_NAME_PREFIX,
					"first",
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					new Rectangle(
							(int) (24 * Math.pow(
									2,
									i)),
							(int) (24 * Math.pow(
									2,
									i))),
					null);
		}

		final Connector conn = ConnectorPool.getInstance().getConnector(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword);
		conn.tableOperations().compact(
				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
				null,
				null,
				true,
				true);
		for (int i = 0; i < 2; i++) {
			testSamplesMatch(
					TEST_COVERAGE_NAME_PREFIX,
					"compact",
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					new Rectangle(
							(int) (24 * Math.pow(
									2,
									i)),
							(int) (24 * Math.pow(
									2,
									i))),
					initialSampleValuesPerRequestSize[i]);
		}
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_PREFIX + i;
			final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_PREFIX + i;
			ToolRunner.run(
					new RasterTileResizeJobRunner(),
					new String[] {
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						originalTileSizeCoverageName,
						new Integer(
								MIN_INPUT_SPLITS).toString(),
						new Integer(
								MAX_INPUT_SPLITS).toString(),
						hdfs,
						jobtracker,
						resizeTileSizeCoverageName,
						TEST_NAMESPACE,
						new Integer(
								(int) Math.pow(
										2,
										MAX_TILE_SIZE_POWER_OF_2 - i)).toString()
					});
		}

		for (int i = 0; i < 2; i++) {
			testSamplesMatch(
					TEST_RESIZE_COVERAGE_NAME_PREFIX,
					"first",
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					new Rectangle(
							(int) (24 * Math.pow(
									2,
									i)),
							(int) (24 * Math.pow(
									2,
									i))),
					initialSampleValuesPerRequestSize[i]);
		}

		conn.tableOperations().compact(
				TEST_NAMESPACE + "_" + IndexType.SPATIAL_RASTER.createDefaultIndex().getId().getString(),
				null,
				null,
				true,
				true);
		for (int i = 0; i < 2; i++) {
			testSamplesMatch(
					TEST_RESIZE_COVERAGE_NAME_PREFIX,
					"compact",
					((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
					new Rectangle(
							(int) (24 * Math.pow(
									2,
									i)),
							(int) (24 * Math.pow(
									2,
									i))),
					initialSampleValuesPerRequestSize[i]);
		}
	}

	private static double[][][] testSamplesMatch(
			final String coverageNamePrefix,
			final String after,
			final int numCoverages,
			final Rectangle pixelDimensions,
			double[][][] expectedResults )
			throws IOException,
			AccumuloException,
			AccumuloSecurityException {
		final GeoWaveRasterReader reader = new GeoWaveRasterReader(
				GeoWaveRasterConfig.createConfig(
						zookeeper,
						accumuloInstance,
						accumuloUser,
						accumuloPassword,
						TEST_NAMESPACE,
						false,
						Interpolation.INTERP_NEAREST));
		final GeneralEnvelope queryEnvelope = new GeneralEnvelope(
				new double[] {
					// this is exactly on a tile boundary, so there will be no
					// scaling on the tile composition/rendering
					52.49267578125,
					29.55322265625
				},
				new double[] {
					// these values are also on a tile boundary, to avoid
					// scaling
					52.62451171875,
					29.68505859375
				});

		queryEnvelope.setCoordinateReferenceSystem(GeoWaveGTRasterFormat.DEFAULT_CRS);
		final Raster[] rasters = new Raster[numCoverages];
		int coverageCount = 0;
		for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
			final String tileSizeCoverageName = coverageNamePrefix + i;
			final GridCoverage gridCoverage = reader.renderGridCoverage(
					tileSizeCoverageName,
					pixelDimensions,
					queryEnvelope,
					null,
					null);
			final RenderedImage image = gridCoverage.getRenderedImage();
			final Raster raster = image.getData();
			rasters[coverageCount] = raster;
			final File dir = new File(
					"C:\\Temp\\kde_test16");
			dir.mkdirs();
			final File f = new File(
					dir,
					coverageNamePrefix + "_" + after + "_" + i + ".png");
			f.delete();
			f.createNewFile();
			final BufferedImage heatmap = new BufferedImage(
					rasters[coverageCount].getWidth(),
					rasters[coverageCount].getHeight(),
					BufferedImage.TYPE_BYTE_GRAY);
			final Graphics g = heatmap.createGraphics();
			for (int x = 0; x < rasters[coverageCount].getWidth(); x++) {
				for (int y = 0; y < rasters[coverageCount].getHeight(); y++) {
					final double sample = rasters[coverageCount].getSampleDouble(
							x,
							y,
							2);
					if (!Double.isNaN(sample)) {
						g.setColor(new Color(
								(float) sample,
								(float) sample,
								(float) sample));
						g.fillRect(
								x,
								y,
								1,
								1);
					}
				}
			}
			coverageCount++;
			heatmap.flush();
			ImageIO.write(
					heatmap,
					"png",
					f);
		}
		for (int i = 0; i < numCoverages; i++) {
			final boolean initialResults = expectedResults == null;
			if (initialResults) {
				expectedResults = new double[rasters[i].getWidth()][rasters[i].getHeight()][rasters[i].getNumBands()];
			}
			else {
				Assert.assertEquals(
						"The expected width does not match the expected width for the coverage " + i,
						expectedResults.length,
						rasters[i].getWidth());
				Assert.assertEquals(
						"The expected height does not match the expected height for the coverage " + i,
						expectedResults[0].length,
						rasters[i].getHeight());
				Assert.assertEquals(
						"The expected number of bands does not match the expected bands for the coverage " + i,
						expectedResults[0][0].length,
						rasters[i].getNumBands());
			}
			for (int x = 0; x < rasters[i].getWidth(); x++) {
				for (int y = 0; y < rasters[i].getHeight(); y++) {
					for (int b = 0; b < rasters[i].getNumBands(); b++) {
						final double sample = rasters[i].getSampleDouble(
								x,
								y,
								b);
						if (initialResults) {
							expectedResults[x][y][b] = sample;
						}
						else {
							Assert.assertEquals(
									"The sample does not match the expected sample value for the coverage " + i + " at x=" + x + ",y=" + y + ",b=" + b,
									new Double(
											expectedResults[x][y][b]),
									new Double(
											sample));
						}
					}
				}
			}
		}
		// make sure all of the counts are the same before and after compaction
		// for (int i = 1; i < counts.length; i++) {
		// Assert.assertEquals('
		// "The count of non-nodata values is different between the 1 pixel KDE and the 2^"
		// + i + " pixel KDE",
		// counts[0],
		// counts[i]);
		// }
		return expectedResults;
	}
}
