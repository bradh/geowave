package mil.nga.giat.geowave.analytics.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature.ClusterFeatureAttribute;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;

public class AnalyticFeatureTest
{
	@Test
	public void testGeometryCreation()
			throws MismatchedDimensionException,
			NoSuchAuthorityCodeException,
			FactoryException,
			CQLException,
			ParseException {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				4326).getType();
		final GeometryFactory factory = new GeometryFactory();
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
		assertEquals(
				new Coordinate(
						02.33,
						0.23),
				((Geometry) feature.getDefaultGeometry()).getCoordinate());
		System.out.println(((Geometry) feature.getDefaultGeometry()).getPrecisionModel());
		System.out.println(((Geometry) feature.getDefaultGeometry()).getEnvelope());

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				10,
				1,
				0);

		assertEquals(
				new Coordinate(
						02.33,
						0.23),
				((Geometry) feature.getDefaultGeometry()).getCoordinate());

		assertEquals(
				"geometry",
				feature.getFeatureType().getGeometryDescriptor().getName().getLocalPart());

		assertEquals(
				new Integer(
						10),
				feature.getAttribute(ClusterFeatureAttribute.ZoomLevel.attrName()));

		Filter gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and level = 10");
		assertTrue(gtFilter.evaluate(feature));
		gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and level = 9");
		assertFalse(gtFilter.evaluate(feature));
		gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and batchID = 'b1'");
		assertTrue(gtFilter.evaluate(feature));

	}
}
