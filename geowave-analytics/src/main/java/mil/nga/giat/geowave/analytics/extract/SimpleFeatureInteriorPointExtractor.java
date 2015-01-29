package mil.nga.giat.geowave.analytics.extract;

import mil.nga.giat.geowave.vector.adapter.FeatureGeometryHandler;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * Extract a set of points representing critical points for a simple feature
 * that me be representative or compared to centroids.
 * 
 */
public class SimpleFeatureInteriorPointExtractor extends SimpleFeatureCentroidExtractor implements
		CentroidExtractor<SimpleFeature>
{
	@Override
	public Point getCentroid(
			final SimpleFeature anObject ) {
		final FeatureGeometryHandler handler = new FeatureGeometryHandler(
				anObject.getDefaultGeometryProperty().getDescriptor());
		final Geometry geometry = handler.toIndexValue(
				anObject).getGeometry();
		final int srid = getSRID(anObject);
		final Point point = geometry.getInteriorPoint();
		point.setSRID(srid);
		return point;
	}

}
