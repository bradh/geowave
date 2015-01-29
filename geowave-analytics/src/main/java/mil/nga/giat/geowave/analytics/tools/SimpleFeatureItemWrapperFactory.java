package mil.nga.giat.geowave.analytics.tools;

import java.io.IOException;
import java.util.UUID;

import mil.nga.giat.geowave.analytics.tools.AnalyticFeature.ClusterFeatureAttribute;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class SimpleFeatureItemWrapperFactory implements
		AnalyticItemWrapperFactory<SimpleFeature>
{

	@Override
	public AnalyticItemWrapper<SimpleFeature> create(
			final SimpleFeature item ) {
		return new SimpleFeatureAnalyticItemWrapper(
				item);
	}

	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {}

	public static class SimpleFeatureAnalyticItemWrapper implements
			AnalyticItemWrapper<SimpleFeature>
	{

		final SimpleFeature item;

		public SimpleFeatureAnalyticItemWrapper(
				final SimpleFeature item ) {
			this.item = item;
		}

		@Override
		public String getID() {
			return item.getID();
		}

		@Override
		public SimpleFeature getWrappedItem() {
			return item;
		}

		@Override
		public long getAssociationCount() {
			final Long countO = (Long) item.getAttribute(ClusterFeatureAttribute.Count.attrName());
			return (countO != null) ? countO.longValue() : 0;
		}

		@Override
		public int getIterationID() {
			return ((Integer) item.getAttribute(ClusterFeatureAttribute.Iteration.attrName())).intValue();
		}

		@Override
		public String getGroupID() {
			return getAttribute(
					item,
					ClusterFeatureAttribute.GroupID.attrName());
		}

		@Override
		public void setGroupID(
				final String groupID ) {
			item.setAttribute(
					ClusterFeatureAttribute.GroupID.attrName(),
					groupID);
		}

		@Override
		public void resetAssociatonCount() {
			item.setAttribute(
					ClusterFeatureAttribute.Count.attrName(),
					0);
		}

		@Override
		public void incrementAssociationCount(
				final long increment ) {
			item.setAttribute(
					ClusterFeatureAttribute.Count.attrName(),
					getAssociationCount() + increment);
		}

		@Override
		public String toString() {
			return "SimpleFeatureCentroid [item=" + item.getID() + ", + group=" + getGroupID() + ", + count=" + getAssociationCount() + ", cost=" + getCost() + "]";
		}

		@Override
		public double getCost() {
			final Double costO = (Double) item.getAttribute(ClusterFeatureAttribute.Weight.attrName());
			return (costO != null) ? costO.doubleValue() : 0.0;
		}

		@Override
		public void setCost(
				final double cost ) {
			// GENERIC GEOMETRY HAS A DISTANCE, NOT A COST
			item.setAttribute(
					ClusterFeatureAttribute.Weight.attrName(),
					cost);
		}

		@Override
		public String getName() {
			return item.getAttribute(
					ClusterFeatureAttribute.Name.attrName()).toString();
		}

		@Override
		public String[] getExtraDimensions() {
			return new String[0];
		}

		@Override
		public double[] getDimensionValues() {
			return new double[0];
		}

		@Override
		public Geometry getGeometry() {
			return (Geometry) item.getAttribute(ClusterFeatureAttribute.Geometry.attrName());
		}

		@Override
		public void setZoomLevel(
				final int level ) {
			item.setAttribute(
					ClusterFeatureAttribute.ZoomLevel.attrName(),
					new Integer(
							level));

		}

		@Override
		public int getZoomLevel() {
			return getIntAttribute(
					item,
					ClusterFeatureAttribute.ZoomLevel.attrName(),
					1);
		}

		@Override
		public void setBatchID(
				final String batchID ) {
			item.setAttribute(
					ClusterFeatureAttribute.BatchId.attrName(),
					batchID);
		}

		@Override
		public String getBatchID() {
			return item.getAttribute(
					ClusterFeatureAttribute.BatchId.attrName()).toString();
		}

	}

	private static String getAttribute(
			final SimpleFeature feature,
			final String name ) {
		final Object att = feature.getAttribute(name);
		return att == null ? null : att.toString();
	}

	private static int getIntAttribute(
			final SimpleFeature feature,
			final String name,
			final int defaultValue ) {
		final Object att = feature.getAttribute(name);
		return att == null ? defaultValue : (att instanceof Number ? ((Number) att).intValue() : Integer.parseInt(att.toString()));
	}

	/*
	 * @see
	 * mil.nga.giat.geowave.analytics.tools.CentroidFactory#createNextCentroid
	 * (java.lang.Object, com.vividsolutions.jts.geom.Coordinate,
	 * java.lang.String[], double[])
	 */

	@Override
	public AnalyticItemWrapper<SimpleFeature> createNextItem(
			final SimpleFeature feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		final Geometry geometry = (Geometry) feature.getAttribute(ClusterFeatureAttribute.Geometry.attrName());

		return new SimpleFeatureAnalyticItemWrapper(
				AnalyticFeature.createGeometryFeature(
						feature.getFeatureType(),
						feature.getAttribute(
								ClusterFeatureAttribute.BatchId.attrName()).toString(),
						UUID.randomUUID().toString(),
						getAttribute(
								feature,
								ClusterFeatureAttribute.Name.attrName()),
						groupID,
						((Double) feature.getAttribute(ClusterFeatureAttribute.Weight.attrName())).doubleValue(),
						geometry.getFactory().createPoint(
								coordinate),
						extraNames,
						extraValues,
						((Integer) feature.getAttribute(ClusterFeatureAttribute.ZoomLevel.attrName())).intValue(),
						((Integer) feature.getAttribute(ClusterFeatureAttribute.Iteration.attrName())).intValue() + 1,
						((Long) feature.getAttribute(ClusterFeatureAttribute.Count.attrName())).longValue()));

	}

}
