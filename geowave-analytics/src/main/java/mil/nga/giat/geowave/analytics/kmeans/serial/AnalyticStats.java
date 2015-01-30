package mil.nga.giat.geowave.analytics.kmeans.serial;

public interface AnalyticStats
{
	public static enum StatValue {
		COST,
		COUNT
	}

	public void notify(
			StatValue stat,
			double amount );

	public void reset();
}
