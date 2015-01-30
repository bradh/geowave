package mil.nga.giat.geowave.analytics.tools.dbops;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

/**
 * Building DB connections. Supports customization for unit tests and caching.
 * 
 */
public interface BasicAccumuloOperationsBuilder
{
	BasicAccumuloOperations build(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException;
}
