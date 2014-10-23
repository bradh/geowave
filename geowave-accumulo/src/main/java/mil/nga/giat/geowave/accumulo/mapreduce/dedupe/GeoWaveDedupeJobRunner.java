package mil.nga.giat.geowave.accumulo.mapreduce.dedupe;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoWaveDedupeJobRunner extends
		Configured implements
		Tool
{
	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;
	protected String namespace;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<Index> indices = new ArrayList<Index>();
	protected DistributableQuery query = null;

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Configuration conf = super.getConf();
		final Job job = new Job(
				conf);

		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);
		job.setJarByClass(this.getClass());

		job.setJobName("GeoWave Dedupe (" + namespace + ")");

		job.setMapperClass(GeoWaveDedupeMapper.class);
		job.setCombinerClass(GeoWaveDedupeCombiner.class);
		job.setReducerClass(GeoWaveDedupeReducer.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(Writable.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(getOutputFormatClass());
		job.setNumReduceTasks(getNumReduceTasks());
		if ((adapters != null) && (adapters.size() > 0)) {
			for (final DataAdapter<?> adapter : adapters) {
				GeoWaveInputFormat.addDataAdapter(
						job,
						adapter);
			}
		}
		if ((indices != null) && (indices.size() > 0)) {
			for (final Index index : indices) {
				GeoWaveInputFormat.addIndex(
						job,
						index);
			}
		}
		if (query != null) {
			GeoWaveInputFormat.setQuery(
					job,
					query);
		}

		final FileSystem fs = FileSystem.get(conf);
		final Path outputPath = getHdfsOutputPath();
		fs.delete(
				outputPath,
				true);
		FileOutputFormat.setOutputPath(
				job,
				outputPath);

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final Index index ) {
		indices.add(index);
	}

	public void setQuery(
			final DistributableQuery query ) {
		this.query = query;
	}

	protected String getHdfsOutputBase() {
		return "/tmp";
	}

	protected Path getHdfsOutputPath() {
		return new Path(
				getHdfsOutputBase() + "/" + namespace + "_dedupe");
	}

	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return SequenceFileOutputFormat.class;
	}

	protected int getNumReduceTasks() {
		return 8;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new GeoWaveDedupeJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		zookeeper = args[0];
		instance = args[1];
		user = args[2];
		password = args[3];
		namespace = args[4];
		return runJob();
	}
}