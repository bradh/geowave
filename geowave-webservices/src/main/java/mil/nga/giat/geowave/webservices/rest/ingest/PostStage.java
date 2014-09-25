package mil.nga.giat.geowave.webservices.rest.ingest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.webservices.rest.Services;

public class PostStage
{
	private IngestFromHdfsDriver driver;
	private String zookeeperUrl;
	private String geowaveUsername;
	private String geowavePassword;
	private String instanceName;
	private String hdfs;
	private String hdfsbase;
	private String jobtracker;
	
	public PostStage() throws IOException {
		loadProperties();
		
		driver = new IngestFromHdfsDriver("poststage");
	}

	public void run(String namespace) throws ParseException {
		run(namespace, null);
	}
	public void run(String namespace, String visibility) throws ParseException {
		run(namespace, visibility, false);
	}
	
	public void run(String namespace, String visibility, boolean clear) throws ParseException {
		Options options = new Options();
		driver.applyOptions(options);

		ArrayList<String> args = new ArrayList<String>();
		args.add("--zookeepers");
		args.add(zookeeperUrl);
		args.add("--namespace");
		args.add(namespace);
		args.add("--user");
		args.add(geowaveUsername);
		args.add("--password");
		args.add(geowavePassword);
		args.add("--instance-id");
		args.add(instanceName);
		if (visibility != null && visibility.trim().length() > 0) {
			args.add("--visibility");
			args.add(visibility);
		}
		args.add("-hdfs");
		args.add(hdfs);
		args.add("-hdfsbase");
		args.add(hdfsbase);
		args.add("-jobtracker");
		args.add(jobtracker);
		if (clear)
			args.add("--clear");
		
		String[] arguments = args.toArray(new String [] {});
		CommandLine commandLine = new BasicParser().parse(options, arguments);
		driver.parseOptions(commandLine);

		driver.run(arguments);
	}
	
	private void loadProperties() throws IOException {
		// load geowave ingest properties
		Properties prop = new Properties();
		String propFileName = "mil/nga/giat/geowave/webservices/ingest/config.properties";
		InputStream inputStream = Services.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		prop.load(inputStream);

		hdfs = prop.getProperty("hdfs");
		hdfsbase = prop.getProperty("hdfsbase");
		jobtracker = prop.getProperty("jobtracker");

		// load geowave properties
		propFileName = "mil/nga/giat/geowave/utils/config.properties";
		inputStream = Services.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		prop.load(inputStream);

		zookeeperUrl = prop.getProperty("zookeeperUrl");
		instanceName = prop.getProperty("instanceName");
		geowaveUsername = prop.getProperty("geowave_username");
		geowavePassword = prop.getProperty("geowave_password");
	}
}