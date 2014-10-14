package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profiler.Profiler;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GatherResults {
	public static void main(String[] args) throws IOException {
		if ((args.length != 1) && (args.length != 2)) {
			System.err.println("Usage 1: bin/hadoop "
					+ GatherResults.class.getName() + " <job_id>");

			System.err.println("Usage 2: bin/hadoop "
					+ GatherResults.class.getName()
					+ " <first_job_id> <last_job_id>");

			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Profiler.loadProfilingSystemProperties(conf);
		String outputDir = conf.get("starfish.profiler.output.dir");

		String localHistory = conf.get(
				"hadoop.job.history.location",
				"file:///"
						+ new File(System.getProperty("hadoop.log.dir"))
								.getAbsolutePath());

		Path localHistoryDir = new Path(localHistory);
		FileSystem fs = localHistoryDir.getFileSystem(conf);
		if (!fs.exists(localHistoryDir)) {
			System.err.println("ERROR: Unable to find the logs directory!");
			System.exit(-1);
		}

		String baseJobId = null;
		int firstJobId = 0;
		Matcher matcher = Pattern.compile("(job_[0-9]+_)([0-9]+)").matcher(
				args[0]);

		if (matcher.find()) {
			baseJobId = matcher.group(1);
			firstJobId = Integer.parseInt(matcher.group(2));
		} else {
			System.err.println("Invalid job id: " + args[0]);
			System.exit(-1);
		}

		int secondJobId = 0;
		if ((args.length == 2) && (args[1].length() != 0)) {
			matcher = Pattern.compile(baseJobId + "([0-9]+)").matcher(args[1]);
			if (matcher.find()) {
				secondJobId = Integer.parseInt(matcher.group(1));
			} else {
				System.err.println("Invalid job id: " + args[1]);
				System.exit(-1);
			}
		} else {
			secondJobId = firstJobId;
		}

		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setGroupingUsed(false);
		nf.setMinimumIntegerDigits(4);
		nf.setMaximumIntegerDigits(4);

		for (int id = firstJobId; id <= secondJobId; id++) {
			String jobId = baseJobId + nf.format(id);
			Path confFile = new Path(localHistoryDir, jobId + "_conf.xml");

			if (fs.exists(confFile)) {
				Configuration jobConf = new Configuration(conf);
				jobConf.addResource(confFile);
				Profiler.gatherJobExecutionFiles(jobConf, new File(outputDir));
				System.out.println("Gathered the execution files for " + jobId);
			} else {
				System.err
						.println("ERROR: Unable to find the configuration file "
								+ confFile.toString());
			}

		}

		System.out.println("Output location: " + outputDir);
	}
}