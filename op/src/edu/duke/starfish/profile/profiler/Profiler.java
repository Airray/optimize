package edu.duke.starfish.profile.profiler;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.profiler.loaders.MRJobHistoryLoader;
import edu.duke.starfish.profile.profiler.loaders.MRTaskProfilesLoader;
import edu.duke.starfish.profile.utils.XMLProfileParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

public class Profiler {
	public static final String PROFILER_BTRACE_DIR = "starfish.profiler.btrace.dir";
	public static final String PROFILER_CLUSTER_NAME = "starfish.profiler.cluster.name";
	public static final String PROFILER_OUTPUT_DIR = "starfish.profiler.output.dir";
	public static final String PROFILER_RETAIN_TASK_PROFS = "starfish.profiler.retain.task.profiles";
	public static final String PROFILER_COLLECT_TRANSFERS = "starfish.profiler.collect.data.transfers";
	public static final String PROFILER_SAMPLING_MODE = "starfish.profiler.sampling.mode";
	public static final String PROFILER_SAMPLING_FRACTION = "starfish.profiler.sampling.fraction";
	public static final String PROFILE_TYPE = "starfish.profiler.profile.type";
	private static final Log LOG = LogFactory.getLog(Profiler.class);

	private static final Pattern JOB_PATTERN = Pattern
			.compile(".*(job_[0-9]+_[0-9]+).*");

	private static final Pattern TRANSFERS_PATTERN = Pattern
			.compile(".*(Shuffling|Read|Failed).*");

	public static boolean enableAllProfiling(Configuration conf) {
		return enableProfiling(conf,
				usesNewHadoopApi(conf) ? "BTraceTaskAndMemProfile.class"
						: "BTraceOldApiTaskAndMemProfile.class");
	}

	public static boolean enableExecutionProfiling(Configuration conf) {
		return enableProfiling(conf,
				usesNewHadoopApi(conf) ? "BTraceTaskProfile.class"
						: "BTraceOldApiTaskProfile.class");
	}

	public static boolean enableMemoryProfiling(Configuration conf) {
		return enableProfiling(conf,
				usesNewHadoopApi(conf) ? "BTraceTaskMemProfile.class"
						: "BTraceOldApiTaskMemProfile.class");
	}

	private static boolean enableProfiling(Configuration conf,
			String profileClass) {
		if (conf.get("starfish.profiler.btrace.dir") == null) {
			LOG.warn("The parameter 'starfish.profiler.btrace.dir' is required to enable profiling");

			return false;
		}

		conf.setBoolean("mapred.task.profile", true);
		conf.set("mapred.task.profile.maps", "0-9999");
		conf.set("mapred.task.profile.reduces", "0-9999");
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 1);
		conf.setInt("min.num.spills.for.combine", 9999);
		conf.setInt("mapred.reduce.parallel.copies", 1);
		conf.setFloat("mapred.job.reduce.input.buffer.percent", 0.0F);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		conf.set(
				"mapred.task.profile.params",
				new StringBuilder()
						.append("-javaagent:${starfish.profiler.btrace.dir}/btrace-agent.jar=dumpClasses=false,debug=false,unsafe=true,probeDescPath=.,noServer=true,script=${starfish.profiler.btrace.dir}/")
						.append(profileClass).append(",scriptOutputFile=%s")
						.toString());

		LOG.info("Job profiling enabled");
		return true;
	}

	public static void gatherJobExecutionFiles(Configuration conf,
			String localDir) {
		try {
			String jobId = gatherJobExecutionFiles(conf, new File(localDir));

			LOG.info(new StringBuilder().append("Job profiling completed for ")
					.append(jobId).toString());
		} catch (Exception e) {
			LOG.error("Job profiling failed!", e);
		}
	}

	public static void gatherJobExecutionFiles(Configuration conf) {
		try {
			String localDir = conf.get("starfish.profiler.output.dir");
			String jobId = gatherJobExecutionFiles(conf, new File(localDir));

			LOG.info(new StringBuilder()
					.append("Gathered execution files for ").append(jobId)
					.toString());
		} catch (Exception e) {
			LOG.error("Unable to gather the execution files!", e);
		}
	}

	public static String gatherJobExecutionFiles(Configuration conf,
			File outputDir) throws IOException {
		outputDir.mkdirs();
		if (!outputDir.isDirectory()) {
			throw new IOException(new StringBuilder()
					.append("Not a valid directory ")
					.append(outputDir.toString()).toString());
		}

		File historyDir = new File(outputDir, "history");
		historyDir.mkdir();
		File[] historyFiles = gatherJobHistoryFiles(conf, historyDir);

		MRJobHistoryLoader historyLoader = new MRJobHistoryLoader(
				historyFiles[0].getAbsolutePath(),
				historyFiles[1].getAbsolutePath());

		MRJobInfo mrJob = historyLoader.getMRJobInfoWithDetails();
		String jobId = mrJob.getExecId();

		if (conf.getBoolean("mapred.task.profile", false)) {
			File taskProfDir = new File(outputDir, "task_profiles");
			taskProfDir.mkdir();
			gatherJobProfileFiles(mrJob, taskProfDir);

			File jobProfDir = new File(outputDir, "job_profiles");
			jobProfDir.mkdir();
			File profileXML = new File(jobProfDir, new StringBuilder()
					.append("profile_").append(jobId).append(".xml").toString());
			exportProfileXMLFile(mrJob, conf, taskProfDir, profileXML);

			if (!conf
					.getBoolean("starfish.profiler.retain.task.profiles", true)) {
				for (File file : listTaskProfiles(jobId, taskProfDir)) {
					file.delete();
				}
				taskProfDir.delete();
			}

		}

		if (conf.getBoolean("starfish.profiler.collect.data.transfers", false)) {
			File transfersDir = new File(outputDir, "transfers");
			transfersDir.mkdir();
			gatherJobTransferFiles(mrJob, transfersDir);
		}

		return jobId;
	}

	public static File[] gatherJobHistoryFiles(Configuration conf,
			File historyDir) throws IOException {
		historyDir.mkdirs();
		if (!historyDir.isDirectory()) {
			throw new IOException(new StringBuilder()
					.append("Not a valid results directory ")
					.append(historyDir.toString()).toString());
		}

		Path hdfsHistoryDir = null;
		String outDir = conf.get("hadoop.job.history.user.location",
				conf.get("mapred.output.dir"));

		if ((outDir != null) && (!outDir.equals("none"))) {
			hdfsHistoryDir = new Path(new Path(outDir), "_logs/history");

			File[] localFiles = copyHistoryFiles(conf, hdfsHistoryDir,
					historyDir);

			if (localFiles != null) {
				return localFiles;
			}
		}

		String localHistory = conf.get(
				"hadoop.job.history.location",
				new StringBuilder()
						.append("file:///")
						.append(new File(System.getProperty("hadoop.log.dir"))
								.getAbsolutePath()).append(File.separator)
						.append("history").toString());

		Path localHistoryDir = new Path(localHistory);

		File[] localFiles = copyHistoryFiles(conf, localHistoryDir, historyDir);
		if (localFiles != null) {
			return localFiles;
		}

		String doneLocation = conf
				.get("mapred.job.tracker.history.completed.location");
		if (doneLocation == null) {
			doneLocation = new Path(localHistoryDir, "done").toString();
		}

		String localHistoryPattern = new StringBuilder()
				.append(doneLocation)
				.append("/version-[0-9]/*_/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/*")
				.toString();

		FileSystem fs = FileSystem.getLocal(conf);
		FileStatus[] status = fs.globStatus(new Path(localHistoryPattern));
		if ((status != null) && (status.length > 0)) {
			for (FileStatus stat : status) {
				localFiles = copyHistoryFiles(conf, stat.getPath(), historyDir);
				if (localFiles != null) {
					return localFiles;
				}
			}
		}

		throw new IOException(new StringBuilder()
				.append("Unable to find history files in directories ")
				.append(localHistoryDir.toString()).append(" or ")
				.append(hdfsHistoryDir.toString()).toString());
	}

	public static void gatherJobProfileFiles(MRJobInfo mrJob, File profilesDir)
			throws IOException {
		profilesDir.mkdirs();
		if (!profilesDir.isDirectory()) {
			throw new IOException(new StringBuilder()
					.append("Not a valid directory ")
					.append(profilesDir.getAbsolutePath()).toString());
		}

		File srcDir = new File(System.getProperty("user.dir"));

		boolean foundProfiles = false;
		for (File file : listTaskProfiles(mrJob.getExecId(), srcDir)) {
			if (!file.renameTo(new File(profilesDir, file.getName()))) {
				throw new IOException(new StringBuilder()
						.append("Unable to move the file  ")
						.append(file.toString()).toString());
			}

			foundProfiles = true;
		}

		if (!foundProfiles) {
			for (MRMapAttemptInfo attempt : mrJob
					.getMapAttempts(MRExecutionStatus.SUCCESS)) {
				downloadTaskProfile(attempt, profilesDir);
			}
			for (MRReduceAttemptInfo attempt : mrJob
					.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
				downloadTaskProfile(attempt, profilesDir);
			}
		}
	}

	public static void gatherJobTransferFiles(MRJobInfo mrJob, File transfersDir)
			throws IOException {
		for (MRReduceAttemptInfo attempt : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
			HttpURLConnection connection = openHttpTaskLogConnection(attempt,
					"syslog");

			BufferedReader input = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));

			File transfer = new File(transfersDir, new StringBuilder()
					.append("transfers_").append(attempt.getExecId())
					.toString());

			BufferedWriter output = new BufferedWriter(new FileWriter(transfer));
			try {
				String logData = null;
				while ((logData = input.readLine()) != null)
					if (TRANSFERS_PATTERN.matcher(logData).matches()) {
						output.write(new StringBuilder().append(logData)
								.append("\n").toString());
						output.flush();
					}
			} finally {
				input.close();
				connection.disconnect();
			}
		}
	}

	public static void exportProfileXMLFile(MRJobInfo mrJob,
			Configuration conf, File profilesDir, File profileXML) {
		String profile_type = conf
				.get("starfish.profiler.profile.type", "task");

		if ((profile_type.equals("task")) || (profile_type.equals("all"))) {
			MRTaskProfilesLoader profileLoader = new MRTaskProfilesLoader(
					mrJob, conf, profilesDir.getAbsolutePath());

			if (profileLoader.loadExecutionProfile(mrJob)) {
				MRJobProfile profile = mrJob.getOrigProfile();
				String clusterName = conf.get("starfish.profiler.cluster.name");
				if (clusterName != null) {
					profile.setClusterName(clusterName);
				}
				XMLProfileParser parser = new XMLProfileParser();
				parser.exportXML(profile, profileXML);
			} else {
				LOG.error(new StringBuilder()
						.append("Unable to create the job profile for ")
						.append(mrJob.getExecId()).toString());
			}
		}
	}

	public static String getJobId(Configuration conf) {
		String jar = conf.get("mapred.jar");

		Matcher matcher = JOB_PATTERN.matcher(jar);
		if (matcher.find()) {
			return matcher.group(1);
		}

		return "job_";
	}

	public static void loadCommonSystemProperties(Configuration conf) {
		if (conf.get("starfish.profiler.btrace.dir") == null) {
			conf.set("starfish.profiler.btrace.dir",
					System.getProperty("starfish.profiler.btrace.dir"));
		}

		if (conf.get("starfish.profiler.cluster.name") == null) {
			conf.set("starfish.profiler.cluster.name",
					System.getProperty("starfish.profiler.cluster.name"));
		}

		if (conf.get("starfish.profiler.output.dir") == null)
			conf.set("starfish.profiler.output.dir",
					System.getProperty("starfish.profiler.output.dir"));
	}

	public static void loadProfilingSystemProperties(Configuration conf) {
		loadCommonSystemProperties(conf);

		if ((conf.get("starfish.profiler.profile.type") == null)
				&& (System.getProperty("starfish.profiler.profile.type") != null)) {
			conf.set("starfish.profiler.profile.type",
					System.getProperty("starfish.profiler.profile.type"));
		}

		if ((conf.get("starfish.profiler.sampling.mode") == null)
				&& (System.getProperty("starfish.profiler.sampling.mode") != null)) {
			conf.set("starfish.profiler.sampling.mode",
					System.getProperty("starfish.profiler.sampling.mode"));
		}

		if ((conf.get("starfish.profiler.sampling.fraction") == null)
				&& (System.getProperty("starfish.profiler.sampling.fraction") != null)) {
			conf.set("starfish.profiler.sampling.fraction",
					System.getProperty("starfish.profiler.sampling.fraction"));
		}

		if ((conf.get("starfish.profiler.retain.task.profiles") == null)
				&& (System
						.getProperty("starfish.profiler.retain.task.profiles") != null)) {
			conf.set("starfish.profiler.retain.task.profiles", System
					.getProperty("starfish.profiler.retain.task.profiles"));
		}

		if ((conf.get("starfish.profiler.collect.data.transfers") == null)
				&& (System
						.getProperty("starfish.profiler.collect.data.transfers") != null)) {
			conf.set("starfish.profiler.collect.data.transfers", System
					.getProperty("starfish.profiler.collect.data.transfers"));
		}
	}

	public static boolean usesNewHadoopApi(Configuration conf) {
		return (conf.getBoolean("mapred.mapper.new-api", false))
				|| (conf.getBoolean("mapred.reducer.new-api", false));
	}

	private static URL buildHttpTaskLogUrl(MRTaskAttemptInfo attempt,
			String logFile, boolean useAttemptId) throws IOException {
		StringBuilder httpTaskLog = new StringBuilder();
		httpTaskLog.append("http://");
		httpTaskLog.append(attempt.getTaskTracker().getHostName());
		httpTaskLog.append(":");
		httpTaskLog.append(attempt.getTaskTracker().getPort());
		if (useAttemptId)
			httpTaskLog.append("/tasklog?plaintext=true&attemptid=");
		else
			httpTaskLog.append("/tasklog?plaintext=true&taskid=");
		httpTaskLog.append(attempt.getExecId());
		httpTaskLog.append("&filter=");
		httpTaskLog.append(logFile);

		return new URL(httpTaskLog.toString());
	}

	private static File[] copyHistoryFiles(Configuration conf,
			Path hadoopHistoryDir, File localHistoryDir) throws IOException {
		FileSystem fs = hadoopHistoryDir.getFileSystem(conf);
		if (!fs.exists(hadoopHistoryDir)) {
			return null;
		}

		String jobId = getJobId(conf);
		Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(hadoopHistoryDir,
				new PathFilter(jobId) {
					public boolean accept(Path path) {
						return path.getName().contains(this.val$jobId);
					}
				}));
		if (jobFiles.length != 2) {
			return null;
		}

		File[] localJobFiles = new File[2];
		for (Path jobFile : jobFiles) {
			File localJobFile = new File(localHistoryDir, jobFile.getName());
			FileUtil.copy(fs, jobFile, localJobFile, false, conf);

			if (localJobFile.getName().endsWith(".xml"))
				localJobFiles[0] = localJobFile;
			else {
				localJobFiles[1] = localJobFile;
			}
		}
		return localJobFiles;
	}

	private static void downloadTaskProfile(MRTaskAttemptInfo attempt,
			File profilesDir) throws IOException {
		HttpURLConnection connection = openHttpTaskLogConnection(attempt,
				"profile");

		if (connection.getResponseCode() == 200) {
			InputStream in = null;
			OutputStream out = null;
			try {
				in = connection.getInputStream();
				out = new FileOutputStream(new File(profilesDir,
						new StringBuilder().append(attempt.getExecId())
								.append(".profile").toString()));

				IOUtils.copyBytes(in, out, 65536, true);
			} finally {
				if (in != null)
					in.close();
				if (out != null)
					out.close();
				connection.disconnect();
			}
		} else {
			connection.disconnect();
		}
	}

	private static File[] listTaskProfiles(String jobId, File dir) {
		String strippedJobId = jobId.substring(4);
		File[] files = dir.listFiles(new FileFilter(strippedJobId) {
			public boolean accept(File pathname) {
				return (pathname.isFile())
						&& (!pathname.isHidden())
						&& (pathname.getName().contains(this.val$strippedJobId))
						&& (pathname.getName().endsWith(".profile"));
			}
		});
		return files;
	}

	private static HttpURLConnection openHttpTaskLogConnection(
			MRTaskAttemptInfo attempt, String logFile) throws IOException {
		URL taskLogUrl = buildHttpTaskLogUrl(attempt, logFile, false);
		HttpURLConnection connection = (HttpURLConnection) taskLogUrl
				.openConnection();

		if (connection.getResponseCode() != 200) {
			taskLogUrl = buildHttpTaskLogUrl(attempt, logFile, true);
			connection.disconnect();
			connection = (HttpURLConnection) taskLogUrl.openConnection();
		}

		return connection;
	}
}