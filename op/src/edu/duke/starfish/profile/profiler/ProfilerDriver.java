package edu.duke.starfish.profile.profiler;

import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profiler.loaders.SysStatsLoader;
import edu.duke.starfish.profile.utils.GeneralUtils;
import edu.duke.starfish.profile.utils.ProfileUtils;
import edu.duke.starfish.profile.utils.XMLProfileParser;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ProfilerDriver {
	private static String MODE = "mode";
	private static String JOB = "job";
	private static String RESULTS = "results";
	private static String MONITOR = "monitor";
	private static String NODE = "node";
	private static String JOB1 = "job1";
	private static String JOB2 = "job2";
	private static String OUTPUT = "output";
	private static String HELP = "help";

	private static String LIST_ALL = "list_all";
	private static String LIST_STATS = "list_stats";
	private static String DETAILS = "details";
	private static String CLUSTER = "cluster";
	private static String TIMELINE = "timeline";
	private static String MAPPERS = "mappers";
	private static String REDUCERS = "reducers";
	private static String TRANSFERS_ALL = "transfers_all";
	private static String TRANSFERS_MAP = "transfers_map";
	private static String TRANSFERS_RED = "transfers_red";
	private static String PROFILE = "profile";
	private static String PROFILE_XML = "profile_xml";
	private static String ADJUST = "adjust";
	private static String CPU_STATS = "cpustats";
	private static String MEM_STATS = "memstats";
	private static String IO_STATS = "iostats";

	private static String TAB = "\t";

	public static void main(String[] args) {
		CommandLine line = parseAndValidateInput(args);

		if (line.hasOption(HELP)) {
			printHelp(System.out);
			System.exit(0);
		}

		PrintStream out = null;
		if (line.hasOption(OUTPUT))
			try {
				File outFile = new File(line.getOptionValue(OUTPUT));
				if (outFile.exists()) {
					System.err.println("The output file '" + outFile.getName()
							+ "' already exists.");

					System.err.println("Please specify a new output file.");
					System.exit(-1);
				}
				out = new PrintStream(outFile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		else {
			out = System.out;
		}

		MRJobLogsManager manager = new MRJobLogsManager();
		if (line.hasOption(RESULTS)) {
			manager.setResultsDir(line.getOptionValue(RESULTS));
		}

		MRJobInfo mrJob = null;
		if (line.hasOption(JOB)) {
			String jobId = line.getOptionValue(JOB);
			mrJob = manager.getMRJobInfo(jobId);
			if (mrJob == null) {
				System.err.println("Unable to find a job with id " + jobId);
				System.exit(-1);
			}

		}

		String mode = line.getOptionValue(MODE);
		if (mode.equals(LIST_ALL)) {
			printMRJobSummaries(out, manager.getAllMRJobInfos());
		} else if (mode.equals(LIST_STATS)) {
			for (MRJobInfo job : manager.getAllMRJobInfos()) {
				manager.loadTaskDetailsForMRJob(job);
			}

			printMRJobStatistics(out, manager.getAllMRJobInfos());
		} else if (mode.equals(DETAILS)) {
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				manager.loadProfilesForMRJob(mrJob);
				ProfileUtils.printMRJobDetails(out, mrJob);
			} else {
				System.err.println("Unable to load the task details for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if (mode.equals(CLUSTER)) {
			ClusterConfiguration cluster = manager
					.getClusterConfiguration(mrJob.getExecId());

			if (cluster != null) {
				cluster.printClusterConfiguration(out);
			} else {
				System.err.println("Unable to load the cluster info for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if ((mode.equals(TRANSFERS_ALL)) || (mode.equals(TRANSFERS_MAP))
				|| (mode.equals(TRANSFERS_RED))) {
			if (!manager.loadTaskDetailsForMRJob(mrJob)) {
				System.err.println("Unable to load the task details for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}

			if (mrJob.isMapOnly()) {
				out.println("Job " + mrJob.getExecId()
						+ " is map-only, no data transfers occurred!");
			} else if (manager.loadDataTransfersForMRJob(mrJob)) {
				ProfileUtils.printDataTransfers(out, mrJob, mode);
			} else if ((manager.loadProfilesForMRJob(mrJob))
					&& (ProfileUtils.generateDataTransfers(mrJob,
							manager.getHadoopConfiguration(mrJob.getExecId())))) {
				out.println("NOTE: Transfers estimated based on profile information!");

				ProfileUtils.printDataTransfers(out, mrJob, mode);
			} else {
				System.err.println("Unable to load the data transfers for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if ((mode.equals(PROFILE)) || (mode.equals(PROFILE_XML))) {
			if (!manager.loadTaskDetailsForMRJob(mrJob)) {
				System.err.println("Unable to load the task details for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}

			MRJobProfile profile = null;
			if (manager.loadProfilesForMRJob(mrJob)) {
				profile = mrJob.getProfile();
			} else {
				System.err.println("Unable to load the full profile for job "
						+ mrJob.getExecId());

				profile = ProfileUtils.generatePartialProfile(mrJob);
			}

			if (mode.equals(PROFILE)) {
				profile.printProfile(out, false);
			} else if (mode.equals(PROFILE_XML)) {
				XMLProfileParser parser = new XMLProfileParser();
				parser.exportXML(profile, out);
			}
		} else if (mode.equals(ADJUST)) {
			String jobId1 = line.getOptionValue(JOB1);
			MRJobInfo rmJob1 = manager.getMRJobInfo(jobId1);
			if ((rmJob1 == null) || (!manager.loadProfilesForMRJob(rmJob1))) {
				System.err.println("Unable to find a job with id " + jobId1);
				System.exit(-1);
			}

			String jobId2 = line.getOptionValue(JOB2);
			MRJobInfo rmJob2 = manager.getMRJobInfo(jobId2);
			if ((rmJob2 == null) || (!manager.loadProfilesForMRJob(rmJob2))) {
				System.err.println("Unable to find a job with id " + jobId2);
				System.exit(-1);
			}

			MRJobProfile[] profResult = ProfileUtils
					.adjustProfilesForCompression(rmJob1.getOrigProfile(),
							rmJob2.getOrigProfile());

			File jobProfDir = new File(line.getOptionValue(RESULTS),
					"job_profiles");

			File profile1XML = new File(jobProfDir, "adj_profile_"
					+ line.getOptionValue(JOB1) + ".xml");

			File profile2XML = new File(jobProfDir, "adj_profile_"
					+ line.getOptionValue(JOB2) + ".xml");

			XMLProfileParser parser = new XMLProfileParser();
			parser.exportXML(profResult[0], profile1XML);
			parser.exportXML(profResult[1], profile2XML);

			out.println("Completed adjusting profiles for "
					+ line.getOptionValue(JOB1) + " and "
					+ line.getOptionValue(JOB2));
		} else if (mode.equals(MAPPERS)) {
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				manager.loadProfilesForMRJob(mrJob);
				ProfileUtils.printMRMapInfo(out, mrJob.getMapTasks());
			} else {
				System.err.println("Unable to load the map timings for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if (mode.equals(REDUCERS)) {
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				manager.loadProfilesForMRJob(mrJob);
				ProfileUtils.printMRReduceInfo(out, mrJob.getReduceTasks());
			} else {
				System.err.println("Unable to load the reduce timings for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if (mode.equals(TIMELINE)) {
			if (manager.loadTaskDetailsForMRJob(mrJob)) {
				ProfileUtils.printMRJobTimeline(out, mrJob);
			} else {
				System.err.println("Unable to load the task timeline for job "
						+ mrJob.getExecId());

				System.exit(-1);
			}
		} else if (mode.equals(CPU_STATS)) {
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));

			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null) {
				success = loader.exportCPUStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			} else {
				success = loader.exportCPUStats(out, nodeName);
			}
			if (!success) {
				System.err.println("Unable to export the CPU stats");
				System.exit(-1);
			}
		} else if (mode.equals(MEM_STATS)) {
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));

			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null) {
				success = loader.exportMemoryStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			} else {
				success = loader.exportMemoryStats(out, nodeName);
			}
			if (!success) {
				System.err.println("Unable to export the Memory stats");
				System.exit(-1);
			}
		} else if (mode.equals(IO_STATS)) {
			SysStatsLoader loader = new SysStatsLoader(
					line.getOptionValue(MONITOR));

			String nodeName = line.getOptionValue(NODE);
			boolean success = false;

			if (mrJob != null) {
				success = loader.exportIOStats(out, nodeName,
						mrJob.getStartTime(), mrJob.getEndTime());
			} else {
				success = loader.exportIOStats(out, nodeName);
			}
			if (!success) {
				System.err.println("Unable to export the I/O stats");
				System.exit(-1);
			}
		}

		out.close();
	}

	private static Options buildProfilerOptions() {
		OptionBuilder.withArgName("mode");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Execution mode options");
		Option modeOption = OptionBuilder.create(MODE);

		OptionBuilder.withArgName("job_id");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("The job id of interest");
		Option jobOption = OptionBuilder.create(JOB);

		OptionBuilder.withArgName("dir");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("The results directory from profiling");
		Option resultsOption = OptionBuilder.create(RESULTS);

		OptionBuilder.withArgName("job_id");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("The job id without compression");
		Option job1Option = OptionBuilder.create(JOB1);

		OptionBuilder.withArgName("job_id");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("The job id with compression");
		Option job2Option = OptionBuilder.create(JOB2);

		OptionBuilder.withArgName("dir");
		OptionBuilder.hasArg();
		OptionBuilder
				.withDescription("The directoryt with the monitoring files");
		Option monitorOption = OptionBuilder.create(MONITOR);

		OptionBuilder.withArgName("node_name");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("The node name of interest");
		Option nodeOption = OptionBuilder.create(NODE);

		OptionBuilder.withArgName("filepath");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("An output file to print to");
		Option outputOption = OptionBuilder.create(OUTPUT);

		OptionBuilder.withArgName("help");
		Option helpOption = OptionBuilder.create(HELP);

		Options opts = new Options();
		opts.addOption(modeOption);
		opts.addOption(jobOption);
		opts.addOption(resultsOption);
		opts.addOption(monitorOption);
		opts.addOption(nodeOption);
		opts.addOption(job1Option);
		opts.addOption(job2Option);
		opts.addOption(outputOption);
		opts.addOption(helpOption);

		return opts;
	}

	private static String getMRJobStatisticsString(MRJobInfo mrJob) {
		StringBuilder sb = new StringBuilder();

		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(2);
		nf.setMaximumFractionDigits(2);

		sb.append(mrJob.getExecId());
		sb.append(TAB);
		sb.append(nf.format(mrJob.getDuration()));
		sb.append(TAB);
		sb.append(mrJob.getMapTasks().size());
		sb.append(TAB);
		double avg = ProfileUtils.calculateDurationAverage(mrJob.getMapTasks());
		sb.append(nf.format(avg));
		sb.append(TAB);
		double dev = ProfileUtils.calculateDurationDeviation(mrJob
				.getMapTasks());
		sb.append(nf.format(dev));
		sb.append(TAB);
		sb.append(mrJob.getReduceTasks().size());
		sb.append(TAB);
		avg = ProfileUtils.calculateDurationAverage(mrJob.getReduceTasks());
		sb.append(nf.format(avg));
		sb.append(TAB);
		dev = ProfileUtils.calculateDurationDeviation(mrJob.getReduceTasks());
		sb.append(nf.format(dev));

		nf.setMinimumFractionDigits(0);
		nf.setMaximumFractionDigits(0);
		sb.append(TAB);
		sb.append(nf.format(ProfileUtils.calculateJobInputSize(mrJob)));
		sb.append(TAB);
		sb.append(nf.format(ProfileUtils.calculateJobOutputSize(mrJob)));

		return sb.toString();
	}

	private static String getMRJobSummaryString(MRJobInfo mrJob) {
		StringBuilder sb = new StringBuilder();

		sb.append(mrJob.getExecId());
		sb.append(TAB);
		sb.append(mrJob.getName());
		sb.append(TAB);
		sb.append(mrJob.getStartTime());
		sb.append(TAB);
		sb.append(mrJob.getEndTime());
		sb.append(TAB);
		sb.append(GeneralUtils.getFormattedDuration(mrJob.getDuration()));
		sb.append(TAB);
		sb.append(mrJob.getStatus());

		return sb.toString();
	}

	private static CommandLine parseAndValidateInput(String[] args) {
		if ((args == null) || (args.length == 0)) {
			printUsage(System.out);
			System.exit(0);
		}

		Options opts = buildProfilerOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(opts, args, true);
		} catch (ParseException e) {
			System.err.println("Unable to parse the input arguments");
			System.err.println(e.getMessage());
			printUsage(System.err);
			System.exit(-1);
		}

		if ((line.getArgs() != null) && (line.getArgs().length > 0)) {
			System.err.println("Unsupported input arguments:");
			for (String arg : line.getArgs()) {
				System.err.println(arg);
			}
			printUsage(System.err);
			System.exit(-1);
		}

		if (line.hasOption(HELP)) {
			return line;
		}

		if (!line.hasOption(MODE)) {
			System.err.println("The 'mode' option is required");
			printUsage(System.err);
			System.exit(-1);
		}

		String mode = line.getOptionValue(MODE);

		if ((mode.equals(LIST_ALL)) || (mode.equals(LIST_STATS))) {
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}

		} else if ((mode.equals(DETAILS)) || (mode.equals(CLUSTER))
				|| (mode.equals(TIMELINE)) || (mode.equals(MAPPERS))
				|| (mode.equals(REDUCERS)) || (mode.equals(TRANSFERS_ALL))
				|| (mode.equals(TRANSFERS_MAP)) || (mode.equals(TRANSFERS_RED))
				|| (mode.equals(PROFILE)) || (mode.equals(PROFILE_XML))) {
			if (!line.hasOption(JOB)) {
				System.err.println("The 'job' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}

		} else if (mode.equals(ADJUST)) {
			if (!line.hasOption(JOB1)) {
				System.err.println("The 'job1' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(JOB2)) {
				System.err.println("The 'job2' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(RESULTS)) {
				System.err.println("The 'results' option is required");
				printUsage(System.err);
				System.exit(-1);
			}

		} else if ((mode.equals(CPU_STATS)) || (mode.equals(MEM_STATS))
				|| (mode.equals(IO_STATS))) {
			if (!line.hasOption(MONITOR)) {
				System.err.println("The 'monitor' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
			if (!line.hasOption(NODE)) {
				System.err.println("The 'node' option is required");
				printUsage(System.err);
				System.exit(-1);
			}
		} else {
			System.err.println("The mode option is not supported: " + mode);
			printUsage(System.err);
			System.exit(-1);
		}

		return line;
	}

	private static void printMRJobStatistics(PrintStream out,
			List<MRJobInfo> mrJobs) {
		out.println("JobID\tDuration(ms)\tMap_Count\tMap_Avg(ms)\tMap_Std_Dev(ms)\tReduce_Count\tReduce_Avg(ms)\tReduce_Std_Dev(ms)\tInput_Data(bytes)\tOutput_Data(bytes)");

		for (MRJobInfo mrJob : mrJobs)
			out.println(getMRJobStatisticsString(mrJob));
	}

	private static void printMRJobSummaries(PrintStream out,
			List<MRJobInfo> mrJobs) {
		out.println("JobID\tJob_Name\tStart_Time\tEnd_Time\tDuration\tStatus");
		for (MRJobInfo mrJob : mrJobs)
			out.println(getMRJobSummaryString(mrJob));
	}

	private static void printUsage(PrintStream out) {
		out.println();
		out.println("Usage:");
		out.println("  bin/hadoop jar starfish_profiler.jar <parameters>");
		out.println();
		out.println("The profiler parameters must be one of the following six cases:");

		out.println("  -mode {list_all|list_stats}");
		out.println("    -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {details|cluster|timeline|mappers|reducers}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {transfers_all|transfers_map|transfers_red}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {profile|profile_xml}");
		out.println("    -job <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode adjust");
		out.println("    -job1 <job_id> -job2 <job_id> -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode {cpustats|memstats|iostats}");
		out.println("    -monitor <dir> -node <node_name> ");
		out.println("    [-job <job_id> -results <dir>] [-output <file>]");
		out.println();
		out.println("  -help");
		out.println();
	}

	private static void printHelp(PrintStream out) {
		out.println("Usage:");
		out.println(" bin/hadoop jar starfish_profiler.jar <parameters>");
		out.println();
		out.println("The profiler parameters must be one of:");
		out.println("  -mode list_all   -results <dir> [-ouput <file>]");
		out.println("  -mode list_stats -results <dir> [-ouput <file>]");
		out.println();
		out.println("  -mode details     -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode cluster     -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode timeline    -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode mappers     -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode reducers    -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode transfers   -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode profile     -job <job_id> -results <dir> [-ouput <file>]");

		out.println("  -mode profile_xml -job <job_id> -results <dir> [-ouput <file>]");

		out.println();
		out.println("  -mode adjust  -job1 <job_id> -job2 <job_id> -results <dir> [-ouput <file>]");

		out.println();
		out.println("  -mode cpustats  -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println("  -mode memstats  -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println("  -mode iostats   -monitor <dir> -node <node_name> ");
		out.println("     [-job <job_id> -results <dir>] [-output <file>]");
		out.println();
		out.println("Description of execution modes:");
		out.println("  list_all     List all available jobs");
		out.println("  list_stats   List stats for all available jobs");
		out.println("  details      Display the details of a job");
		out.println("  cluster      Display the cluster information");
		out.println("  timeline     Generate timeline of tasks");
		out.println("  mappers      Display mappers information of a job");
		out.println("  reducers     Display reducers information of a job");
		out.println("  transfers_all Display all data transfers of a job");
		out.println("  transfers_map Display aggregated data transfers from maps");

		out.println("  transfers_red Display aggregated data transfers to reducers");

		out.println("  profile      Display the profile of a job");
		out.println("  profile_xml  Display the profile of a job in XML format");

		out.println("  adjust       Adjusts compression costs for two jobs");
		out.println("  cpustats     Display CPU stats of a node");
		out.println("  memstats     Display Memory stats of a node");
		out.println("  iostats      Display I/O stats of a node");
		out.println();
		out.println("Description of parameter flags:");
		out.println("  -mode <option>    The execution mode");
		out.println("  -job <job_id>     The job id of interest");
		out.println("  -results <dir>    The results directory generated from profiling");

		out.println("  -monitor <dir>    The directory with the monitoring files");

		out.println("  -job1 <file>      The job id for job run without compression");

		out.println("  -job2 <file>      The job id for job run with compression");

		out.println("  -node <node_name> The node name of interest (for monitor info)");

		out.println("  -output <file>    An optional file to write the output to");

		out.println("  -help             Display detailed instructions");
		out.println();
	}
}