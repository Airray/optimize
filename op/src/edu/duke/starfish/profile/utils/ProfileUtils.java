package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRTaskInfo;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.profiler.MRJobLogsManager;
import java.io.File;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

public class ProfileUtils {
	private static final Log LOG = LogFactory.getLog(ProfileUtils.class);
	private static final String TAB = "\t";
	private static String TRANSFERS_ALL = "transfers_all";
	private static String TRANSFERS_MAP = "transfers_map";
	private static String TRANSFERS_RED = "transfers_red";

	private static final Pattern jvmMem = Pattern
			.compile("-Xmx([0-9]+)([M|m|G|g])");

	public static MRJobProfile[] adjustProfilesForCompression(
			MRJobProfile profNoCompr, MRJobProfile profWithCompr) {
		List mapProfsNoCompr = profNoCompr.getMapProfiles();
		List mapProfsWithCompr = profWithCompr.getMapProfiles();
		if (mapProfsNoCompr.size() != mapProfsWithCompr.size()) {
			mapProfsNoCompr = profNoCompr.getAvgMapProfiles();
			mapProfsWithCompr = profWithCompr.getAvgMapProfiles();
			if (mapProfsNoCompr.size() != mapProfsWithCompr.size()) {
				throw new RuntimeException(
						"ERROR: Expected the same number of map profiles");
			}

			LOG.warn("Unequal number of map profiles! Using the representative profiles instead!");
		}

		MRJobProfile adjProfNoCompr = new MRJobProfile(profNoCompr);
		MRJobProfile adjProfWithCompr = new MRJobProfile(profWithCompr);

		int numMapProfs = mapProfsNoCompr.size();
		for (int i = 0; i < numMapProfs; i++) {
			MRMapProfile mapProf = new MRMapProfile("");
			adjustMapProfilesForCompression(
					(MRMapProfile) mapProfsNoCompr.get(i),
					(MRMapProfile) mapProfsWithCompr.get(i), mapProf);

			((MRMapProfile) adjProfNoCompr.getMapProfiles().get(i))
					.addCostFactors(mapProf.getCostFactors());

			((MRMapProfile) adjProfWithCompr.getMapProfiles().get(i))
					.addCostFactors(mapProf.getCostFactors());

			((MRMapProfile) adjProfNoCompr.getMapProfiles().get(i))
					.addStatistics(mapProf.getStatistics());

			((MRMapProfile) adjProfWithCompr.getMapProfiles().get(i))
					.addStatistics(mapProf.getStatistics());
		}

		List redProfsNoCompr = profNoCompr.getReduceProfiles();
		List redProfsWithCompr = profWithCompr.getReduceProfiles();

		if (redProfsNoCompr.size() != redProfsWithCompr.size()) {
			throw new RuntimeException(
					"ERROR: Expected the same number of reduce profiles");
		}

		int numRedProfs = redProfsNoCompr.size();
		for (int i = 0; i < numRedProfs; i++) {
			MRReduceProfile redProf = new MRReduceProfile("");
			adjustReduceProfilesForCompression(
					(MRReduceProfile) redProfsNoCompr.get(i),
					(MRReduceProfile) redProfsWithCompr.get(i), redProf);

			((MRReduceProfile) adjProfNoCompr.getReduceProfiles().get(i))
					.addCostFactors(redProf.getCostFactors());

			((MRReduceProfile) adjProfWithCompr.getReduceProfiles().get(i))
					.addCostFactors(redProf.getCostFactors());

			((MRReduceProfile) adjProfNoCompr.getReduceProfiles().get(i))
					.addStatistics(redProf.getStatistics());

			((MRReduceProfile) adjProfWithCompr.getReduceProfiles().get(i))
					.addStatistics(redProf.getStatistics());
		}

		adjProfNoCompr.updateProfile();
		adjProfWithCompr.updateProfile();

		MRJobProfile[] result = { adjProfNoCompr, adjProfWithCompr };
		return result;
	}

	public static long aggregateMapCounter(MRCounter counter,
			List<MRMapAttemptInfo> maps, int inputIndex) {
		if ((maps == null) || (maps.size() == 0)) {
			return 0L;
		}
		Long zero = new Long(0L);
		long result = 0L;

		for (MRMapAttemptInfo map : maps) {
			if (map.getProfile().getInputIndex() == inputIndex) {
				result += map.getProfile().getCounter(counter, zero)
						.longValue();
			}
		}
		return result;
	}

	public static long aggregateTaskCounter(MRCounter counter,
			List<? extends MRTaskAttemptInfo> tasks) {
		if ((tasks == null) || (tasks.size() == 0)) {
			return 0L;
		}
		Long zero = new Long(0L);
		long result = 0L;

		for (MRTaskAttemptInfo task : tasks) {
			result += task.getProfile().getCounter(counter, zero).longValue();
		}

		return result;
	}

	public static double calculateDurationAverage(
			List<? extends MRTaskInfo> tasks) {
		if ((tasks == null) || (tasks.size() == 0)) {
			return 0.0D;
		}

		double sum = 0.0D;
		for (MRTaskInfo task : tasks) {
			sum += task.getDuration();
		}

		return sum / tasks.size();
	}

	public static double calculateDurationDeviation(
			List<? extends MRTaskInfo> tasks) {
		if ((tasks == null) || (tasks.size() == 0)) {
			return 0.0D;
		}

		double avg = calculateDurationAverage(tasks);
		double variance = 0.0D;
		for (MRTaskInfo task : tasks) {
			variance += (task.getDuration() - avg) * (task.getDuration() - avg);
		}

		return Math.sqrt(variance / tasks.size());
	}

	public static long calculateJobInputSize(MRJobInfo mrJob) {
		long input = aggregateTaskCounter(MRCounter.HDFS_BYTES_READ,
				mrJob.getMapAttempts(MRExecutionStatus.SUCCESS));

		if (input == 0L) {
			input = aggregateTaskCounter(MRCounter.S3N_BYTES_READ,
					mrJob.getMapAttempts(MRExecutionStatus.SUCCESS));
		}

		return input;
	}

	public static long calculateJobInputSize(MRJobInfo mrJob, int inputIndex) {
		long input = aggregateMapCounter(MRCounter.HDFS_BYTES_READ,
				mrJob.getMapAttempts(MRExecutionStatus.SUCCESS), inputIndex);

		if (input == 0L) {
			input = aggregateMapCounter(MRCounter.S3N_BYTES_READ,
					mrJob.getMapAttempts(MRExecutionStatus.SUCCESS), inputIndex);
		}

		return input;
	}

	public static long calculateJobOutputSize(MRJobInfo mrJob) {
		long output = 0L;
		if (mrJob.isMapOnly()) {
			output = aggregateTaskCounter(MRCounter.HDFS_BYTES_WRITTEN,
					mrJob.getMapAttempts(MRExecutionStatus.SUCCESS));

			if (output == 0L)
				output = aggregateTaskCounter(MRCounter.S3N_BYTES_WRITTEN,
						mrJob.getMapAttempts(MRExecutionStatus.SUCCESS));
		} else {
			output = aggregateTaskCounter(MRCounter.HDFS_BYTES_WRITTEN,
					mrJob.getReduceAttempts(MRExecutionStatus.SUCCESS));

			if (output == 0L) {
				output = aggregateTaskCounter(MRCounter.S3N_BYTES_WRITTEN,
						mrJob.getReduceAttempts(MRExecutionStatus.SUCCESS));
			}

		}

		return output;
	}

	public static boolean generateDataTransfers(MRJobInfo job, Configuration conf)
  {
    job.getDataTransfers().clear();
    List redAttempts = job.getReduceAttempts(MRExecutionStatus.SUCCESS);

    int numReducers = redAttempts.size();
    if (numReducers == 0)
    {
      return false;
    }

    double totalShuffle = 0.0D;
    double[] redSizeRatio = new double[numReducers];
    double[] redTimeRatio = new double[numReducers];

    for (int i = 0; i < numReducers; i++) {
      redSizeRatio[i] = ((MRReduceAttemptInfo)redAttempts.get(i)).getProfile().getCounter(MRCounter.REDUCE_SHUFFLE_BYTES, Long.valueOf(0L)).longValue();

      redTimeRatio[i] = (redSizeRatio[i] != 0.0D ? ((MRReduceAttemptInfo)redAttempts.get(i)).getProfile().getTiming(MRTaskPhase.SHUFFLE, Double.valueOf(0.0D)).doubleValue() / redSizeRatio[i] : 0.0D);

      totalShuffle += redSizeRatio[i];
    }

    if (totalShuffle == 0.0D)
    {
      return false;
    }

    for (int i = 0; i < numReducers; i++) {
      redSizeRatio[i] /= totalShuffle;
    }

    long comprSize = 0L;
    long uncomprSize = 0L;
    boolean isCompr = conf.getBoolean("mapred.compress.map.output", false);
    double comprRatio = job.getProfile().getAvgReduceProfile().getStatistic(MRStatistics.INTERM_COMPRESS_RATIO, Double.valueOf(1.0D)).doubleValue();

    List mapAttempts = job.getMapAttempts(MRExecutionStatus.SUCCESS);

    for (MRMapAttemptInfo mapAttempt : mapAttempts) {
      long outSize = mapAttempt.getProfile().getCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES, Long.valueOf(0L)).longValue();

      for (int i = 0; i < numReducers; i++)
      {
        comprSize = Math.round(outSize * redSizeRatio[i]);
        if (isCompr)
          uncomprSize = Math.round(comprSize / comprRatio);
        else {
          uncomprSize = comprSize;
        }
        if (comprSize != 0L) {
          DataTransfer transfer = new DataTransfer(mapAttempt, (MRReduceAttemptInfo)redAttempts.get(i), comprSize, uncomprSize);

          long duration = ()Math.ceil(comprSize * redTimeRatio[i]);

          transfer.setEndTime(new Date(transfer.getStartTime().getTime() + duration));

          job.addDataTransfer(transfer);
        }
      }
    }

    return true;
  }

	public static MRJobProfile generatePartialProfile(MRJobInfo mrJob) {
		MRJobProfile profile = new MRJobProfile(mrJob.getExecId());

		profile.addCounter(MRCounter.MAP_TASKS,
				Long.valueOf(mrJob.getMapTasks().size()));

		profile.addCounter(MRCounter.REDUCE_TASKS,
				Long.valueOf(mrJob.getReduceTasks().size()));

		for (MRMapAttemptInfo a : mrJob
				.getMapAttempts(MRExecutionStatus.SUCCESS)) {
			profile.addMapProfile(a.getProfile());
		}

		for (MRReduceAttemptInfo a : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
			profile.addReduceProfile(a.getProfile());
		}

		profile.updateProfile();
		return profile;
	}

	public static String[] getInputDirs(Configuration conf) {
		String mrInputDirs = conf.get("mapred.input.dir");
		String pigInputDirs = conf.get("pig.input.dirs");

		if ((mrInputDirs == null) && (pigInputDirs != null))
			return StringUtils.split(pigInputDirs);
		if ((mrInputDirs != null) && (pigInputDirs == null))
			return StringUtils.split(mrInputDirs);
		if ((mrInputDirs == null) && (pigInputDirs == null)) {
			return new String[0];
		}

		String[] mrInputs = StringUtils.split(mrInputDirs);
		String[] pigInputs = StringUtils.split(pigInputDirs);
		return mrInputs.length > pigInputs.length ? mrInputs : pigInputs;
	}

	public static String[] getOutputDirs(Configuration conf) {
		String outputDirs = conf.get("pig.map.output.dirs");
		if (outputDirs == null)
			outputDirs = conf.get("pig.reduce.output.dirs");
		if (outputDirs == null)
			outputDirs = conf.get("pig.mapred.output.dir");
		if (outputDirs == null) {
			outputDirs = conf.get("mapred.output.dir");
		}
		if (outputDirs == null) {
			return new String[0];
		}
		return StringUtils.split(outputDirs);
	}

	public static long getMapTaskMemory(Configuration conf) {
		return getTaskMemory(conf.get("mapred.map.child.java.opts",
				conf.get("mapred.child.java.opts", "")));
	}

	public static long getReduceTaskMemory(Configuration conf) {
		return getTaskMemory(conf.get("mapred.reduce.child.java.opts",
				conf.get("mapred.child.java.opts", "")));
	}

	public static boolean isMapTaskMemorySet(Configuration conf) {
		return isTaskMemorySet(conf.get("mapred.map.child.java.opts",
				conf.get("mapred.child.java.opts", "")));
	}

	public static boolean isReduceTaskMemorySet(Configuration conf) {
		return isTaskMemorySet(conf.get("mapred.reduce.child.java.opts",
				conf.get("mapred.child.java.opts", "")));
	}

	public static boolean isMROutputCompressionOn(Configuration conf) {
		String[] outPaths = getOutputDirs(conf);
		if (outPaths.length > 0) {
			if (isPigTempPath(conf, outPaths[0])) {
				return conf.getBoolean("pig.tmpfilecompression", false);
			}
			if (GeneralUtils.hasCompressionExtension(outPaths[0])) {
				return true;
			}
		}
		return conf.getBoolean("mapred.output.compress", false);
	}

	public static boolean isPigTempPath(Configuration conf, String path) {
		int index = path.indexOf("://");
		if (index > 0) {
			index = path.indexOf(47, index + 3);
			path = path.substring(index);
		}

		return path.startsWith(conf.get("pig.temp.dir", "/tmp"));
	}

	public static MRJobProfile loadSourceProfile(String profileIdOrFile,
			Configuration conf) {
		File profFile = new File(profileIdOrFile);
		if (profFile.exists()) {
			XMLProfileParser parser = new XMLProfileParser();
			return (MRJobProfile) parser.importXML(profFile);
		}

		MRJobLogsManager manager = new MRJobLogsManager();
		manager.setResultsDir(conf.get("starfish.profiler.output.dir"));
		return manager.getMRJobProfile(profileIdOrFile);
	}

	public static void setMapTaskMemory(Configuration conf, long memory) {
		String newJavaOpts = setTaskMemory(
				conf.get("mapred.map.child.java.opts",
						conf.get("mapred.child.java.opts", "")), memory);

		conf.set("mapred.map.child.java.opts", newJavaOpts);

		conf.set("mapred.child.java.opts", newJavaOpts);
	}

	public static void setReduceTaskMemory(Configuration conf, long memory) {
		String newJavaOpts = setTaskMemory(
				conf.get("mapred.reduce.child.java.opts",
						conf.get("mapred.child.java.opts", "")), memory);

		conf.set("mapred.reduce.child.java.opts", newJavaOpts);

		conf.set("mapred.child.java.opts", newJavaOpts);
	}

	public static void printDataTransfers(PrintStream out, MRJobInfo mrJob,
			String mode) {
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(0);
		nf.setMaximumFractionDigits(0);
		nf.setGroupingUsed(true);

		if (mode.equals(TRANSFERS_ALL)) {
			out.println("Source(Map_Attempt)\tDestination(Reduce_Attempt)\tUncompressed_Data_Size(bytes)\tCompressed_Data_Size(bytes)\tDuration(ms)");

			StringBuilder sb = new StringBuilder();
			for (DataTransfer transfer : mrJob.getDataTransfers()) {
				sb.append(transfer.getSource().getExecId());
				sb.append("\t");
				sb.append(transfer.getDestination().getExecId());
				sb.append("\t");
				sb.append(nf.format(transfer.getUncomprData()));
				sb.append("\t");
				sb.append(nf.format(transfer.getComprData()));
				sb.append("\t");
				sb.append(nf.format(transfer.getDuration()));

				out.println(sb.toString());
				sb.delete(0, sb.length());
			}
			return;
		}

		if (mode.equals(TRANSFERS_MAP)) {
			out.println("Map_Attempt\tUncompressed_Data_Size(bytes)\tCompressed_Data_Size(bytes)\tTotal Duration(ms)");

			StringBuilder sb = new StringBuilder();
			for (MRMapAttemptInfo mrMap : mrJob
					.getMapAttempts(MRExecutionStatus.SUCCESS)) {
				long duration;
				long compr;
				long uncompr = compr = duration = 0L;
				for (DataTransfer transfer : mrJob
						.getDataTransfersFromMap(mrMap)) {
					uncompr += transfer.getUncomprData();
					compr += transfer.getComprData();
					duration += transfer.getDuration();
				}

				sb.append(mrMap.getExecId());
				sb.append("\t");
				sb.append(nf.format(uncompr));
				sb.append("\t");
				sb.append(nf.format(compr));
				sb.append("\t");
				sb.append(nf.format(duration));

				out.println(sb.toString());
				sb.delete(0, sb.length());
			}
			return;
		}

		if (mode.equals(TRANSFERS_RED)) {
			out.println("Reduce_Attempt\tUncompressed_Data_Size(bytes)\tCompressed_Data_Size(bytes)\tTotal_Duration(ms)");

			StringBuilder sb = new StringBuilder();
			for (MRReduceAttemptInfo mrReduce : mrJob
					.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
				long duration;
				long compr;
				long uncompr = compr = duration = 0L;
				for (DataTransfer transfer : mrJob
						.getDataTransfersToReduce(mrReduce)) {
					uncompr += transfer.getUncomprData();
					compr += transfer.getComprData();
					duration += transfer.getDuration();
				}

				sb.append(mrReduce.getExecId());
				sb.append("\t");
				sb.append(nf.format(uncompr));
				sb.append("\t");
				sb.append(nf.format(compr));
				sb.append("\t");
				sb.append(nf.format(duration));

				out.println(sb.toString());
				sb.delete(0, sb.length());
			}
			return;
		}
	}

	public static void printMRJobDetails(PrintStream out, MRJobInfo mrJob)
  {
    out.println("MapReduce Job: ");
    out.println(new StringBuilder().append("\tJob ID:\t").append(mrJob.getExecId()).toString());
    out.println(new StringBuilder().append("\tJob Name:\t").append(mrJob.getName()).toString());
    out.println(new StringBuilder().append("\tUser Name:\t").append(mrJob.getUser()).toString());
    out.println(new StringBuilder().append("\tStart Time:\t").append(mrJob.getStartTime()).toString());
    out.println(new StringBuilder().append("\tEnd Time:\t").append(mrJob.getEndTime()).toString());
    out.println(new StringBuilder().append("\tDuration:\t").append(GeneralUtils.getFormattedDuration(mrJob.getDuration())).toString());

    out.println(new StringBuilder().append("\tInput Size:\t").append(GeneralUtils.getFormattedSize(calculateJobInputSize(mrJob))).toString());

    out.println(new StringBuilder().append("\tOutput Size:\t").append(GeneralUtils.getFormattedSize(calculateJobOutputSize(mrJob))).toString());

    out.println(new StringBuilder().append("\tStatus: \t").append(mrJob.getStatus()).toString());
    out.println();

    int mapSuccessCount = 0;
    int mapFailedCount = 0;
    int mapKilledCount = 0;
    int dataLocalCount = 0;
    int rackLocalCount = 0;
    int nonLocalCount = 0;

    for (MRMapInfo mrMap : mrJob.getMapTasks()) {
      for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts())
      {
        switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$MRExecutionStatus[mrMapAttempt.getStatus().ordinal()]) {
        case 1:
          mapSuccessCount++;
          break;
        case 2:
          mapFailedCount++;
          break;
        case 3:
          mapKilledCount++;
          break;
        }

        switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$DataLocality[mrMapAttempt.getDataLocality().ordinal()]) {
        case 1:
          dataLocalCount++;
          break;
        case 2:
          rackLocalCount++;
          break;
        case 3:
          nonLocalCount++;
        }

      }

    }

    long mapAvg = Math.round(calculateDurationAverage(mrJob.getMapTasks()));

    long mapDev = Math.round(calculateDurationDeviation(mrJob.getMapTasks()));

    long mapInput = calculateJobInputSize(mrJob);
    long mapOutput = 0L;
    if (mrJob.isMapOnly())
      mapOutput = calculateJobOutputSize(mrJob);
    else {
      mapOutput = aggregateTaskCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES, mrJob.getMapAttempts(MRExecutionStatus.SUCCESS));
    }

    if (mapSuccessCount > 0) {
      mapInput = Math.round(mapInput / mapSuccessCount);
      mapOutput = Math.round(mapOutput / mapSuccessCount);
    }

    out.println("Map Statistics:");
    out.println(new StringBuilder().append("\tCount:\t").append(mrJob.getMapTasks().size()).toString());
    out.println(new StringBuilder().append("\tSuccessful:\t").append(mapSuccessCount).toString());
    out.println(new StringBuilder().append("\tFailed:\t").append(mapFailedCount).toString());
    out.println(new StringBuilder().append("\tKilled:\t").append(mapKilledCount).toString());
    out.println(new StringBuilder().append("\tData Local:\t").append(dataLocalCount).toString());
    out.println(new StringBuilder().append("\tRack Local:\t").append(rackLocalCount).toString());
    out.println(new StringBuilder().append("\tNon Local:\t").append(nonLocalCount).toString());
    out.println(new StringBuilder().append("\tAvg Duration:\t").append(GeneralUtils.getFormattedDuration(mapAvg)).toString());

    out.println(new StringBuilder().append("\tSD of Duration:\t").append(GeneralUtils.getFormattedDuration(mapDev)).toString());

    out.println(new StringBuilder().append("\tAvg Input Read:\t").append(GeneralUtils.getFormattedSize(mapInput)).toString());

    out.println(new StringBuilder().append("\tAvg Output Written:\t").append(GeneralUtils.getFormattedSize(mapOutput)).toString());

    out.println();

    if (mrJob.isMapOnly()) {
      return;
    }

    int redSuccessCount = 0;
    int redFailedCount = 0;
    int redKilledCount = 0;

    long sumShuffleDur = 0L;
    long sumSortDur = 0L;
    long sumReduceDur = 0L;
    long sumTotalDur = 0L;

    for (MRReduceInfo mrReduce : mrJob.getReduceTasks()) {
      for (MRReduceAttemptInfo mrReduceAttempt : mrReduce.getAttempts())
      {
        switch (1.$SwitchMap$edu$duke$starfish$profile$profileinfo$execution$MRExecutionStatus[mrReduceAttempt.getStatus().ordinal()]) {
        case 1:
          redSuccessCount++;
          break;
        case 2:
          redFailedCount++;
          break;
        case 3:
          redKilledCount++;
          break;
        }

        if (mrReduceAttempt.getStatus() == MRExecutionStatus.SUCCESS) {
          sumShuffleDur += mrReduceAttempt.getShuffleDuration();
          sumSortDur += mrReduceAttempt.getSortDuration();
          sumReduceDur += mrReduceAttempt.getReduceDuration();
          sumTotalDur += mrReduceAttempt.getDuration();
        }
      }

    }

    double numReds = mrJob.getReduceTasks().size();
    long redDev = Math.round(calculateDurationDeviation(mrJob.getReduceTasks()));

    long redInput = aggregateTaskCounter(MRCounter.REDUCE_SHUFFLE_BYTES, mrJob.getReduceAttempts(MRExecutionStatus.SUCCESS));

    long redOutput = calculateJobOutputSize(mrJob);
    if (mapSuccessCount > 0) {
      redInput = Math.round(redInput / redSuccessCount);
      redOutput = Math.round(redOutput / redSuccessCount);
    }

    out.println("Reduce Statistics:");
    out.println(new StringBuilder().append("\tCount:\t").append(mrJob.getReduceTasks().size()).toString());
    out.println(new StringBuilder().append("\tSuccessful:\t").append(redSuccessCount).toString());
    out.println(new StringBuilder().append("\tFailed:\t").append(redFailedCount).toString());
    out.println(new StringBuilder().append("\tKilled:\t").append(redKilledCount).toString());
    out.println(new StringBuilder().append("\tAvg Shuffle Duration:\t").append(GeneralUtils.getFormattedDuration(Math.round(sumShuffleDur / numReds))).toString());

    out.println(new StringBuilder().append("\tAvg Sort Duration:\t").append(GeneralUtils.getFormattedDuration(Math.round(sumSortDur / numReds))).toString());

    out.println(new StringBuilder().append("\tAvg Reduce Duration:\t").append(GeneralUtils.getFormattedDuration(Math.round(sumReduceDur / numReds))).toString());

    out.println(new StringBuilder().append("\tAvg Total Duration:\t").append(GeneralUtils.getFormattedDuration(Math.round(sumTotalDur / numReds))).toString());

    out.println(new StringBuilder().append("\tSD of Total Duration:\t").append(GeneralUtils.getFormattedDuration(Math.round((float)redDev))).toString());

    out.println(new StringBuilder().append("\tAvg Data Shuffled:\t").append(GeneralUtils.getFormattedSize(redInput)).toString());

    out.println(new StringBuilder().append("\tAvg Output Written:\t").append(GeneralUtils.getFormattedSize(redOutput)).toString());

    out.println();
  }

	public static void printMRJobTimeline(PrintStream out, MRJobInfo mrJob) {
		TimelineCalc timeline = new TimelineCalc(mrJob.getStartTime(),
				mrJob.getEndTime());

		timeline.addJob(mrJob);
		timeline.printTimeline(out);
	}

	public static void printMRMapInfo(PrintStream out, List<MRMapInfo> tasks) {
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(0);
		nf.setMaximumFractionDigits(0);
		nf.setGroupingUsed(true);

		out.println("TaskID\tHost\tStatus\tLocality\tDuration(ms)\tInput(bytes)\tOutput(bytes)");

		for (MRMapInfo task : tasks) {
			MRMapAttemptInfo attempt = task.getSuccessfulAttempt();
			if (attempt != null) {
				out.print(task.getExecId());
				out.print("\t");
				out.print(attempt.getTaskTracker().getHostName());
				out.print("\t");
				out.print(attempt.getStatus());
				out.print("\t");
				out.print(attempt.getDataLocality());
				out.print("\t");
				out.print(nf.format(attempt.getDuration()));

				long input = attempt
						.getProfile()
						.getCounter(MRCounter.HDFS_BYTES_READ, Long.valueOf(0L))
						.longValue();

				if (input == 0L) {
					input = attempt
							.getProfile()
							.getCounter(MRCounter.S3N_BYTES_READ,
									Long.valueOf(0L)).longValue();
				}
				long output = attempt
						.getProfile()
						.getCounter(MRCounter.MAP_OUTPUT_MATERIALIZED_BYTES,
								Long.valueOf(0L)).longValue();

				if (output == 0L) {
					output = attempt
							.getProfile()
							.getCounter(MRCounter.HDFS_BYTES_WRITTEN,
									Long.valueOf(0L)).longValue();
				}
				if (output == 0L) {
					output = attempt
							.getProfile()
							.getCounter(MRCounter.S3N_BYTES_WRITTEN,
									Long.valueOf(0L)).longValue();
				}

				out.print("\t");
				out.print(nf.format(input));
				out.print("\t");
				out.println(nf.format(output));
			} else {
				out.print(task.getExecId());
				out.print("\tNA\t");
				out.print(task.getStatus());
				out.print("\tNA\t");
				out.print(nf.format(task.getDuration()));
				out.println("\tNA\tNA");
			}
		}
	}

	public static void printMRReduceInfo(PrintStream out,
			List<MRReduceInfo> tasks) {
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumFractionDigits(0);
		nf.setMaximumFractionDigits(0);
		nf.setGroupingUsed(true);

		out.println("TaskID\tHost\tStatus\tShuffle_Duration(ms)\tSort_Duration(ms)\tReduce_Duration(ms)\tTotal_Duration(ms)\tShuffle_Data(bytes)\tOutput_Data(bytes)");

		for (MRReduceInfo task : tasks) {
			MRReduceAttemptInfo attempt = task.getSuccessfulAttempt();
			if (attempt != null) {
				out.print(task.getExecId());
				out.print("\t");
				out.print(attempt.getTaskTracker().getHostName());
				out.print("\t");
				out.print(attempt.getStatus());
				out.print("\t");
				out.print(nf.format(attempt.getShuffleDuration()));
				out.print("\t");
				out.print(nf.format(attempt.getSortDuration()));
				out.print("\t");
				out.print(nf.format(attempt.getReduceDuration()));
				out.print("\t");
				out.print(nf.format(attempt.getDuration()));

				long input = attempt
						.getProfile()
						.getCounter(MRCounter.REDUCE_SHUFFLE_BYTES,
								Long.valueOf(0L)).longValue();

				long output = attempt
						.getProfile()
						.getCounter(MRCounter.HDFS_BYTES_WRITTEN,
								Long.valueOf(0L)).longValue();

				if (output == 0L) {
					output = attempt
							.getProfile()
							.getCounter(MRCounter.S3N_BYTES_WRITTEN,
									Long.valueOf(0L)).longValue();
				}

				out.print("\t");
				out.print(nf.format(input));
				out.print("\t");
				out.println(nf.format(output));
			} else {
				out.print(task.getExecId());
				out.print("\tNA\t");
				out.print(task.getStatus());
				out.print("\tNA\tNA\tNA\t");
				out.print(nf.format(task.getDuration()));
				out.println("\tNA\tNA");
			}
		}
	}

	private static void adjustMapProfilesForCompression(
			MRMapProfile profNoCompr, MRMapProfile profWithCompr,
			MRMapProfile profResult) {
		for (MRCostFactors factor : MRCostFactors.values()) {
			if ((!profNoCompr.containsCostFactor(factor))
					|| (!profWithCompr.containsCostFactor(factor)))
				continue;
			profResult.addCostFactor(factor, Double.valueOf((profNoCompr
					.getCostFactor(factor).doubleValue() + profWithCompr
					.getCostFactor(factor).doubleValue()) / 2.0D));
		}

		if (profWithCompr.containsStatistic(MRStatistics.INPUT_COMPRESS_RATIO)) {
			adjustTaskInputCompression(profNoCompr, profWithCompr, profResult);
		}

		if (profWithCompr.containsStatistic(MRStatistics.INTERM_COMPRESS_RATIO)) {
			double avgRecSize = profWithCompr.getCounter(
					MRCounter.MAP_OUTPUT_BYTES).longValue()
					/ profWithCompr.getCounter(MRCounter.MAP_OUTPUT_RECORDS)
							.longValue();

			adjustTaskIntermCompression(profNoCompr, profWithCompr, profResult,
					avgRecSize,
					profWithCompr.getCounter(MRCounter.SPILLED_RECORDS)
							.longValue());
		}

		if (profWithCompr.containsStatistic(MRStatistics.OUT_COMPRESS_RATIO))
			adjustTaskOutputCompression(profNoCompr, profWithCompr, profResult,
					profWithCompr.getCounter(MRCounter.MAP_OUTPUT_RECORDS)
							.longValue(),
					profWithCompr.getCounter(MRCounter.MAP_OUTPUT_BYTES)
							.longValue());
	}

	private static void adjustReduceProfilesForCompression(
			MRReduceProfile profNoCompr, MRReduceProfile profWithCompr,
			MRReduceProfile profResult) {
		for (MRCostFactors factor : MRCostFactors.values()) {
			if ((!profNoCompr.containsCostFactor(factor))
					|| (!profWithCompr.containsCostFactor(factor)))
				continue;
			profResult.addCostFactor(factor, Double.valueOf((profNoCompr
					.getCostFactor(factor).doubleValue() + profWithCompr
					.getCostFactor(factor).doubleValue()) / 2.0D));
		}

		if (profWithCompr.containsStatistic(MRStatistics.INTERM_COMPRESS_RATIO)) {
			double avgRecSize = profWithCompr.getCounter(
					MRCounter.REDUCE_INPUT_BYTES).longValue()
					/ profWithCompr.getCounter(MRCounter.REDUCE_INPUT_RECORDS)
							.longValue();

			adjustTaskIntermCompression(profNoCompr, profWithCompr, profResult,
					avgRecSize,
					profWithCompr.getCounter(MRCounter.SPILLED_RECORDS)
							.longValue());

			double uncomprCost = profWithCompr.getCostFactor(
					MRCostFactors.NETWORK_COST).doubleValue()
					- profNoCompr.getCostFactor(MRCostFactors.NETWORK_COST)
							.doubleValue();

			if (uncomprCost < 0.0D) {
				uncomprCost = 0.0D;
			}

			uncomprCost = (uncomprCost + profResult.getCostFactor(
					MRCostFactors.INTERM_UNCOMPRESS_CPU_COST).doubleValue()) / 2.0D;

			profResult.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					Double.valueOf(uncomprCost));

			profResult.addCostFactor(MRCostFactors.NETWORK_COST,
					profNoCompr.getCostFactor(MRCostFactors.NETWORK_COST));
		}

		if (profWithCompr.containsStatistic(MRStatistics.OUT_COMPRESS_RATIO))
			adjustTaskOutputCompression(profNoCompr, profWithCompr, profResult,
					profWithCompr.getCounter(MRCounter.REDUCE_OUTPUT_RECORDS)
							.longValue(),
					profWithCompr.getCounter(MRCounter.REDUCE_OUTPUT_BYTES)
							.longValue());
	}

	private static void adjustTaskInputCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult) {
		if (profNoCompr.containsStatistic(MRStatistics.INPUT_COMPRESS_RATIO)) {
			double halfReadCost = (profNoCompr.getCostFactor(
					MRCostFactors.READ_HDFS_IO_COST).doubleValue() + profWithCompr
					.getCostFactor(MRCostFactors.READ_HDFS_IO_COST)
					.doubleValue()) / 4.0D;

			profResult.addCostFactor(MRCostFactors.READ_HDFS_IO_COST,
					Double.valueOf(halfReadCost));

			profResult.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
					Double.valueOf(halfReadCost));
		} else {
			profResult.addStatistic(MRStatistics.INPUT_COMPRESS_RATIO,
					profWithCompr
							.getStatistic(MRStatistics.INPUT_COMPRESS_RATIO));

			double uncomprCost = profWithCompr.getCostFactor(
					MRCostFactors.READ_HDFS_IO_COST).doubleValue()
					- profNoCompr
							.getCostFactor(MRCostFactors.READ_HDFS_IO_COST)
							.doubleValue();

			if (uncomprCost < 0.0D)
				uncomprCost = 0.0D;
			profResult.addCostFactor(MRCostFactors.INPUT_UNCOMPRESS_CPU_COST,
					Double.valueOf(uncomprCost));

			profResult.addCostFactor(MRCostFactors.READ_HDFS_IO_COST,
					profNoCompr.getCostFactor(MRCostFactors.READ_HDFS_IO_COST));
		}
	}

	private static void adjustTaskIntermCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult,
			double avgRecSize, long numRecs) {
		profResult.addStatistic(MRStatistics.INTERM_COMPRESS_RATIO,
				profWithCompr.getStatistic(MRStatistics.INTERM_COMPRESS_RATIO));

		if ((avgRecSize == 0.0D) || (numRecs == 0L)) {
			return;
		}

		if ((profNoCompr.containsCostFactor(MRCostFactors.READ_LOCAL_IO_COST))
				&& (profWithCompr
						.containsCostFactor(MRCostFactors.READ_LOCAL_IO_COST))) {
			double readCost = profNoCompr.getCostFactor(
					MRCostFactors.READ_LOCAL_IO_COST).doubleValue();

			double uncomprCost = profWithCompr.getCostFactor(
					MRCostFactors.READ_LOCAL_IO_COST).doubleValue()
					- readCost;

			profResult.addCostFactor(MRCostFactors.READ_LOCAL_IO_COST,
					Double.valueOf(readCost));

			profResult.addCostFactor(MRCostFactors.INTERM_UNCOMPRESS_CPU_COST,
					Double.valueOf(uncomprCost < 0.0D ? 0.0D : uncomprCost));
		}

		if ((profNoCompr.containsCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST))
				&& (profWithCompr
						.containsCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST))
				&& (profWithCompr.containsCounter(MRCounter.FILE_BYTES_WRITTEN))) {
			double writeCost = profNoCompr.getCostFactor(
					MRCostFactors.WRITE_LOCAL_IO_COST).doubleValue();

			double comprCost = (profWithCompr.getCostFactor(
					MRCostFactors.WRITE_LOCAL_IO_COST).doubleValue() - writeCost)
					* profWithCompr.getCounter(MRCounter.FILE_BYTES_WRITTEN)
							.longValue() / (numRecs * avgRecSize);

			profResult.addCostFactor(MRCostFactors.WRITE_LOCAL_IO_COST,
					Double.valueOf(writeCost));

			profResult.addCostFactor(MRCostFactors.INTERM_COMPRESS_CPU_COST,
					Double.valueOf(comprCost < 0.0D ? 0.0D : comprCost));
		}
	}

	private static void adjustTaskOutputCompression(MRTaskProfile profNoCompr,
			MRTaskProfile profWithCompr, MRTaskProfile profResult,
			long numRecs, long numBytes) {
		profResult.addStatistic(MRStatistics.OUT_COMPRESS_RATIO,
				profWithCompr.getStatistic(MRStatistics.OUT_COMPRESS_RATIO));

		if ((numRecs == 0L) || (numBytes == 0L)) {
			return;
		}

		double avgRecSize = numBytes / numRecs;
		double comprCost = (profWithCompr.getCostFactor(
				MRCostFactors.WRITE_HDFS_IO_COST).doubleValue() - profNoCompr
				.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST).doubleValue())
				* profWithCompr.getCounter(
						MRCounter.HDFS_BYTES_WRITTEN,
						profWithCompr.getCounter(MRCounter.S3N_BYTES_WRITTEN,
								Long.valueOf(0L))).longValue()
				/ (numRecs * avgRecSize);

		if (comprCost < 0.0D)
			comprCost = 0.0D;
		profResult.addCostFactor(MRCostFactors.OUTPUT_COMPRESS_CPU_COST,
				Double.valueOf(comprCost));

		profResult.addCostFactor(MRCostFactors.WRITE_HDFS_IO_COST,
				profNoCompr.getCostFactor(MRCostFactors.WRITE_HDFS_IO_COST));
	}

	private static long getTaskMemory(String javaOpts) {
		if ((javaOpts == null) || (javaOpts.equals(""))) {
			return 209715200L;
		}
		Matcher m = jvmMem.matcher(javaOpts);
		if (m.find()) {
			if ((m.group(2).equals("m")) || (m.group(2).equals("M")))
				return Long.parseLong(m.group(1)) << 20;
			if ((m.group(2).equals("g")) || (m.group(2).equals("G"))) {
				return Long.parseLong(m.group(1)) << 30;
			}
			return Long.parseLong(m.group(1));
		}
		return 209715200L;
	}

	private static boolean isTaskMemorySet(String javaOpts) {
		if ((javaOpts == null) || (javaOpts.equals(""))) {
			return false;
		}
		Matcher m = jvmMem.matcher(javaOpts);
		return m.find();
	}

	private static String setTaskMemory(String currJavaOpts, long memory) {
		String taskMem = new StringBuilder().append("-Xmx")
				.append(memory >> 20).append("M").toString();

		String newJavaOpts = null;
		Matcher m = jvmMem.matcher(currJavaOpts);

		if (m.find())
			newJavaOpts = m.replaceAll(taskMem);
		else {
			newJavaOpts = new StringBuilder().append(currJavaOpts).append(" ")
					.append(taskMem).toString();
		}

		return newJavaOpts.trim();
	}
}