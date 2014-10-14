package edu.duke.starfish.profile.sampling;

import edu.duke.starfish.profile.utils.SFInputSplit;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class ProfileSampler {
	private static final Log LOG = LogFactory.getLog(ProfileSampler.class);
	private static final NumberFormat nf = NumberFormat.getNumberInstance();

	public static boolean sampleTasksToProfile(Configuration conf) {
		double fraction = conf.getFloat("starfish.profiler.sampling.fraction",
				0.1F);

		if (fraction == 0.0D) {
			return false;
		}

		List splits = null;
		try {
			splits = SFInputSplit.getInputSplits(conf);
		} catch (IOException e) {
			LOG.error("Unable to create input splits", e);
			return false;
		} catch (InterruptedException e) {
			LOG.error("Unable to create input splits", e);
			return false;
		} catch (ClassNotFoundException e) {
			LOG.error("Unable to create input splits", e);
			return false;
		}

		if ((splits != null) && (splits.size() != 0)) {
			conf.set("mapred.task.profile.maps",
					sampleTasksToProfile(splits.size(), fraction));
		}

		int numReducers = conf.getInt("mapred.reduce.tasks", 1);
		if (numReducers != 0) {
			conf.set("mapred.task.profile.reduces",
					sampleTasksToProfile(numReducers, fraction));
		}

		nf.setMaximumFractionDigits(2);
		LOG.info(new StringBuilder().append("Profiling only ")
				.append(nf.format(fraction * 100.0D)).append("% of the tasks")
				.toString());

		return true;
	}

	public static void sampleInputSplits(JobContext job, List<InputSplit> splits) {
		Configuration conf = job.getConfiguration();
		double fraction = conf.getFloat("starfish.profiler.sampling.fraction",
				0.1F);

		if ((fraction < 0.0D) || (fraction > 1.0D)) {
			throw new RuntimeException(new StringBuilder()
					.append("ERROR: Invalid sampling fraction: ")
					.append(fraction).toString());
		}

		if ((fraction == 0.0D) || (splits.size() == 0)) {
			splits.clear();
			return;
		}
		if (fraction == 1.0D) {
			return;
		}

		int numSplits = splits.size();
		int sampleSize = (int) Math.round(numSplits * fraction);
		if (sampleSize == 0) {
			sampleSize = 1;
		}

		Collections.shuffle(splits);

		for (int i = splits.size() - 1; i >= sampleSize; i--) {
			splits.remove(i);
		}

		nf.setMaximumFractionDigits(2);
		LOG.info(new StringBuilder().append("Executing only ")
				.append(nf.format(fraction * 100.0D))
				.append("% of the map tasks").toString());
	}

	private static String convertArrayToString(int[] numbers) {
		if (numbers.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append(numbers[0]);
		for (int i = 1; i < numbers.length; i++) {
			sb.append(',');
			sb.append(numbers[i]);
		}

		return sb.toString();
	}

	private static String sampleTasksToProfile(int numTasks, double fraction) {
		String tasks = convertArrayToString(sampleIntsFromDomain(0, numTasks,
				fraction));

		tasks = new StringBuilder().append(tasks).append(",").append(numTasks)
				.append(",").append(numTasks + 1).toString();

		return tasks;
	}

	private static int[] sampleIntsFromDomain(int min, int max, double fraction) {
		int domainSize = max - min;
		if (domainSize <= 0) {
			throw new RuntimeException(new StringBuilder()
					.append("ERROR: Invalid domain range to sample from: min=")
					.append(min).append(" max=").append(max).toString());
		}

		if ((fraction < 0.0D) || (fraction > 1.0D)) {
			throw new RuntimeException(new StringBuilder()
					.append("ERROR: Invalid sampling fraction: ")
					.append(fraction).toString());
		}

		int sampleSize = (int) Math.round(domainSize * fraction);
		if (sampleSize == 0) {
			sampleSize = 1;
		}

		int[] domain = new int[domainSize];
		for (int i = 0; i < domainSize; i++) {
			domain[i] = (min + i);
		}

		Random rgen = new Random();
		for (int i = 0; i < domainSize; i++) {
			int rndPos = rgen.nextInt(domainSize);
			int temp = domain[i];
			domain[i] = domain[rndPos];
			domain[rndPos] = temp;
		}

		int[] samples = new int[sampleSize];
		for (int i = 0; i < sampleSize; i++) {
			samples[i] = domain[i];
		}
		Arrays.sort(samples);

		return samples;
	}
}