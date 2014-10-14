package edu.duke.starfish.profile.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

public class SFInputSplit implements Comparable<SFInputSplit> {
	org.apache.hadoop.mapred.InputSplit oldApiSplit;
	org.apache.hadoop.mapreduce.InputSplit newApiSplit;

	public SFInputSplit(org.apache.hadoop.mapred.InputSplit oldApiSplit) {
		this.oldApiSplit = oldApiSplit;
		this.newApiSplit = null;
	}

	public SFInputSplit(org.apache.hadoop.mapreduce.InputSplit newApiSplit) {
		this.oldApiSplit = null;
		this.newApiSplit = newApiSplit;
	}

	public org.apache.hadoop.mapred.InputSplit getOldApiSplit() {
		return this.oldApiSplit;
	}

	public org.apache.hadoop.mapreduce.InputSplit getNewApiSplit() {
		return this.newApiSplit;
	}

	public long getLength() throws IOException, InterruptedException {
		return this.oldApiSplit != null ? this.oldApiSplit.getLength()
				: this.newApiSplit != null ? this.newApiSplit.getLength() : 0L;
	}

	public String[] getLocations() throws IOException, InterruptedException {
		return this.oldApiSplit != null ? this.oldApiSplit.getLocations()
				: this.newApiSplit != null ? this.newApiSplit.getLocations()
						: null;
	}

	public boolean isFileSplit() {
		if ((this.newApiSplit != null)
				&& ((this.newApiSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit))) {
			return true;
		}

		return (this.oldApiSplit != null)
				&& ((this.oldApiSplit instanceof org.apache.hadoop.mapred.FileSplit));
	}

	public Path getPath() {
		if ((this.newApiSplit != null)
				&& ((this.newApiSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit))) {
			return ((org.apache.hadoop.mapreduce.lib.input.FileSplit) this.newApiSplit)
					.getPath();
		}

		if ((this.oldApiSplit != null)
				&& ((this.oldApiSplit instanceof org.apache.hadoop.mapred.FileSplit))) {
			return ((org.apache.hadoop.mapred.FileSplit) this.oldApiSplit)
					.getPath();
		}

		return null;
	}

	public int compareTo(SFInputSplit other) {
		try {
			if (getLength() == other.getLength()) {
				return 0;
			}
			return getLength() < other.getLength() ? 1 : -1;
		} catch (IOException e) {
		} catch (InterruptedException e) {
		}
		return 0;
	}

	public static List<SFInputSplit> getInputSplits(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		List result = null;

		boolean useNewApi = false;
		if (conf.get("mapreduce.inputformat.class") != null)
			useNewApi = true;
		else if (conf.get("mapred.input.format.class") != null)
			useNewApi = false;
		else {
			useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
		}
		if (useNewApi) {
			JobContext context = new JobContext(conf, null);
			conf = context.getConfiguration();
			org.apache.hadoop.mapreduce.InputFormat input = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
					.newInstance(context.getInputFormatClass(), conf);

			List splits = input.getSplits(context);

			result = new ArrayList(splits.size());
			for (org.apache.hadoop.mapreduce.InputSplit split : splits)
				result.add(new SFInputSplit(split));
		} else {
			JobConf job = new JobConf(conf);
			org.apache.hadoop.mapred.InputSplit[] splits = job.getInputFormat()
					.getSplits(job, job.getNumMapTasks());

			result = new ArrayList(splits.length);
			for (org.apache.hadoop.mapred.InputSplit split : splits) {
				result.add(new SFInputSplit(split));
			}
		}
		return result;
	}
}