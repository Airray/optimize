package edu.duke.starfish.profile.profiler.loaders;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.metrics.DataTransfer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MRJobTransfersLoader {
	private static final Log LOG = LogFactory
			.getLog(MRJobTransfersLoader.class);
	private MRJobInfo mrJob;
	private String inputDir;
	private boolean loadedData;
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss,SSS");
	private static final String SYSLOG = "syslog";
	private static final String TRANSFERS = "transfers_";
	private static final String INFO_SHUFFLING = "org.apache.hadoop.mapred.ReduceTask: Shuffling";
	private static final String INFO_READ_SHUFFLE = "org.apache.hadoop.mapred.ReduceTask: Read";
	private static final String INFO_FAILED_SHUFFLE = "org.apache.hadoop.mapred.ReduceTask: Failed to shuffle from";
	private static final Pattern SHUFFLE_PATTERN = Pattern
			.compile("([\\d-:, ]+) (INFO|DEBUG) .* (\\d+) bytes \\((\\d+) raw bytes\\) .* from ([\\w\\d_]+)");

	private static final Pattern READ_PATTERN = Pattern
			.compile("([\\d-:, ]+) (INFO|DEBUG) .* for ([\\w\\d_]+)");

	private static final Pattern FAILED_PATTERN = Pattern
			.compile("([\\d-:, ]+) (INFO|DEBUG) .* from ([\\w\\d_]+)");

	public MRJobTransfersLoader(MRJobInfo mrJob, String inputDir) {
		this.mrJob = mrJob;
		this.inputDir = inputDir;
		this.loadedData = false;
	}

	public MRJobInfo getMrJob() {
		return this.mrJob;
	}

	public String getInputDir() {
		return this.inputDir;
	}

	public boolean loadDataTransfers(MRJobInfo mrJob) {
		if (!this.mrJob.getExecId().equalsIgnoreCase(mrJob.getExecId()))
			return false;
		if ((this.loadedData) && (this.mrJob == mrJob)) {
			return true;
		}

		if (mrJob.isMapOnly()) {
			return false;
		}

		this.mrJob = mrJob;

		File filesDir = new File(this.inputDir);
		if (!filesDir.isDirectory()) {
			LOG.error(filesDir.getAbsolutePath() + " is not a directory!");
			return false;
		}

		boolean success = false;

		for (MRReduceAttemptInfo mrReduceAttempt : mrJob
				.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
			try {
				List reducerTransfers = parseReducerSyslog(filesDir,
						mrReduceAttempt);
				if ((reducerTransfers != null) && (!reducerTransfers.isEmpty())) {
					success = true;
					mrJob.addDataTransfers(reducerTransfers);
				}
			} catch (ParseException e) {
				LOG.error("Unable to load data transfers", e);
				return false;
			}

		}

		this.loadedData = success;
		return success;
	}

	private List<DataTransfer> parseReducerSyslog(File logsDir,
			MRReduceAttemptInfo mrReduceAttempt) throws ParseException {
		File syslog = null;
		File attemptDir = new File(logsDir, mrReduceAttempt.getExecId());
		if (attemptDir.isDirectory())
			syslog = new File(attemptDir, "syslog");
		else {
			syslog = new File(logsDir, "transfers_"
					+ mrReduceAttempt.getExecId());
		}

		if (!syslog.exists())
			return null;
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(syslog));
		} catch (FileNotFoundException e) {
			LOG.error("Unable to find file: " + syslog.getAbsolutePath());
			return null;
		}

		HashMap startedTransfers = new HashMap();
		HashMap emptyTransfers = new HashMap();
		List completedTransfers = new ArrayList();
		try {
			String line = br.readLine();
			while (line != null) {
				if (line.contains("org.apache.hadoop.mapred.ReduceTask: Shuffling")) {
					Matcher matcher = SHUFFLE_PATTERN.matcher(line);
					if (matcher.find()) {
						String mapAttemptId = matcher.group(5);
						MRMapAttemptInfo mrMapAttempt = this.mrJob
								.findMRMapAttempt(mapAttemptId);

						if (mrMapAttempt == null) {
							LOG.error("The map attempt id " + mapAttemptId
									+ " was not found in the job");

							Object localObject1 = null;
							return localObject1;
						}
						DataTransfer transfer = new DataTransfer(mrMapAttempt,
								mrReduceAttempt, Long.parseLong(matcher
										.group(4)), Long.parseLong(matcher
										.group(3)));

						transfer.setStartTime(DATE_FORMAT.parse(matcher
								.group(1)));

						startedTransfers.put(mapAttemptId, transfer);

						if (transfer.getUncomprData() == 2L)
							emptyTransfers.put(mapAttemptId, transfer);
					} else {
						LOG.error("Unexpected line format from: ");
						LOG.error(line);
					}
				} else {
					DataTransfer dataTransfer;
					if (line.contains("org.apache.hadoop.mapred.ReduceTask: Read")) {
						Matcher matcher = READ_PATTERN.matcher(line);
						if (matcher.find()) {
							String mapAttemptId = matcher.group(3);
							if (emptyTransfers.containsKey(mapAttemptId)) {
								emptyTransfers.remove(mapAttemptId);
								startedTransfers.remove(mapAttemptId);
							} else if (startedTransfers
									.containsKey(mapAttemptId)) {
								dataTransfer = (DataTransfer) startedTransfers
										.get(mapAttemptId);

								dataTransfer.setEndTime(DATE_FORMAT
										.parse(matcher.group(1)));

								startedTransfers.remove(mapAttemptId);
								completedTransfers.add(dataTransfer);
							} else {
								LOG.error("The map attempt id " + mapAttemptId
										+ " has not been seen before");

								dataTransfer = null;
								return dataTransfer;
							}
						} else {
							LOG.error("Unexpected line format from: ");
							LOG.error(line);
						}
					} else if (line
							.contains("org.apache.hadoop.mapred.ReduceTask: Failed to shuffle from")) {
						matcher = FAILED_PATTERN.matcher(line);
						if (matcher.find()) {
							String mapAttemptId = matcher.group(3);
							if (startedTransfers.containsKey(mapAttemptId)) {
								startedTransfers.remove(mapAttemptId);
								emptyTransfers.remove(mapAttemptId);
							} else {
								LOG.error("The map attempt id " + mapAttemptId
										+ " has not been seen before");

								dataTransfer = null;
								return dataTransfer;
							}
						} else {
							LOG.error("Unexpected line format from: ");
							LOG.error(line);
						}
					}
				}

				line = br.readLine();
			}
		} catch (IOException e) {
			LOG.error("Unable to parse the task's syslog file", e);
			Matcher matcher = null;
			return matcher;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
			}
		}
		return completedTransfers;
	}
}