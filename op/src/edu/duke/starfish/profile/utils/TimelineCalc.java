package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRMapInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtasks.MRReduceInfo;
import java.io.PrintStream;
import java.util.Date;

public class TimelineCalc {
	private int[] mappers;
	private int[] shuffling;
	private int[] sorting;
	private int[] reducing;
	private int[] waste;
	private long grandStart;
	private int grandDuration;

	public TimelineCalc(Date start, Date end) {
		this.grandStart = (start.getTime() / 1000L);
		this.grandDuration = (int) Math
				.ceil((end.getTime() - start.getTime()) / 1000.0D);

		this.mappers = new int[this.grandDuration];
		this.shuffling = new int[this.grandDuration];
		this.sorting = new int[this.grandDuration];
		this.reducing = new int[this.grandDuration];
		this.waste = new int[this.grandDuration];

		for (int i = 0; i < this.grandDuration; i++) {
			int tmp130_129 = (this.sorting[i] = this.reducing[i] = this.waste[i] = 0);
			this.shuffling[i] = tmp130_129;
			this.mappers[i] = tmp130_129;
		}
	}

	public void addJob(MRJobInfo mrJob) {
		if (mrJob.getStartTime().getTime() / 1000L < this.grandStart) {
			throw new RuntimeException(
					"The job's start time cannot be before the grand start time");
		}
		if (mrJob.getEndTime().getTime() / 1000L > this.grandStart
				+ this.grandDuration) {
			throw new RuntimeException(
					"The job's end time cannot be after the grand end time");
		}

		boolean success = true;
		int start = 0;
		int shuffle = 0;
		int sort = 0;
		int end = 0;
		int time = 0;

		for (MRMapInfo mrMap : mrJob.getMapTasks()) {
			for (MRMapAttemptInfo mrMapAttempt : mrMap.getAttempts()) {
				success = mrMapAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrMapAttempt.getStartTime().getTime() / 1000L - this.grandStart);
				end = (int) (mrMapAttempt.getEndTime().getTime() / 1000L - this.grandStart);
				if (success) {
					for (time = start; time < end; time++)
						this.mappers[time] += 1;
				}
				for (time = start; time < end; time++) {
					this.waste[time] += 1;
				}
			}

		}

		for (MRReduceInfo mrRed : mrJob.getReduceTasks())
			for (MRReduceAttemptInfo mrRedAttempt : mrRed.getAttempts()) {
				success = mrRedAttempt.getStatus() == MRExecutionStatus.SUCCESS;
				start = (int) (mrRedAttempt.getStartTime().getTime() / 1000L - this.grandStart);
				end = (int) (mrRedAttempt.getEndTime().getTime() / 1000L - this.grandStart);

				if (success) {
					shuffle = (int) (mrRedAttempt.getShuffleEndTime().getTime() / 1000L - this.grandStart);
					sort = (int) (mrRedAttempt.getSortEndTime().getTime() / 1000L - this.grandStart);

					for (time = start; time < shuffle; time++)
						this.shuffling[time] += 1;
					for (time = shuffle; time < sort; time++)
						this.sorting[time] += 1;
					for (time = sort; time < end; time++)
						this.reducing[time] += 1;
				}
				for (time = start; time < end; time++)
					this.waste[time] += 1;
			}
	}

	public void printTimeline(PrintStream ps) {
		StringBuffer sb = new StringBuffer();
		ps.println("Time\tMaps\tShuffle\tMerge\tReduce\tWaste");
		for (int t = 0; t < this.grandDuration; t++) {
			sb.append(t);
			sb.append("\t");
			sb.append(this.mappers[t]);
			sb.append("\t");
			sb.append(this.shuffling[t]);
			sb.append("\t");
			sb.append(this.sorting[t]);
			sb.append("\t");
			sb.append(this.reducing[t]);
			sb.append("\t");
			sb.append(this.waste[t]);

			ps.println(sb.toString());
			sb.delete(0, sb.length());
		}
	}
}