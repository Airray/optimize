package edu.duke.starfish.profile.profileinfo.execution.profile;

import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.Map;

public class MRTaskProfile extends MRExecProfile {
	private String taskId;
	private Map<MRTaskPhase, Double> timings;
	private int numTasks;

	public MRTaskProfile(String taskId) {
		this.taskId = taskId;
		this.timings = null;
		this.numTasks = 1;
	}

	public MRTaskProfile(MRTaskProfile other) {
		super(other);

		this.taskId = other.taskId;
		if (other.timings != null)
			this.timings = new EnumMap(other.timings);
		this.numTasks = other.numTasks;
	}

	public void printProfile(PrintStream out) {
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(0);

		out.println("Tasks:\n\t" + this.numTasks);

		if (!getCounters().isEmpty()) {
			out.println("Counters:");
			printEnumToNumberMap(out, getCounters(), nf);
		}

		if (containsAuxCounters()) {
			out.println("Auxiliary Counters:");
			printStringToNumberMap(out, getAuxCounters(), nf);
		}

		nf.setMinimumFractionDigits(6);
		nf.setMaximumFractionDigits(6);

		if (!getStatistics().isEmpty()) {
			out.println("Statistics:");
			printEnumToNumberMap(out, getStatistics(), nf);
		}

		if (!getCostFactors().isEmpty()) {
			out.println("Cost Factors:");
			printEnumToNumberMap(out, getCostFactors(), nf);
		}

		if (!getTimings().isEmpty()) {
			out.println("Timings:");
			printEnumToNumberMap(out, getTimings(), nf);
		}
		out.println("");
	}

	public String getTaskId() {
		return this.taskId;
	}

	public Map<MRTaskPhase, Double> getTimings() {
		if (this.timings == null)
			this.timings = new EnumMap(MRTaskPhase.class);
		return this.timings;
	}

	public int getNumTasks() {
		return this.numTasks;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public void setNumTasks(int numTasks) {
		this.numTasks = numTasks;
	}

	public void addTiming(MRTaskPhase phase, Double value) {
		if (this.timings == null)
			this.timings = new EnumMap(MRTaskPhase.class);
		this.timings.put(phase, value);
	}

	public void addTimings(Map<MRTaskPhase, Double> timings) {
		if (this.timings == null)
			this.timings = new EnumMap(MRTaskPhase.class);
		this.timings.putAll(timings);
	}

	public boolean containsTiming(MRTaskPhase phase) {
		return (this.timings != null) && (this.timings.containsKey(phase));
	}

	public Double getTiming(MRTaskPhase phase, Double defaultValue) {
		if ((this.timings == null) || (!this.timings.containsKey(phase))) {
			return defaultValue;
		}
		return (Double) this.timings.get(phase);
	}

	public boolean isEmpty() {
		return (super.isEmpty())
				&& ((this.timings == null) || (this.timings.isEmpty()));
	}

	public void clearProfile() {
		super.clearProfile();
		if (this.timings != null)
			this.timings.clear();
	}

	public int hashCode() {
		int hash = super.hashCode();
		hash = 31 * hash + (this.taskId == null ? 0 : this.taskId.hashCode());
		hash = 37 * hash + (this.timings == null ? 0 : this.timings.hashCode());
		return hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof MRTaskProfile))
			return false;
		MRTaskProfile other = (MRTaskProfile) obj;
		if (this.taskId == null) {
			if (other.taskId != null)
				return false;
		} else if (!this.taskId.equals(other.taskId))
			return false;
		if (this.timings == null) {
			if (other.timings != null)
				return false;
		} else if (!this.timings.equals(other.timings))
			return false;
		return true;
	}

	public String toString() {
		return "MRTaskProfile [task=" + this.taskId + ", counters="
				+ getCounters().size() + ", stats=" + getStatistics().size()
				+ ", costs=" + getCostFactors().size() + ", timings="
				+ getTimings().size() + "]";
	}
}