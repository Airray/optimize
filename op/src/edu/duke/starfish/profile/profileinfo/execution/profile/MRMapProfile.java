package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;

public class MRMapProfile extends MRTaskProfile {
	private int inputIndex;

	public MRMapProfile(String taskId) {
		super(taskId);
		this.inputIndex = 0;
	}

	public MRMapProfile(MRMapProfile other) {
		super(other);
		this.inputIndex = other.inputIndex;
	}

	public int getInputIndex() {
		return this.inputIndex;
	}

	public void setInputIndex(int inputIndex) {
		this.inputIndex = inputIndex;
	}

	public void printProfile(PrintStream out) {
		out.println("MAP PROFILE:\n\t" + getTaskId());
		out.println("Input Path Index:\n\t" + this.inputIndex);

		super.printProfile(out);
	}
}