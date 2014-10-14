package edu.duke.starfish.profile.profileinfo.execution.profile;

import java.io.PrintStream;

public class MRReduceProfile extends MRTaskProfile {
	public MRReduceProfile(String taskId) {
		super(taskId);
	}

	public MRReduceProfile(MRReduceProfile other) {
		super(other);
	}

	public void printProfile(PrintStream out) {
		out.println("REDUCE PROFILE:\n\t" + getTaskId());
		super.printProfile(out);
	}
}