package edu.duke.starfish.profile.profileinfo.metrics;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRReduceAttemptInfo;
import java.util.Date;

public class DataTransfer {
	private MRMapAttemptInfo source;
	private MRReduceAttemptInfo destination;
	private Date startTime;
	private Date endTime;
	private long comprData;
	private long uncomprData;
	private int hash = -1;

	public DataTransfer(MRMapAttemptInfo source,
			MRReduceAttemptInfo destination, long comprData, long uncomprData) {
		this.source = source;
		this.destination = destination;
		this.comprData = comprData;
		this.uncomprData = uncomprData;

		if (source.getEndTime().after(destination.getStartTime()))
			this.startTime = source.getEndTime();
		else
			this.startTime = destination.getStartTime();
		this.endTime = destination.getShuffleEndTime();
	}

	public DataTransfer(DataTransfer other) {
		this.source = (other.source == null ? null : new MRMapAttemptInfo(
				other.source));

		this.destination = (other.destination == null ? null
				: new MRReduceAttemptInfo(other.destination));

		this.comprData = other.comprData;
		this.uncomprData = other.uncomprData;

		this.startTime = (other.startTime == null ? null : new Date(
				other.startTime.getTime()));

		this.endTime = (other.endTime == null ? null : new Date(
				other.endTime.getTime()));
	}

	public MRMapAttemptInfo getSource() {
		return this.source;
	}

	public MRReduceAttemptInfo getDestination() {
		return this.destination;
	}

	public long getDuration() {
		if ((this.endTime != null) && (this.startTime != null)) {
			return this.endTime.getTime() - this.startTime.getTime();
		}
		return 0L;
	}

	public Date getStartTime() {
		return this.startTime;
	}

	public Date getEndTime() {
		return this.endTime;
	}

	public long getComprData() {
		return this.comprData;
	}

	public long getUncomprData() {
		return this.uncomprData;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = 1;
			this.hash = (31 * this.hash + (int) (this.comprData ^ this.comprData >>> 32));
			this.hash = (37 * this.hash + (int) (this.uncomprData ^ this.uncomprData >>> 32));
			this.hash = (41 * this.hash + (this.destination == null ? 0
					: this.destination.hashCode()));

			this.hash = (43 * this.hash + (this.source == null ? 0
					: this.source.hashCode()));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DataTransfer))
			return false;
		DataTransfer other = (DataTransfer) obj;
		if (this.comprData != other.comprData)
			return false;
		if (this.uncomprData != other.uncomprData)
			return false;
		if (this.destination == null) {
			if (other.destination != null)
				return false;
		} else if (!this.destination.equals(other.destination))
			return false;
		if (this.source == null) {
			if (other.source != null)
				return false;
		} else if (!this.source.equals(other.source))
			return false;
		return true;
	}

	public String toString() {
		return "DataTransfer [source=" + this.source.getExecId()
				+ ", destination=" + this.destination.getExecId()
				+ ", compressed data=" + this.comprData
				+ ", uncompressed data=" + this.uncomprData + "]";
	}
}