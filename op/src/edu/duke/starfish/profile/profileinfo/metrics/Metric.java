package edu.duke.starfish.profile.profileinfo.metrics;

import java.util.Date;

public class Metric implements Comparable<Metric> {
	private Date time;
	private double value;
	private int hash = -1;

	public Metric(Date time, double value) {
		this.time = time;
		this.value = value;
	}

	public Metric(Metric other) {
		this.time = (other.time == null ? null : new Date(other.time.getTime()));
		this.value = other.value;
	}

	public Date getTime() {
		return this.time;
	}

	public double getValue() {
		return this.value;
	}

	public int hashCode() {
		if (this.hash == -1) {
			this.hash = 1;
			this.hash = (31 * this.hash + (this.time == null ? 0 : this.time
					.hashCode()));
			long temp = Double.doubleToLongBits(this.value);
			this.hash = (37 * this.hash + (int) (temp ^ temp >>> 32));
		}
		return this.hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Metric))
			return false;
		Metric other = (Metric) obj;
		if (this.time == null) {
			if (other.time != null)
				return false;
		} else if (!this.time.equals(other.time)) {
			return false;
		}

		return Double.doubleToLongBits(this.value) == Double
				.doubleToLongBits(other.value);
	}

	public String toString() {
		return "Metric [time=" + this.time + ", value=" + this.value + "]";
	}

	public int compareTo(Metric o) {
		return getTime().compareTo(o.getTime());
	}
}