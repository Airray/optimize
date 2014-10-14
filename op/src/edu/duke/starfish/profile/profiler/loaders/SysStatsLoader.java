package edu.duke.starfish.profile.profiler.loaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SysStatsLoader {
	private static final Log LOG = LogFactory.getLog(SysStatsLoader.class);
	private String monitorDir;
	private Map<String, File> ioStatFiles;
	private Map<String, File> vmStatFiles;
	private final String IOSTAT_PREFIX = "iostat_output-";
	private final String VMSTAT_PREFIX = "vmstat_output-";
	private final String AVG_CPU = "avg-cpu";
	private final String DEVICE = "Device";
	private final String EMPTY = "";
	private final String TAB = "\t";
	private final String SW = "sw";

	private final Pattern p = Pattern.compile("\\s+");

	public SysStatsLoader(String monitorDir) {
		this.monitorDir = monitorDir;
		this.ioStatFiles = new HashMap();
		this.vmStatFiles = new HashMap();

		readMonitorDirectory();
	}

	public boolean exportCPUStats(PrintStream out, String nodeName) {
		return exportCPUStats(out, nodeName, -9223372036854775808L,
				9223372036854775807L);
	}

	public boolean exportCPUStats(PrintStream out, String nodeName, Date start,
			Date end) {
		return exportCPUStats(out, nodeName, start.getTime() / 1000L,
				end.getTime() / 1000L);
	}

	public boolean exportMemoryStats(PrintStream out, String nodeName) {
		return exportMemoryStats(out, nodeName, -9223372036854775808L,
				9223372036854775807L);
	}

	public boolean exportMemoryStats(PrintStream out, String nodeName,
			Date start, Date end) {
		return exportMemoryStats(out, nodeName, start.getTime() / 1000L,
				end.getTime() / 1000L);
	}

	public boolean exportIOStats(PrintStream out, String nodeName) {
		return exportIOStats(out, nodeName, -9223372036854775808L,
				9223372036854775807L);
	}

	public boolean exportIOStats(PrintStream out, String nodeName, Date start,
			Date end) {
		return exportIOStats(out, nodeName, start.getTime() / 1000L,
				end.getTime() / 1000L);
	}

	private boolean exportCPUStats(PrintStream out, String nodeName,
			long start, long end) {
		if (!this.ioStatFiles.containsKey(nodeName)) {
			LOG.error("Unable to find a file for node " + nodeName);
			return false;
		}

		BufferedReader input = null;
		try {
			input = new BufferedReader(new FileReader(
					(File) this.ioStatFiles.get(nodeName)));

			String line = null;
			out.println("time\t%user\t%system\t%iowait\t%idle");
			while ((line = input.readLine()) != null) {
				if (!line.contains("avg-cpu")) {
					continue;
				}
				line = input.readLine();
				if (line == null)
					continue;
				pieces = this.p.split(line);
				if ((pieces.length == 7)
						&& (satisfyBounds(pieces[0], start, end))) {
					out.print(pieces[0]);
					out.print("\t");
					out.print(pieces[1]);
					out.print("\t");
					out.print(pieces[3]);
					out.print("\t");
					out.print(pieces[4]);
					out.print("\t");
					out.println(pieces[6]);
				}

			}

			input.close();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
			pieces = 0;
			return pieces;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			String[] pieces = 0;
			return pieces;
		} finally {
			try {
				if (input != null)
					input.close();
			} catch (IOException e) {
			}
		}
		return true;
	}

	private boolean exportMemoryStats(PrintStream out, String nodeName,
			long start, long end) {
		if (!this.vmStatFiles.containsKey(nodeName)) {
			LOG.error("Unable to find a file for node " + nodeName);
			return false;
		}

		BufferedReader input = null;
		try {
			File file = (File) this.vmStatFiles.get(nodeName);
			input = new BufferedReader(new FileReader(file));

			line = null;
			out.println("time\tswpd\tfree\tbuff\tcache");
			while ((line = input.readLine()) != null) {
				String[] pieces = this.p.split(line);
				if ((!line.contains("sw")) && (pieces.length == 17)
						&& (satisfyBounds(pieces[0], start, end))) {
					out.print(pieces[0]);
					out.print("\t");
					out.print(pieces[3]);
					out.print("\t");
					out.print(pieces[4]);
					out.print("\t");
					out.print(pieces[5]);
					out.print("\t");
					out.println(pieces[6]);
				}
			}

			input.close();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
			line = 0;
			return line;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			String line = 0;
			return line;
		} finally {
			try {
				if (input != null)
					input.close();
			} catch (IOException e) {
			}
		}
		return true;
	}

	private boolean exportIOStats(PrintStream out, String nodeName, long start,
			long end) {
		if (!this.ioStatFiles.containsKey(nodeName)) {
			LOG.error("Unable to find a file for node " + nodeName);
			return false;
		}

		BufferedReader input = null;
		try {
			input = new BufferedReader(new FileReader(
					(File) this.ioStatFiles.get(nodeName)));

			String line = null;
			hasTime = false;
			out.println("time\tMBRead/s\tMBWrite/s");
			while ((line = input.readLine()) != null) {
				if (!line.contains("Device")) {
					continue;
				}
				line = input.readLine();
				if (line == null)
					continue;
				String[] pieces = this.p.split(line);
				if ((pieces.length == 6)
						|| ((pieces.length == 7) && (satisfyBounds(pieces[0],
								start, end)))) {
					hasTime = pieces.length == 7;
					out.print(hasTime ? pieces[0] : "");
					out.print("\t");
					out.print(hasTime ? pieces[3] : pieces[2]);
					out.print("\t");
					out.println(hasTime ? pieces[4] : pieces[3]);
				}

			}

			input.close();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
			hasTime = false;
			return hasTime;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			boolean hasTime = false;
			return hasTime;
		} finally {
			try {
				if (input != null)
					input.close();
			} catch (IOException e) {
			}
		}
		return true;
	}

	private void readMonitorDirectory() {
		if (this.monitorDir == null) {
			return;
		}

		File dir = new File(this.monitorDir);
		if (!dir.isDirectory()) {
			LOG.error(dir.getAbsolutePath() + " is not a directory!");
			return;
		}

		for (File file : dir.listFiles())
			if ((file.isFile()) && (!file.isHidden())) {
				String name = file.getName();
				if (name.startsWith("iostat_output-")) {
					this.ioStatFiles.put(
							name.substring("iostat_output-".length()), file);
				} else {
					if (!name.startsWith("vmstat_output-"))
						continue;
					this.vmStatFiles.put(
							name.substring("vmstat_output-".length()), file);
				}
			}
	}

	private boolean satisfyBounds(String value, long start, long end) {
		try {
			if (value.equals("")) {
				return true;
			}

			long num = Long.parseLong(value);
			return (num >= start) && (num <= end);
		} catch (NumberFormatException e) {
		}
		return false;
	}
}