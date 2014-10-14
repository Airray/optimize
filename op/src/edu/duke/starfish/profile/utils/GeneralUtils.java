package edu.duke.starfish.profile.utils;

import java.text.NumberFormat;
import java.util.List;

public class GeneralUtils {
	private static int MS_IN_SEC = 1000;
	private static int SEC_IN_MIN = 60;
	private static int MIN_IN_HR = 60;
	private static int SEC_IN_HR = 3600;

	public static String buildMROutputName(int id, boolean mapOnly,
			boolean compress) {
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);

		StringBuilder sb = new StringBuilder();
		sb.append("part-");
		sb.append(mapOnly ? "m-" : "r-");
		sb.append(nf.format(id));
		if (compress) {
			sb.append(".deflate");
		}
		return sb.toString();
	}

	public static String convertGlobToRegEx(String line, boolean matchAll) {
		line = line.trim();
		int strLen = line.length();
		StringBuilder sb = new StringBuilder(strLen);

		if (!matchAll) {
			sb.append(".*");
		}
		boolean escaping = false;
		int inCurlies = 0;
		for (char currentChar : line.toCharArray()) {
			switch (currentChar) {
			case '*':
				if (escaping)
					sb.append("\\*");
				else
					sb.append(".*");
				escaping = false;
				break;
			case '?':
				if (escaping)
					sb.append("\\?");
				else
					sb.append('.');
				escaping = false;
				break;
			case '$':
			case '%':
			case '(':
			case ')':
			case '+':
			case '.':
			case '@':
			case '^':
			case '|':
				sb.append('\\');
				sb.append(currentChar);
				escaping = false;
				break;
			case '\\':
				if (escaping) {
					sb.append("\\\\");
					escaping = false;
				} else {
					escaping = true;
				}
				break;
			case '{':
				if (escaping) {
					sb.append("\\{");
				} else {
					sb.append('(');
					inCurlies++;
				}
				escaping = false;
				break;
			case '}':
				if ((inCurlies > 0) && (!escaping)) {
					sb.append(')');
					inCurlies--;
				} else if (escaping) {
					sb.append("\\}");
				} else {
					sb.append("}");
				}
				escaping = false;
				break;
			case ',':
				if ((inCurlies > 0) && (!escaping))
					sb.append('|');
				else if (escaping)
					sb.append("\\,");
				else
					sb.append(",");
				break;
			default:
				escaping = false;
				sb.append(currentChar);
			}
		}

		if (!matchAll) {
			sb.append(".*");
		}
		return sb.toString();
	}

	public static int getIndexInPathArray(String[] paths, String path) {
		if ((path == null) || (paths == null) || (paths.length == 0)) {
			return -1;
		}
		int pos = -1;
		int numMatches = 0;

		for (int i = paths.length - 1; i >= 0; i--) {
			if (path.matches(convertGlobToRegEx(paths[i], false))) {
				pos = i;
				numMatches++;
			}

		}

		if ((numMatches == 1) || (pos == -1)) {
			return pos;
		}

		int newPos = -1;
		for (int i = paths.length - 1; i >= 0; i--) {
			if (!path
					.matches(convertGlobToRegEx(
							new StringBuilder().append(paths[i]).append("/")
									.toString(), false)))
				continue;
			newPos = i;
		}

		if (newPos != -1) {
			return newPos;
		}
		return pos;
	}

	public static String getFormattedDuration(long duration) {
		long sec = Math.round((float) duration / MS_IN_SEC);
		String result;
		String result;
		if (duration < MS_IN_SEC) {
			result = String.format("%d ms",
					new Object[] { Long.valueOf(duration) });
		} else {
			String result;
			if (sec < SEC_IN_MIN) {
				result = String.format(
						"%d sec %d ms",
						new Object[] { Long.valueOf(sec),
								Long.valueOf(duration % MS_IN_SEC) });
			} else {
				String result;
				if (sec < SEC_IN_HR) {
					result = String.format(
							"%d min %d sec",
							new Object[] { Long.valueOf(sec / SEC_IN_MIN),
									Long.valueOf(sec % SEC_IN_MIN) });
				} else {
					result = String.format(
							"%d hr %d min %d sec",
							new Object[] { Long.valueOf(sec / SEC_IN_HR),
									Long.valueOf(sec / MIN_IN_HR % SEC_IN_MIN),
									Long.valueOf(sec % SEC_IN_MIN) });
				}
			}
		}
		return result;
	}

	public static String getFormattedSize(long bytes) {
		String result;
		String result;
		if (bytes < 1024L) {
			result = String.format("%d Bytes",
					new Object[] { Long.valueOf(bytes) });
		} else {
			String result;
			if (bytes < 1048576L) {
				result = String.format("%.2f KB",
						new Object[] { Double.valueOf(bytes / 1024.0D) });
			} else {
				String result;
				if (bytes < 1073741824L)
					result = String
							.format("%.2f MB", new Object[] { Double
									.valueOf(bytes / 1048576.0D) });
				else
					result = String.format("%.2f GB", new Object[] { Double
							.valueOf(bytes / 1073741824.0D) });
			}
		}
		return result;
	}

	public static boolean hasCompressionExtension(String fileName) {
		return (fileName.endsWith(".deflate")) || (fileName.endsWith(".gz"))
				|| (fileName.endsWith(".tgz")) || (fileName.endsWith(".bz"))
				|| (fileName.endsWith(".bz2")) || (fileName.endsWith(".zip"))
				|| (fileName.endsWith(".lzo")) || (fileName.endsWith(".rar"));
	}

	public static boolean hasNonSplittableComprExtension(String fileName) {
		return (fileName.endsWith(".deflate")) || (fileName.endsWith(".gz"))
				|| (fileName.endsWith(".tgz")) || (fileName.endsWith(".zip"))
				|| (fileName.endsWith(".rar"));
	}

	public static boolean hasSplittableComprExtension(String fileName) {
		return (fileName.endsWith(".bz")) || (fileName.endsWith(".bz2"))
				|| (fileName.endsWith(".lzo"));
	}

	public static String join(CharSequence separator, Iterable<String> strings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;

		for (String s : strings) {
			if (first)
				first = false;
			else {
				sb.append(separator);
			}
			sb.append(s);
		}

		return sb.toString();
	}

	public static String normalizePath(String path) {
		String output = path.trim();

		int index = output.indexOf("://");
		if (index > 0) {
			index = output.indexOf(47, index + 3);
			output = output.substring(index);
		}

		if (!output.startsWith("/")) {
			output = new StringBuilder()
					.append(System.getProperty("user.home")).append("/")
					.append(output).toString();
		}

		if ((output.endsWith("/")) && (output.length() > 1)) {
			output = output.substring(0, output.length() - 1);
		}
		return output;
	}

	public static void unionPathsWithOrder(List<String> union, String[] paths) {
		for (String path : paths) {
			path = normalizePath(path);
			if (!union.contains(path))
				union.add(path);
		}
	}
}