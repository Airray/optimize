package edu.duke.starfish.profile.utils;

import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRMapProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRReduceProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCostFactors;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRStatistics;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLProfileParser extends XMLBaseParser<MRJobProfile> {
	private static final String JOB_PROFILE = "job_profile";
	private static final String MAP_PROFILE = "map_profile";
	private static final String REDUCE_PROFILE = "reduce_profile";
	private static final String INPUTS = "inputs";
	private static final String INPUT = "input";
	private static final String COUNTERS = "counters";
	private static final String AUX_COUNTERS = "aux_counters";
	private static final String STATS = "statistics";
	private static final String FACTORS = "cost_factors";
	private static final String TIMINGS = "timings";
	private static final String AUX_COUNTER = "aux_counter";
	private static final String COUNTER = "counter";
	private static final String STAT = "statistic";
	private static final String FACTOR = "cost_factor";
	private static final String TIMING = "timing";
	private static final String ID = "id";
	private static final String CLUSTER_NAME = "cluster_name";
	private static final String NUM_MAPPERS = "num_mappers";
	private static final String NUM_REDUCERS = "num_reducers";
	private static final String INPUT_INDEX = "input_index";
	private static final String NUM_TASKS = "num_tasks";
	private static final String KEY = "key";
	private static final String VALUE = "value";

	protected MRJobProfile importXML(Document doc) {
		Element root = doc.getDocumentElement();
		if (!"job_profile".equals(root.getTagName())) {
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <job_profile>");
		}

		MRJobProfile jobProfile = new MRJobProfile(root.getAttribute("id"));
		jobProfile.addCounter(MRCounter.MAP_TASKS,
				Long.valueOf(Long.parseLong(root.getAttribute("num_mappers"))));

		jobProfile
				.addCounter(MRCounter.REDUCE_TASKS, Long.valueOf(Long
						.parseLong(root.getAttribute("num_reducers"))));

		String clusterName = root.getAttribute("cluster_name");
		if ((clusterName != null) && (!clusterName.equals(""))) {
			jobProfile.setClusterName(clusterName);
		}

		NodeList inputs = root.getElementsByTagName("inputs").item(0)
				.getChildNodes();

		ArrayList inputList = new ArrayList(1);
		for (int i = 0; i < inputs.getLength(); i++) {
			if ((inputs.item(i) instanceof Element)) {
				inputList.add(inputs.item(i).getTextContent());
			}
		}
		jobProfile.setJobInputs((String[]) inputList.toArray(new String[0]));

		NodeList maps = root.getElementsByTagName("map_profile");
		for (int i = 0; i < maps.getLength(); i++) {
			if ((maps.item(i) instanceof Element)) {
				Element map = (Element) maps.item(i);

				MRMapProfile mapProf = new MRMapProfile(map.getAttribute("id"));
				mapProf.setInputIndex(Integer.parseInt(map
						.getAttribute("input_index")));

				mapProf.setNumTasks(Integer.parseInt(map
						.getAttribute("num_tasks")));

				loadTaskProfileCounters(mapProf, map);
				loadTaskProfileStatistics(mapProf, map);
				loadTaskProfileCostFactors(mapProf, map);
				loadTaskProfileTimings(mapProf, map);
				loadTaskProfileAuxCounters(mapProf, map);

				jobProfile.addMapProfile(mapProf);
			}

		}

		NodeList reducers = root.getElementsByTagName("reduce_profile");
		for (int i = 0; i < reducers.getLength(); i++) {
			if ((reducers.item(i) instanceof Element)) {
				Element reducer = (Element) reducers.item(i);

				MRReduceProfile redProf = new MRReduceProfile(
						reducer.getAttribute("id"));

				redProf.setNumTasks(Integer.parseInt(reducer
						.getAttribute("num_tasks")));

				loadTaskProfileCounters(redProf, reducer);
				loadTaskProfileStatistics(redProf, reducer);
				loadTaskProfileCostFactors(redProf, reducer);
				loadTaskProfileTimings(redProf, reducer);
				loadTaskProfileAuxCounters(redProf, reducer);

				jobProfile.addReduceProfile(redProf);
			}

		}

		jobProfile.updateProfile();
		return jobProfile;
	}

	protected void exportXML(MRJobProfile jobProfile, Document doc) {
		Element job = doc.createElement("job_profile");
		doc.appendChild(job);

		job.setAttribute("id", jobProfile.getJobId());
		job.setAttribute("num_mappers",
				jobProfile.getCounter(MRCounter.MAP_TASKS, Long.valueOf(0L))
						.toString());

		job.setAttribute("num_reducers",
				jobProfile.getCounter(MRCounter.REDUCE_TASKS, Long.valueOf(0L))
						.toString());

		if (jobProfile.getClusterName() != null) {
			job.setAttribute("cluster_name", jobProfile.getClusterName());
		}

		Element inputs = doc.createElement("inputs");
		job.appendChild(inputs);
		for (String jobInput : jobProfile.getJobInputs()) {
			Element input = doc.createElement("input");
			inputs.appendChild(input);
			input.appendChild(doc.createTextNode(jobInput));
		}

		for (MRMapProfile mapProfile : jobProfile.getAvgMapProfiles()) {
			Element map = buildTaskProfileElement(mapProfile, doc,
					"map_profile");
			map.setAttribute("input_index",
					Integer.toString(mapProfile.getInputIndex()));

			job.appendChild(map);
		}

		MRReduceProfile redProfile = jobProfile.getAvgReduceProfile();
		if (!redProfile.isEmpty()) {
			Element reducer = buildTaskProfileElement(redProfile, doc,
					"reduce_profile");

			job.appendChild(reducer);
		}
	}

	private Element buildEnumMapElement(Map<? extends Enum<?>, ?> map,
			Document doc, String parentName, String childName) {
		Element counters = doc.createElement(parentName);
		for (Map.Entry e : map.entrySet()) {
			Element counter = doc.createElement(childName);
			counters.appendChild(counter);

			counter.setAttribute("key", ((Enum) e.getKey()).name());
			counter.setAttribute("value", e.getValue().toString());
		}

		return counters;
	}

	private Element buildTaskProfileElement(MRTaskProfile taskProfile,
			Document doc, String name) {
		Element task = doc.createElement(name);
		task.setAttribute("id", taskProfile.getTaskId());
		task.setAttribute("num_tasks",
				Integer.toString(taskProfile.getNumTasks()));

		task.appendChild(buildEnumMapElement(taskProfile.getCounters(), doc,
				"counters", "counter"));

		task.appendChild(buildEnumMapElement(taskProfile.getStatistics(), doc,
				"statistics", "statistic"));

		task.appendChild(buildEnumMapElement(taskProfile.getCostFactors(), doc,
				"cost_factors", "cost_factor"));

		task.appendChild(buildEnumMapElement(taskProfile.getTimings(), doc,
				"timings", "timing"));

		if (taskProfile.containsAuxCounters()) {
			Element counters = doc.createElement("aux_counters");
			for (Map.Entry e : taskProfile.getAuxCounters().entrySet()) {
				Element counter = doc.createElement("aux_counter");
				counters.appendChild(counter);
				counter.setAttribute("key", (String) e.getKey());
				counter.setAttribute("value", ((Long) e.getValue()).toString());
			}
			task.appendChild(counters);
		}

		return task;
	}

	private void loadTaskProfileCounters(MRTaskProfile taskProf, Element task) {
		Element counters = (Element) task.getElementsByTagName("counters")
				.item(0);

		NodeList counterList = counters.getElementsByTagName("counter");
		for (int j = 0; j < counterList.getLength(); j++) {
			Element counter = (Element) counterList.item(j);
			taskProf.addCounter(MRCounter.valueOf(counter.getAttribute("key")),
					Long.valueOf(Long.parseLong(counter.getAttribute("value"))));
		}
	}

	private void loadTaskProfileStatistics(MRTaskProfile taskProf, Element task) {
		Element stats = (Element) task.getElementsByTagName("statistics").item(
				0);
		NodeList statList = stats.getElementsByTagName("statistic");
		for (int j = 0; j < statList.getLength(); j++) {
			Element stat = (Element) statList.item(j);
			taskProf.addStatistic(
					MRStatistics.valueOf(stat.getAttribute("key")), Double
							.valueOf(Double.parseDouble(stat
									.getAttribute("value"))));
		}
	}

	private void loadTaskProfileCostFactors(MRTaskProfile taskProf, Element task) {
		Element factors = (Element) task.getElementsByTagName("cost_factors")
				.item(0);
		NodeList factorList = factors.getElementsByTagName("cost_factor");
		for (int j = 0; j < factorList.getLength(); j++) {
			Element factor = (Element) factorList.item(j);
			taskProf.addCostFactor(MRCostFactors.valueOf(factor
					.getAttribute("key")), Double.valueOf(Double
					.parseDouble(factor.getAttribute("value"))));
		}
	}

	private void loadTaskProfileTimings(MRTaskProfile taskProf, Element task) {
		Element timings = (Element) task.getElementsByTagName("timings")
				.item(0);
		NodeList timingList = timings.getElementsByTagName("timing");
		for (int j = 0; j < timingList.getLength(); j++) {
			Element timing = (Element) timingList.item(j);
			taskProf.addTiming(MRTaskPhase.valueOf(timing.getAttribute("key")),
					Double.valueOf(Double.parseDouble(timing
							.getAttribute("value"))));
		}
	}

	private void loadTaskProfileAuxCounters(MRTaskProfile taskProf, Element task) {
		Element counters = (Element) task.getElementsByTagName("aux_counters")
				.item(0);

		if (counters != null) {
			NodeList counterList = counters.getElementsByTagName("aux_counter");
			for (int j = 0; j < counterList.getLength(); j++) {
				Element counter = (Element) counterList.item(j);
				taskProf.addAuxCounter(counter.getAttribute("key"), Long
						.valueOf(Long.parseLong(counter.getAttribute("value"))));
			}
		}
	}
}