package org.mangohealth.beeproof;

import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class provides a facsimile of
 */
public class FakeEmrManifest {

    class Task {

        private final String scriptPath;
        private final Map<String, String> hiveVariables;

        protected Task(String scriptPath, Map<String, String> hiveVariables) {
            this.scriptPath = scriptPath;
            this.hiveVariables = hiveVariables;
        }

        public String getScriptPath() { return scriptPath; }

        public Map<String, String> getHiveVariables() { return hiveVariables; }
    }

    /**
     * If false, causes all hive script executions to not actually run any map-reduce tasks.  Only DDL and commands,
     * though any other SQL will still be validated.
     */
    private final boolean hadoopEnabled;

    /**
     * Turns on the hive cli's verbose mode which will echo statements to stdout as they execute.
     */
    private final boolean verboseOutput;

    private final List<Task> tasks;

    public FakeEmrManifest(String filePath) throws RuntimeException {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(filePath));
            String rawJson = new String(encoded, "UTF-8");
            JSONObject obj = new JSONObject(rawJson);

            this.hadoopEnabled = obj.getBoolean("enableHadoop");
            this.verboseOutput = !obj.getBoolean("quietOutput");

            this.tasks = new ArrayList<Task>();
            JSONArray manifestTasks = obj.getJSONArray("tasks");
            for(int i = 0; i < manifestTasks.length(); ++i) {
                JSONObject task = manifestTasks.getJSONObject(i);
                String script = task.getString("script");

                JSONObject hiveVariablesRaw = task.getJSONObject("variables");
                Map<String, String> hiveVariables = new HashMap<String, String>();
                Iterator keys = hiveVariablesRaw.keys();
                while(keys.hasNext()) {
                    String key = (String) keys.next();
                    hiveVariables.put(key, hiveVariablesRaw.getString(key));
                }

                tasks.add(new Task(script, hiveVariables));
            }
        }
        catch(Exception ex) {
            throw new RuntimeException("Could not load fake EMR manifest file", ex);
        }
    }

    public boolean isHadoopEnabled() { return hadoopEnabled; }

    public boolean isVerboseOutput() { return verboseOutput; }

    public List<Task> getTasks() { return tasks; }

}
