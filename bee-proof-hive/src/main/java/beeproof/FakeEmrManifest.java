package beeproof;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class provides a facsimile of
 */
public class FakeEmrManifest {

    class Task {

        private final String scriptPath;
        private final String type;
        private final Map<String, String> hiveVariables;

        protected Task(String scriptPath, String type, Map<String, String> hiveVariables) {
            this.scriptPath = scriptPath;
            this.type = type;
            this.hiveVariables = hiveVariables;
        }

        public String getScriptPath() { return scriptPath; }

        public String getRawType() { return type; }

        public Map<String, String> getHiveVariables() { return hiveVariables; }

        public boolean isHive() {
            return "hive".equalsIgnoreCase(type);
        }

        public boolean isSpark() {
            return "spark".equalsIgnoreCase(type);
        }
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

    private final List<String> auxJarPaths;
    private final List<Task> tasks;

    public FakeEmrManifest(String filePath) throws RuntimeException {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(filePath));
            String rawJson = new String(encoded, "UTF-8");
            JSONObject obj = new JSONObject(rawJson);

            this.hadoopEnabled = obj.getBoolean("enableHadoop");
            this.verboseOutput = !obj.getBoolean("quietOutput");

            this.auxJarPaths = new ArrayList<>();
            if(obj.has("auxJars")) {
                JSONArray rawAuxJars = obj.getJSONArray("auxJars");
                for(int i = 0; i < rawAuxJars.length(); ++i) {
                    String auxJarPath = rawAuxJars.getString(i);
                    File auxJarFile = new File(auxJarPath);
                    if(!auxJarFile.exists()) {
                        throw new RuntimeException("Could not jar file to load @ " + auxJarPath);
                    }
                    if(!auxJarPath.endsWith(".jar")) {
                        throw new RuntimeException("Should be a list of jars to load, but got:  " + auxJarPath);
                    }
                    auxJarPaths.add(auxJarPath);
                }
            }


            this.tasks = new ArrayList<Task>();
            JSONArray manifestTasks = obj.getJSONArray("tasks");
            for(int i = 0; i < manifestTasks.length(); ++i) {
                JSONObject task = manifestTasks.getJSONObject(i);
                String script = task.getString("script");
                String scriptType = task.getString("type");

                Map<String, String> hiveVariables = new HashMap<String, String>();
                if(task.has("variables")) {
                    JSONObject hiveVariablesRaw = task.getJSONObject("variables");
                    Iterator keys = hiveVariablesRaw.keys();
                    while(keys.hasNext()) {
                        String key = (String) keys.next();
                        hiveVariables.put(key, hiveVariablesRaw.getString(key));
                    }
                }

                tasks.add(new Task(script, scriptType, hiveVariables));
            }
        }
        catch(Exception ex) {
            throw new RuntimeException("Could not load fake EMR manifest file", ex);
        }
    }

    public boolean isHadoopEnabled() { return hadoopEnabled; }

    public boolean isVerboseOutput() { return verboseOutput; }

    public List<String> getAuxJarPaths() { return auxJarPaths; }

    public List<Task> getTasks() { return tasks; }

}
