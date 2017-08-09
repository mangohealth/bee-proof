package beeproof;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;

/**
 * Main entry point for running EMR-like task manifests.
 */
public class HiveManifestRunner {

    private static final Set emrConfigs = Sets.newHashSet(
            "hive.optimize.s3.query"
    );

    private final PrintStream output;
    private final String manifestFilePath;
    private FakeEmrManifest manifest;
    private boolean hiveIsReady = false;

    public HiveManifestRunner(String manifestFilePath, PrintStream output) {
        this.output = output;
        this.manifestFilePath = manifestFilePath;
    }

    public void run() {
        initLog4j();

        FakeEmrManifest manifest = getManifest();

        if(!manifest.isHadoopEnabled()) {
            blockMapReduceTasks();
        }

        // Load any jars that the manifest requested be in the system classpath
        for(String auxJarPath : manifest.getAuxJarPaths()) {
            loadJar(auxJarPath);
        }

        for(FakeEmrManifest.Task task : manifest.getTasks()) {
            executeHiveScript(task.getScriptPath(), task.getHiveVariables());
        }
    }

    /**
     * Lazy manifest initializer
     */
    protected FakeEmrManifest getManifest() {
        if(manifest == null) {
            this.manifest = new FakeEmrManifest(manifestFilePath);
        }

        return manifest;
    }

    /**
     * Force all log4j output to the console so we don't pollute output for the tool
     */
    protected void initLog4j() {
        ConsoleAppender console = new ConsoleAppender();
        console.setLayout(new PatternLayout("%d [%p|%c|%C{1}] %m%n"));
        console.setThreshold(Level.FATAL);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);
    }

    /**
     * Re-builds Hive map reduce jobs on the fly such that no data transforms will actually get executed.  This is
     * good if you only wish to verify HiveQL syntax.
     */
    protected void blockMapReduceTasks() {
        String[] tasksToBlock = {
                "org.apache.hadoop.hive.ql.exec.mr.ExecDriver",
                "org.apache.hadoop.hive.ql.exec.mr.MapRedTask",
                "org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask",
                "org.apache.hadoop.hive.ql.exec.MoveTask",
                "org.apache.hadoop.hive.ql.exec.FetchTask",
                "org.apache.hadoop.hive.ql.exec.CopyTask",
                "org.apache.hadoop.hive.ql.exec.tez.TezTask",
                "org.apache.hadoop.hive.ql.exec.DependencyCollectionTask",
                "org.apache.hadoop.hive.ql.exec.ExplainSQRewriteTask",
                "org.apache.hadoop.hive.ql.exec.StatsTask",
                "org.apache.hadoop.hive.ql.exec.ExplainTask",
                "org.apache.hadoop.hive.ql.exec.ColumnStatsUpdateTask",
                "org.apache.hadoop.hive.ql.exec.ConditionalTask",
                "org.apache.hadoop.hive.ql.exec.StatsNoJobTask",
                "org.apache.hadoop.hive.ql.index.IndexMetadataChangeTask",
                "org.apache.hadoop.hive.ql.io.rcfile.merge.BlockMergeTask",
                "org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateTask",
                "org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanTask",
                "org.apache.hadoop.hive.ql.io.merge.MergeFileTask"
        };

        ClassPatchUtil.prependClassMethod(
                "org.apache.hadoop.hive.ql.exec.Task",
                "executeTask",
                "{ org.apache.hadoop.hive.ql.session.SessionState.get().out.println(\"> Running task:  \" + this.getClass()); }"
        );

        // Force exec parallel to off despite what script says as this will always *massively* slow down execution in
        // these fake, simulated cases
        ClassPatchUtil.prependClassMethod(
                "org.apache.hadoop.hive.ql.Driver",
                "launchTask",
                "{ org.apache.hadoop.hive.conf.HiveConf.setBoolVar(conf, org.apache.hadoop.hive.conf.HiveConf.ConfVars.EXECPARALLEL, false); }"
        );

        for (String taskToBlock : tasksToBlock) {
            try {
                ClassPatchUtil.blockClassMethod(
                        taskToBlock,
                        "execute",
                        "{ org.apache.hadoop.hive.ql.session.SessionState.get().out.println(\"> Skipped!\"); return 0; }"
                );
            }
            catch(Exception ex) {
                output.println("[WARN] Could not block task for this EMR release:  " + taskToBlock);
            }
        }
    }

    public static String emrHiveConfStripper(String name) {
        if(emrConfigs.contains(name.toLowerCase())) {
            return "emrhive." + name.substring(5);
        }

        return name;
    }

    protected void patchForEmrHiveConf() {
        ClassPatchUtil.prependClassMethod(
                "org.apache.hadoop.hive.ql.processors.SetProcessor",
                "setConf",
                "{ " +
                        "$1 = org.mangohealth.beeproof.HiveManifestRunner.emrHiveConfStripper($1); " +
                        "$2 = org.mangohealth.beeproof.HiveManifestRunner.emrHiveConfStripper($2); " +
                "}"
        );
    }

    /**
     * Re-builds the CLI driver that the hive script uses so that logging isn't a complete mess for local mode.
     */
    protected void patchCliLogging() {
        ClassPatchUtil.prependClassMethod(
                "org.apache.hadoop.hive.cli.CliDriver",
                "processCmd",
                "{ $1 = $1.trim(); org.apache.hadoop.hive.ql.session.SessionState.get().out.println(); }"
        );
    }

    /**
     * Imitates CliDriver session startup
     */
    protected void initializeHiveSession() {
        try {
            FakeEmrManifest manifest = getManifest();
            CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
            ss.setIsVerbose(manifest.isVerboseOutput());

            // TODO Make this configurable?  Default to somewhere special in /tmp?
            String workBaseDir = Paths.get("").toAbsolutePath().toString();
            String workingDir = Paths.get(workBaseDir, "bee-proof-tmp").toString();
            FileUtils.deleteDirectory(new File(workingDir));
            FileUtils.forceMkdir(new File(workingDir));

            HiveConf conf = ss.getConf();
                conf.setBoolVar(CLIIGNOREERRORS, false);
                conf.setVar(METASTORECONNECTURLKEY, "jdbc:derby:" + Paths.get(workingDir, "metastore_db").toString() + ";create=true");
                conf.setVar(METASTOREWAREHOUSE, Paths.get(workingDir, "warehouse").toString());
                conf.setVar(SCRATCHDIR, Paths.get(workingDir, "scratch").toString());
                conf.setVar(LOCALSCRATCHDIR, Paths.get(workingDir, "local_scratch").toString());
                conf.setVar(HIVEHISTORYFILELOC, Paths.get(workingDir, "tmp").toString());
                conf.setBoolVar(HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);
                conf.setBoolVar(HIVESTATSAUTOGATHER, false);
                conf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
                conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
                conf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
                conf.setBoolVar(HIVEOPTINDEXFILTER, false);
                conf.setBoolVar(HIVECONVERTJOIN, false);
                conf.setBoolVar(HIVESKEWJOIN, false);
                conf.setLongVar(HIVECOUNTERSPULLINTERVAL, 1L);
                conf.setBoolVar(HIVE_RPC_QUERY_PLAN, true);
                conf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
                conf.setVar(METASTORE_CONNECTION_POOLING_TYPE, "None");
                conf.setBoolVar(METASTORE_AUTO_CREATE_ALL, true);

            System.setProperty("hadoop.tmp.dir", Paths.get(workingDir, "hadoop_tmp").toString());
            System.setProperty("mapred.system.dir", Paths.get(workingDir, "mapred_sys").toString());
            System.setProperty("mapred.local.dir", Paths.get(workingDir, "mapred_local").toString());
            System.setProperty("test.log.dir", Paths.get(workingDir, "test_logs").toString());
            System.setProperty("derby.stream.error.file", Paths.get(workingDir, "derby.log").toString());

            ss.in = System.in;
            ss.out = this.output;
            ss.info = this.output;
            ss.err = this.output;

            SessionState.start(ss);
        }
        catch(Exception ex) {
            throw new RuntimeException("Could not initialize session state", ex);
        }
    }

    /**
     * Super hack to force main classloader to consider new jars at runtime.  Referenced from here:
     *   https://stackoverflow.com/questions/60764/how-should-i-load-jars-dynamically-at-runtime
     *
     * Note that this is SUPER wonky and might not work in the future if Oracle patches this, but
     * it works so ridiculously conveniently right now that I'm going to stick with it.  Because
     * this is a kind of quality toolset, breaking the normal rules a bit seems ok!
     */
    private void loadJar(String path) {
        try {
            File file = new File(path);
            URL url = file.toURI().toURL();
            URLClassLoader classLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, url);
        }
        catch(Exception ex) {
            throw new RuntimeException("Failed to load jar @ " + path, ex);
        }
    }

    protected void setupHive() {
        if(!hiveIsReady) {
            patchCliLogging();
            patchForEmrHiveConf();
            initializeHiveSession();
            this.hiveIsReady = true;
        }
    }

    protected void executeHiveScript(String scriptPath, Map<String, String> hiveVariables) {
        setupHive();
        try {
            CliDriver driver = new CliDriver();
            driver.setHiveVariables(hiveVariables);
            output.println(">>>>>>>> Processing:  " + scriptPath);
            int retVal = driver.processFile(scriptPath);
            output.println();
            output.println();
            if(retVal != 0) {
                throw new Exception("Error returned by CliDriver");
            }
        }
        catch(Exception ex) {
            throw new RuntimeException("Failed to execute script:  " + scriptPath, ex);
        }
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            throw new RuntimeException("You must provide a CLI param with a path to the manifest file!");
        }

        long startTime = System.currentTimeMillis();

        try {
            new HiveManifestRunner(args[0], new PrintStream(System.out, true, "UTF-8")).run();
        }
        catch(UnsupportedEncodingException ex) {
            throw new RuntimeException("Could not create HiveManifestRunner", ex);
        }

        System.out.println("> Total time:  " + (System.currentTimeMillis() - startTime) + "ms");
    }

}
