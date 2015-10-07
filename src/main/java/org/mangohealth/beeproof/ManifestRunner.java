package org.mangohealth.beeproof;

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
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

/**
 * Main entry point for running EMR-like task manifests.
 */
public class ManifestRunner {

    private static final Set emrConfigs = Sets.newHashSet(
            "hive.optimize.s3.query"
    );

    private final PrintStream output;
    private final String manifestFilePath;
    private FakeEmrManifest manifest;

    public ManifestRunner(String manifestFilePath, PrintStream output) {
        this.output = output;
        this.manifestFilePath = manifestFilePath;
    }

    public void run() {
        initLog4j();

        FakeEmrManifest manifest = getManifest();

        if(!manifest.isHadoopEnabled()) {
            blockMapReduceTasks();
        }

        patchCliLogging();
        patchForEmrHiveConf();
        initializeHiveSession();

        for(FakeEmrManifest.Task task : manifest.getTasks()) {
            executeScript(task.getScriptPath(), task.getHiveVariables());
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
                        "$1 = org.mangohealth.beeproof.ManifestRunner.emrHiveConfStripper($1); " +
                        "$2 = org.mangohealth.beeproof.ManifestRunner.emrHiveConfStripper($2); " +
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
            String metastoreDir = Paths.get(workingDir,  "metastore_db").toString();
            String warehouseDir = Paths.get(workingDir, "warehouse").toString();
            String derbyLogPath = Paths.get(workingDir, "derby.log").toString();
            FileUtils.deleteDirectory(new File(workingDir));
            FileUtils.forceMkdir(new File(workingDir));

            HiveConf conf = ss.getConf();
                conf.setBoolVar(HiveConf.ConfVars.CLIIGNOREERRORS, false);
                conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:" + metastoreDir + ";create=true");
                conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir);
            System.setProperty("derby.stream.error.file", derbyLogPath);

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

    protected void executeScript(String scriptPath, Map<String, String> hiveVariables) {
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
            new ManifestRunner(args[0], new PrintStream(System.out, true, "UTF-8")).run();
        }
        catch(UnsupportedEncodingException ex) {
            throw new RuntimeException("Could not create ManifestRunner", ex);
        }

        System.out.println("> Total time:  " + (System.currentTimeMillis() - startTime) + "ms");
    }

}
