package org.mangohealth.beeproof;

import com.sun.tools.javac.util.List;
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

/**
 * TODO
 */
public class ManifestRunner {

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
        List<String> tasksToBlock = List.of(
                "org.apache.hadoop.hive.ql.exec.mr.MapRedTask",
                "org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask"
        );

        for (String taskToBlock : tasksToBlock) {
            ClassPatchUtil.blockClassMethod(taskToBlock, "execute", "{ return 0; }");
        }
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
