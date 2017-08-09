package beeproof;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;


/*
    TODO These things:
      1) Bring in some realistically shaped data to run through spark verifier to see if I can replicate / debug EMR crashes
      2) Split this into 3 jars / sub-projects/modules:  common-stuff, bee-proof-hive, and bee-proof-spark
         Need to do this because spark's embedded Hive classes use the same exact namespace as Hive and
         the versions of both are SUPER different.
      3) Patch rb entry script to switch between jars depending on context
      4) Patch rb entry script to dynamically load different pom dependencies depending on context
      5) Clean up and polish this big change!
 */


/**
 * Main entry point for running EMR-like task manifests.
 */
public class SparkManifestRunner {

    private static final Set emrConfigs = Sets.newHashSet(
            "hive.optimize.s3.query"
    );

    private final PrintStream output;
    private final String manifestFilePath;
    private FakeEmrManifest manifest;
    private SparkSession spark;

    public SparkManifestRunner(String manifestFilePath, PrintStream output) {
        this.output = output;
        this.manifestFilePath = manifestFilePath;
    }

    public void run() {
        initLog4j();

        FakeEmrManifest manifest = getManifest();

        // Load any jars that the manifest requested be in the system classpath
        for(String auxJarPath : manifest.getAuxJarPaths()) {
            loadJar(auxJarPath);
        }

        for(FakeEmrManifest.Task task : manifest.getTasks()) {
            executeSparkSqlScript(task.getScriptPath());
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
    // TODO Move to a parent class
    protected void initLog4j() {
        ConsoleAppender console = new ConsoleAppender();
        console.setLayout(new PatternLayout("%d [%p|%c|%C{1}] %m%n"));
        console.setThreshold(Level.FATAL);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);
    }

    /**
     * Super hack to force main classloader to consider new jars at runtime.  Referenced from here:
     *   https://stackoverflow.com/questions/60764/how-should-i-load-jars-dynamically-at-runtime
     *
     * Note that this is SUPER wonky and might not work in the future if Oracle patches this, but
     * it works so ridiculously conveniently right now that I'm going to stick with it.  Because
     * this is a kind of quality toolset, breaking the normal rules a bit seems ok!
     */
    // TODO Move to parent class?
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

    /**
     * See:  https://spark.apache.org/docs/latest/sql-programming-guide.html
     */
    protected void setupSpark() {
        if(spark == null) {
            String workBaseDir = Paths.get("").toAbsolutePath().toString();
            String workingDir = Paths.get(workBaseDir, "bee-proof-tmp").toString();

            try {
                FileUtils.deleteDirectory(new File(workingDir));
                FileUtils.forceMkdir(new File(workingDir));
            } catch(IOException ex) {
                throw new RuntimeException("Could not re-build working dir", ex);
            }

            this.spark = SparkSession
                    .builder()
                    .appName("Local test")
                    .master("local")
                    .config("spark.sql.warehouse.dir", Paths.get(workingDir, "warehouse").toString())
                    .config("javax.jdo.option.ConnectionURL", "jdbc:derby:" + Paths.get(workingDir, "metastore_db").toString() + ";create=true")
                    .enableHiveSupport()
                    .getOrCreate();
        }
    }

    protected void executeSparkSqlScript(String scriptPath) {
        setupSpark();

        output.println(">>>>>>>> Processing:  " + scriptPath);
        long startTime = System.currentTimeMillis();
        String rawFile = null;
        try {
            rawFile = new String(Files.readAllBytes(Paths.get(scriptPath)));
        } catch(IOException ex) {
            throw new RuntimeException("Failed to find script:  " + scriptPath, ex);
        }

        for(String statement : rawFile.split("[ \t\r\n]*;[ \t\n]*")) {
            if(!statement.isEmpty()) {
                System.out.println();
                System.out.println(">>> EXECUTING:");
                System.out.println(statement);
                System.out.println();
                spark.sql(statement);
            }
        }
        output.println();
        output.println(">>>>>>>> Script took:  " + (System.currentTimeMillis() - startTime) + "ms");
        output.println();
    }

    // TODO Move to parent class?  Is this possible to have a shared public main?
    public static void main(String[] args) {
        if(args.length < 1) {
            throw new RuntimeException("You must provide a CLI param with a path to the manifest file!");
        }

        long startTime = System.currentTimeMillis();

        try {
            new SparkManifestRunner(args[0], new PrintStream(System.out, true, "UTF-8")).run();
        }
        catch(UnsupportedEncodingException ex) {
            throw new RuntimeException("Could not create SparkManifestRunner", ex);
        }

        System.out.println("> Total time:  " + (System.currentTimeMillis() - startTime) + "ms");
    }

}
