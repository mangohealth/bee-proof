# bee-proof
Tool for simulating and verifying EMR hive clusters locally.

## What? Why?
Because launching and running a non-persistent EMR cluster takes a while only to find out hours later that you had a typo in one of your queries.  This is very frustrating and annoying for no good reason!

## How would you even do that?
There is a java wrapper for the hive calls which will make repeated calls to the basic Hive CliDriver in a way very similar to how EMR will execute successive Hive script steps.  The wrapper will also keep consistent, sensible defaults for Hive configuration in a local, throw-away environment.  For instance, the derby metastore, log files, and warehouse directories will all get mapped to `./bee-proof-tmp/` relative to the directory where you're executing bee-proof.

In addition to the above, there is also a provided ruby gem wrapper to the java wrapper which makes it easy to call the fully wired java app without having to worry about how to build the classpath for the different supports versions of Hive / Hadoop that the different EMR releases support or how to construct the JSON manifest interface for the java wrapper.

## Different EMR releases?  Why would I ever want to do that?
For one, this is great if you want to test all your scripts against the particular Hive / Hadoop versions for a new EMR release without actually requesting a full cluster.  This saves time, money, and frustration which is nice.  You can then only launch a full cluster once you have fixed all the little bits and pieces that have changed throughout all your scripts in order to see what else may have broken at full scale.

## What EMR releases are supported? How do you specify which to use?
The currently supported releases are:

1. Release 3.?
2. Release 4

We may remove support for older releases in the future so as to avoid building a massive collection of JAR files in the repo.  If you have a need to test on an older EMR release, then please check out an older version of the repo.

If you're using this as a java library, then it's the first argument to the ManifestRunner class specifies which release to use.  If you're using the gem, then you would create you would inialize your verification object via a call like:  `BeeProof::Verification.for_emr_XXX` where XXX is the version of the EMR release you want to use (e.g., 3 or 4).

## Wait, *why* is there a massive number of JAR files in the repo in the first place?
Unfortunately there isn't any great (that we can find) way to resolve maven dependecies from ruby.  To run this as a gem, we must already require that you either have the java process either on your path or defined via JAVA_HOME.  If we use maven itself to build the dependencies you need, then we must also require you to have maven installed along with having a very complicated, error prone install process for the gem.  This means we would either have to additionally package maven itself into the library (boo!) or we (for now) package the full java builds for each release.

Therefore, if we find a better way to package this, then we'll do it!  Please send suggestions, we'd love to hear from you.

## Does it support spark-sql too?
Unfortunately, not yet!  But there's no great reason it couldn't be added in the future since it can be backed by CliDriver as well.

## If you're so smart, then what does this JSON manifest look like?
It looks like this!

```
{
    "enableHadoop": <boolean>,
    "quietOutput": <boolean>,
    "tasks": [
      {
          "script": "<local-path-to-HQL-script>",
          "variables": {
              "<variable-name>": "<value-as-string>"
          }
      }
    ]
}
```

1. **enableHadoop**: Defaults to false.  If true, then hadoop tasks will be blocked to the best of our ability such that syntax and metastore validations happen as quickly as possible.  This is great if you're trying to test that your scripts will run without having a good local data set as you won't have to launch an entire cluster just to find out hours later that it failed.<br/><br/>
2. **quietOutput**: Defaults to false.  If true, then queries will not be echoed to stdout but any error messages will be.<br/><br/>
3. **tasks**:  This is an array of objects that detail which scripts should get run and in what order.
  4. **script**:  Specifies a local path to an HQL script file that should be executed.  For now, this must be local (versus S3, HTTP, or FTP for instance) so any calling systems must manage pulling this down and wiring in the proper task orders.<br/><br/>
  5. **variables**:  This is a map of string key to string value pairs that get mapped to -d command line options when EMR makes the hive script call to execute your script.  So, for instance, if you were to say `{"script":"/tmp/taste_test.hql", "variables": {"BANANA":"'Delicious'"}}` and your script was `SELECT ${BANANA} AS Banana;` then you could expect the execution of the script to return the result set of the single value 'Delicious'.<br/><br/>

## What would a usage of the gem look like?
Using bundler, you can add a dependecy on bee-proof via a line like:

```ruby
gem 'bee-proof', :github => 'mangohealth/bee-proof'
```

Before running any ruby code using bee-proof, you must be sure that there is either java process on the PATH for the shell you're executing in or a JAVA_HOME environment variable defined.  The JAVA_HOME variable will take higher precedence than a java process on the path.

Given that, you can then run a verification like so:

```ruby
require 'bee-proof'
verification = BeeProof::Verification.for_emr_4
verification.add_task('<path-to-script>', 'i_am_a_hive_variable' => 'I have a value!')
verification.run
```

The run call will execute all your requested scripts in order and will either halt with a RuntimeError if an error occurs or end with a line like `> Total time:  XXXXXms` where XXXXX is the total amount of time in milliseconds the verification took.  If an error occurs, then you can scan back through STDOUT for any messages from hive describing why there was a problem.  Note too that if you copy the statement echoed to STDOUT right before the error message to a text file, then the text address in the error message will be accurate (e.g., 56:5 means the error was found on line 56 at character position 5).
