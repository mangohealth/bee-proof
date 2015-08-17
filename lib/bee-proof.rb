module BeeProof

  BASE_DIR = File.dirname(__FILE__)

  # Note that,for now, this is linux/bsd specific
  def self.run_for_release(emr_release, *params)
    cmd = 
        [
            java_bin_path,
            '-Dfile.encoding=UTF-8',
            '-classpath',
            [ 
                '"',
                [
                  jar_file_for_release(emr_release),
                  Dir["#{BASE_DIR}/#{emr_release}-deps/*.jar"],
                  Dir["#{BASE_DIR}/emr-common-deps/*.jar"]
                ].flatten.join(':'),
                '"'
            ].join(''),
            'org.mangohealth.beeproof.ManifestRunner',
            *params
        ].join(' ')
    system(cmd)
  end
  
  def self.java_bin_path
    @@java_bin_path ||= [
        java_home_bin,
        java_path_bin
    ].compact.first
  end

  def self.jar_file_for_release(emr_release)
    File.expand_path("bee-proof-1.0.0-#{emr_release}.jar", BASE_DIR)
  end
  
private
  
  def self.java_home_bin
    if ENV['JAVA_HOME']
      ret_val = File.join(ENV['JAVA_HOME'], 'bin', 'java')
      ret_val if File.exists?(ret_val)
    end
  end
  
  def self.java_path_bin
    require 'mkmf'
    
    # Turn off mkmf logging so we don't pollute these nice people's working directory
    ::MakeMakefile::Logging.instance_variable_set(:@logfile, File::NULL)
    ::MakeMakefile::Logging.quiet = true
    
    find_executable 'java'
  end
  
end

unless BeeProof.java_bin_path
  raise 'You must either either have a valid JAVA_HOME set or have the java process on your path!'
end

require_relative 'bee-proof/verification'
