module BeeProof

  BASE_DIR = File.dirname(__FILE__)

  def self.java_cp_for_release(emr_release)
    (@@java_cp_for_release ||= {})[emr_release] ||= begin
        tmp_pom_path = File.join('/tmp/', "pom-#{emr_release}.xml")

        # Prepare pom file for desired release
        require 'rexml/document'
        base_pom = REXML::Document.new(File.read(File.join(BASE_DIR, '..', 'pom.xml')))
        release_profile = base_pom.root.elements["profiles/profile[id='#{emr_release}']"] ||
            raise("No pom profile for release:  #{emr_release}")
        release_profile.add_element('activation').add_element('activeByDefault').add_text('true')
        File.write(tmp_pom_path, base_pom.to_s)

        # Load maven dependencies for requested release profile
        require 'naether/bootstrap'
        Naether::Bootstrap.bootstrap_local_repo
        naether = Naether.create
        naether.add_pom_dependencies(tmp_pom_path)
        naether.resolve_dependencies

        # The -classpath to add manifest runner calls for this EMR release
        naether.dependencies_classpath
    end
  end

  # Note that,for now, this is linux/bsd specific
  def self.run_for_release(emr_release, *params)
    cmd = 
        [
            "'#{java_bin_path}'",
            '-Dfile.encoding=UTF-8',
            '-classpath',
            [ 
                '"',
                [
                  jar_file_for_release(emr_release),
                  java_cp_for_release(emr_release)
                ].flatten.compact.join(':'),
                '"'
            ].join(''),
            'org.mangohealth.beeproof.ManifestRunner',
            *params
        ].join(' ')

    # Make sure ruby proc fails if java proc fails
    exit 1 unless system(cmd)
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
