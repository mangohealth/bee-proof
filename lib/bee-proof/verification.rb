require 'json'
require_relative 'task'

module BeeProof
  class Verification

    private_class_method :new
  
    attr_reader :emr_release, :tasks
    attr_accessor :enable_hadoop, :verbose_output, :debug_output

    def self.for_release(emr_release)
      new(emr_release)
    end

    def initialize(emr_release)
      @emr_release = emr_release
      @tasks = []
      @enable_hadoop = false
      @verbose_output = false
      @debug_output = false
    end
    
    def add_task(script_path, variables={})
      @tasks << Task.new(script_path, variables)
    end
    
    def manifest_contents
      {
          'enableHadoop' => enable_hadoop,
          'verboseOutput' => verbose_output,
          'debugOutput' => debug_output,
          'tasks' => tasks.map(&:json_hash)
      }.to_json
    end
    
    def run
      manifest_path = '/tmp/bee-proof-manifest.json'
      File.write(manifest_path, manifest_contents)
      BeeProof.run_for_release(emr_release, manifest_path)
    end
    
  end
end