require 'json'
require_relative 'task'

module BeeProof
  class Verification

    private_class_method :new
  
    attr_reader :emr_release, :tasks
    attr_accessor :enable_hadoop, :quiet_output
  
    def self.for_emr_3
      new('emr-3')
    end

    def self.for_emr_4
      new('emr-4')
    end

    def self.for_emr_5
      new('emr-5')
    end

    def initialize(emr_release)
      @emr_release = emr_release
      @tasks = []
      @enable_hadoop = false
      @quiet_output = false
    end
    
    def add_task(script_path, variables={})
      @tasks << Task.new(script_path, variables)
    end
    
    def manifest_contents
      {
          'enableHadoop' => enable_hadoop,
          'quietOutput' => quiet_output,
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