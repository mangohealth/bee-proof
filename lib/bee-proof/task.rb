module BeeProof
  class Task
    
    attr_reader :script_path, :variables
    
    def initialize(script_path, variables={})
      @script_path = script_path
      @variables = variables
    end
    
    def valid?
      File.exists?(script_path)
    end
    
    def json_hash
      {
          'script' => script_path,
          'variables' => variables
      }
    end
    
  end
end