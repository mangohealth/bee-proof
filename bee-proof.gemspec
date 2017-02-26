Gem::Specification.new do |s|
  s.name        = 'bee-proof'
  s.version     = '1.0.0'
  s.date        = '2015-08-14'
  s.summary     = "Bee-proof"
  s.description = "Tool for simulating and verifying EMR hive clusters locally"
  s.authors     = ["David Rom"]
  s.email       = 'drom@mangohealth.com'
  s.files       = Dir["lib/**/*"]
  s.homepage    =
    'https://github.com/mangohealth/bee-proof'
  s.license       = 'MIT'
  s.add_dependency('naether', '0.15.0')
end
