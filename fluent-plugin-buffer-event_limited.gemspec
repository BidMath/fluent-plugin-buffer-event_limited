# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-buffer-event_limited"
  spec.version       = "0.1.6"
  spec.authors       = ['Gergo Sulymosi']
  spec.email         = ['gergo.sulymosi@gmail.com']
  spec.description   = %{Fluentd memory buffer plugin with many types of chunk limits}
  spec.summary       = %{Alternative file buffer plugin for Fluentd to limit events in a buffer not it's size}
  spec.homepage      = "https://github.com/trekdemo/fluent-plugin-buffer-event_limited"
  spec.license       = "APLv2"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "pry"
  spec.add_runtime_dependency "fluentd", ">= 0.10.42"
end
