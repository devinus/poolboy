version = String.strip(File.read!("VERSION"))

Expm.Package.new(
  name: "poolboy",
  description: "A hunky Erlang worker pool factory",
  homepage: "http://devintorr.es/poolboy",
  version: version,
  keywords: %w(Erlang library pool pools pooler),
  maintainers: [[name: "Devin Torres", email: "devin@devintorr.es"],
                [name: "Andrew Thompson", email: "andrew@hijacked.us"]],
  repositories: [[github: "devinus/poolboy", tag: version]]
)
