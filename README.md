OpenTSDB2 is a rewrite of the original OpenTSDB project.

· Implemented new bootstrap as a simple bean to allow deployment into a spring container
· Added new Datastore implementation to use a Spring provided DataSource reference rather than the built in -n-memory H2 instance.
· Added  queuing of data-store writes ( H2 was not liking the volume of writes I was sending when an agent JVM comes on line, so the queuing alleviated that)
· Added simple JMX instrumentation for the DataStore interface instances.

See Documentation for parent projects [at](http://code.google.com/p/opentsdb2/).
