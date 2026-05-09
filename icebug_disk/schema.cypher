CREATE NODE TABLE user(id INT32, PRIMARY KEY(id)) WITH (storage = 'icebug-disk:/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/icebug_disk');
CREATE REL TABLE follows(FROM user TO user) WITH (storage = 'icebug-disk:/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/icebug_disk');
