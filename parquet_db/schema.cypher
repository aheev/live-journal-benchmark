CREATE NODE TABLE user(id INT32, PRIMARY KEY(id)) WITH (storage = '/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/parquet_db/lj');
CREATE REL TABLE follows(FROM user TO user) WITH (storage = '/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/parquet_db/lj');
