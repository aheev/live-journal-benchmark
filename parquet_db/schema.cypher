CREATE NODE TABLE user(id INT32, PRIMARY KEY(id)) WITH (storage = '__STORAGE_PATH__');
CREATE REL TABLE follows(FROM user TO user) WITH (storage = '__STORAGE_PATH__');
LOAD EXTENSION './libalgo.lbug_extension';
CALL project_graph('lj', ['user'], ['follows']);
