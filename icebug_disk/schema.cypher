CREATE NODE TABLE user(id INT32, PRIMARY KEY(id)) WITH (storage = 'icebug-disk:__STORAGE_PATH__');
CREATE REL TABLE follows(FROM user TO user) WITH (storage = 'icebug-disk:__STORAGE_PATH__');
INSTALL algo;
LOAD EXTENSION algo;
CALL project_graph('lj', ['user'], ['follows']);
