create table tests.test_data(
                          id bigint,
                          data text,
                          PRIMARY KEY (id)
);

insert into tests.test_data (id, data) values (0, null);
insert into tests.test_data (id, data) values (1, 'one');
insert into tests.test_data (id, data) values (2, 'two');
insert into tests.test_data (id, data) values (3, 'three');

create table tests.test_data_multiple_keys(
                                        id1 bigint,
                                        id2 int,
                                        data text,
                                        PRIMARY KEY (id1, id2)
);

insert into tests.test_data_multiple_keys (id1, id2, data) values (1, 2, 'one-two');

create type tests.basic_info(
    weight double,
    height text,
    datapoints frozen<set<int>>
);

create table tests.person_attributes(
                                  person_id int,
                                  info frozen<basic_info>,
                                  PRIMARY KEY (person_id)
);

create table tests.test_collection(
                                id int,
                                maptest frozen<map<text, uuid>>,
                                settest frozen<set<int>>,
                                listtest frozen<list<date>>,
                                PRIMARY KEY (id)
);

CREATE TYPE tests.example_type(
    x bigint,
    y bigint,
    date date,
    time time
);

CREATE TYPE tests.example_nested_type(
    a int,
    b text,
    c frozen<example_type>
);

CREATE TYPE tests.example_collection_nested_udt_type(
    a int,
    b frozen<map<int,frozen<set<frozen<set<frozen<set<frozen<set<example_nested_type>>>>>>>>>>
);

CREATE TYPE tests.example_nested_primitive_type(
    a int,
    b frozen<map<int, frozen<set<frozen<set<frozen<set<frozen<set<int>>>>>>>>>>
);

CREATE TABLE tests.heavily_nested_udt_table(
                                         id int,
                                         data example_collection_nested_udt_type,
                                         PRIMARY KEY (id)
);

CREATE TABLE tests.heavily_nested_prim_table(
                                          id int,
                                          data example_nested_primitive_type,
                                          PRIMARY KEY (id)
);
