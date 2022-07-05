create table tests.test_data(
                          id bigint,
                          data text,
                          count int,
                          dataset frozen<set<int>>,
                          PRIMARY KEY (id)
);

insert into tests.test_data (id, data, count, dataset) values (0, null, null, null);
insert into tests.test_data (id, data, count, dataset) values (1, 'one', 10, {});
insert into tests.test_data (id, data, count, dataset) values (2, 'two', 20, {201});
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

create table tests.reads_type_test(
                                id bigint,
                                data text,
                                count int,
                                flag boolean,
                                dataset frozen<set<int>>,
                                datalist frozen<list<int>>,
                                datamap frozen<map<int, text>>,
                                PRIMARY KEY (id)
);

insert into tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
  values (0, 'zero', null, null, null, null, null);

insert into tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
  values (1, 'one', null, null, {}, [], {});

insert into tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
  values (2, 'two', 20, false, {200}, [210], {220: '2_zero'});

insert into tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
  values (3, 'three', 30, true, {300, 301, 302}, [310, 311, 312], {320: '3_zero', 321: '3_one'});

insert into tests.reads_type_test (id, data)
  values (4, 'four');

create table tests.reads_default_name_test(
                                            id bigint,
                                            allupper text,
                                            alllower text,
                                            someName text,
                                            someLongName text,
                                            PRIMARY KEY (id)
);

insert into tests.reads_default_name_test (id, allupper, alllower, someName, someLongName) values (0, 'ALL-UPPER', 'all-lower', 'some-name', 'some-long-name');

create table tests.reads_snake_name_test(
                                id bigint,
                                allupper text,
                                alllower text,
                                some_name text,
                                some_long_name text,
                                PRIMARY KEY (id)
);

insert into tests.reads_snake_name_test (id, allupper, alllower, some_name, some_long_name) values (0, 'ALL-UPPER', 'all-lower', 'some-name', 'some-long-name');

create type tests.udt_for_udt_reads_type_test(
    data text,
    count int,
    flag boolean,
    dataset frozen<set<int>>,
    datalist frozen<list<int>>,
    datamap frozen<map<int, text>>
);

create table tests.udt_reads_type_test(
                                    id bigint,
                                    udt frozen<udt_for_udt_reads_type_test>,
                                    PRIMARY KEY (id)
);

insert into tests.udt_reads_type_test (id, udt)
values (0, {data: 'zero', count: null, flag: null, dataset: null, datalist: null, datamap: null});

insert into tests.udt_reads_type_test (id, udt)
values (1, {data: 'one', count: null, flag: null, dataset: {}, datalist: [], datamap: {}});

insert into tests.udt_reads_type_test (id, udt)
values (2, {data: 'two', count: 20, flag: false, dataset: {200}, datalist: [210], datamap: {220: '2_zero'}});

insert into tests.udt_reads_type_test (id, udt)
values (3, {data: 'three', count: 30, flag: true, dataset: {300, 301, 302}, datalist: [310, 311, 312], datamap: {320: '3_zero', 321: '3_one'}});

insert into tests.udt_reads_type_test (id, udt)
values (4, {data: 'four'});

create type tests.udt_for_udt_reads_default_name_test(
    allupper text,
    alllower text,
    someName text,
    someLongName text
);

create table tests.udt_reads_default_name_test(
                                            id bigint,
                                            udt udt_for_udt_reads_default_name_test,
                                            PRIMARY KEY (id)
);

insert into tests.udt_reads_default_name_test (id, udt)
values (0, {allupper: 'ALL-UPPER', alllower: 'all-lower', someName: 'some-name', someLongName: 'some-long-name'});

create type tests.udt_for_udt_reads_snake_name_test(
    allupper text,
    alllower text,
    some_name text,
    some_long_name text
);

create table tests.udt_reads_snake_name_test(
                                          id bigint,
                                          udt udt_for_udt_reads_snake_name_test,
                                          PRIMARY KEY (id)
);

insert into tests.udt_reads_snake_name_test (id, udt)
values (0, {allupper: 'ALL-UPPER', alllower: 'all-lower', some_name: 'some-name', some_long_name: 'some-long-name'});

create table tests.nullable_collection_tests
(
    id           int,
    regular_list list<int>,
    frozen_list  frozen<list<int>>,
    PRIMARY KEY (id)
);

insert into tests.nullable_collection_tests (id)
values (0);
insert into tests.nullable_collection_tests (id, regular_list, frozen_list)
values (0, null, null);
insert into tests.nullable_collection_tests (id, regular_list, frozen_list)
values (0, [], []);
