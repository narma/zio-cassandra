CREATE TABLE tests.test_data
(
    id      bigint,
    data    text,
    count   int,
    dataset frozen<set<int>>,
    PRIMARY KEY (id)
);

INSERT INTO tests.test_data (id, data, count, dataset)
VALUES (0, null, null, null);

INSERT INTO tests.test_data (id, data, count, dataset)
VALUES (1, 'one', 10, {});

INSERT INTO tests.test_data (id, data, count, dataset)
VALUES (2, 'two', 20, {201});

INSERT INTO tests.test_data (id, data)
VALUES (3, 'three');

CREATE TABLE tests.test_data_multiple_keys
(
    id1  bigint,
    id2  int,
    data text,
    PRIMARY KEY (id1, id2)
);

INSERT INTO tests.test_data_multiple_keys (id1, id2, data)
VALUES (1, 2, 'one-two');

CREATE TYPE tests.basic_info(
    weight double,
    height text,
    datapoints frozen<set<int>>
    );

CREATE TABLE tests.person_attributes
(
    person_id int,
    info      frozen<basic_info>,
    PRIMARY KEY (person_id)
);

CREATE TABLE tests.test_collection
(
    id       int,
    maptest  frozen<map<text, uuid>>,
    settest  frozen<set<int>>,
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

CREATE TABLE tests.heavily_nested_udt_table
(
    id   int,
    data example_collection_nested_udt_type,
    PRIMARY KEY (id)
);

CREATE TABLE tests.heavily_nested_prim_table
(
    id   int,
    data example_nested_primitive_type,
    PRIMARY KEY (id)
);

CREATE TABLE tests.nested_udt_table
(
    id   int,
    data example_nested_type,
    PRIMARY KEY (id)
);

CREATE TABLE tests.reads_type_test
(
    id       bigint,
    data     text,
    count    int,
    flag     boolean,
    dataset  frozen<set<int>>,
    datalist frozen<list<int>>,
    datamap  frozen<map<int, text>>,
    PRIMARY KEY (id)
);

INSERT INTO tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
VALUES (0, 'zero', null, null, null, null, null);

INSERT INTO tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
VALUES (1, 'one', null, null, {}, [], {});

INSERT INTO tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
VALUES (2, 'two', 20, false, {200}, [210], {220: '2_zero'});

INSERT INTO tests.reads_type_test (id, data, count, flag, dataset, datalist, datamap)
VALUES (3, 'three', 30, true, {300, 301, 302}, [310, 311, 312], {320: '3_zero', 321: '3_one'});

INSERT INTO tests.reads_type_test (id, data)
VALUES (4, 'four');

CREATE TABLE tests.reads_default_name_test
(
    id           bigint,
    allupper     text,
    alllower     text,
    somename     text,
    somelongname text,
    PRIMARY KEY (id)
);

INSERT INTO tests.reads_default_name_test (id, allupper, alllower, somename, somelongname)
VALUES (0, 'ALL-UPPER', 'all-lower', 'some-name', 'some-long-name');

CREATE TABLE tests.reads_snake_name_test
(
    id             bigint,
    allupper       text,
    alllower       text,
    some_name      text,
    some_long_name text,
    PRIMARY KEY (id)
);

INSERT INTO tests.reads_snake_name_test (id, allupper, alllower, some_name, some_long_name)
VALUES (0, 'ALL-UPPER', 'all-lower', 'some-name', 'some-long-name');

CREATE TYPE tests.udt_for_udt_reads_type_test(
    data text,
    count int,
    flag boolean,
    dataset frozen<set<int>>,
    datalist frozen<list<int>>,
    datamap frozen<map<int, text>>
    );

CREATE TABLE tests.udt_reads_type_test
(
    id  bigint,
    udt frozen<udt_for_udt_reads_type_test>,
    PRIMARY KEY (id)
);

INSERT INTO tests.udt_reads_type_test (id, udt)
VALUES (0, {data: 'zero', count: null, flag: null, dataset: null, datalist: null, datamap: null});

INSERT INTO tests.udt_reads_type_test (id, udt)
VALUES (1, {data: 'one', count: null, flag: null, dataset: {}, datalist: [], datamap: {}});

INSERT INTO tests.udt_reads_type_test (id, udt)
VALUES (2, {data: 'two', count: 20, flag: false, dataset: {200}, datalist: [210], datamap: {220: '2_zero'}});

INSERT INTO tests.udt_reads_type_test (id, udt)
VALUES (3, {data: 'three', count: 30, flag: true, dataset: {300, 301, 302}, datalist: [310, 311, 312], datamap: {320: '3_zero', 321: '3_one'}});

INSERT INTO tests.udt_reads_type_test (id, udt)
VALUES (4, {data: 'four'});

CREATE TYPE tests.udt_for_udt_codec_default_name_test(
    allupper text,
    alllower text,
    somename text,
    somelongname text
    );

CREATE TABLE tests.udt_codec_default_name_test
(
    id  bigint,
    udt udt_for_udt_codec_default_name_test,
    PRIMARY KEY (id)
);

INSERT INTO tests.udt_codec_default_name_test (id, udt)
VALUES (0, {allupper: 'ALL-UPPER', alllower: 'all-lower', somename: 'some-name', somelongname: 'some-long-name'});

CREATE TYPE tests.udt_for_udt_codec_snake_name_test(
    allupper text,
    alllower text,
    some_name text,
    some_long_name text
    );

CREATE TABLE tests.udt_codec_snake_name_test
(
    id  bigint,
    udt udt_for_udt_codec_snake_name_test,
    PRIMARY KEY (id)
);

INSERT INTO tests.udt_codec_snake_name_test (id, udt)
VALUES (0, {allupper: 'ALL-UPPER', alllower: 'all-lower', some_name: 'some-name', some_long_name: 'some-long-name'});

CREATE TABLE tests.nullable_collection_tests
(
    id           int,
    regular_list list<int>,
    frozen_list  frozen<list<int>>,
    PRIMARY KEY (id)
);

INSERT INTO tests.nullable_collection_tests (id)
VALUES (0);
INSERT INTO tests.nullable_collection_tests (id, regular_list, frozen_list)
VALUES (0, null, null);
INSERT INTO tests.nullable_collection_tests (id, regular_list, frozen_list)
VALUES (0, [], []);
