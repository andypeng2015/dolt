#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "json: Create table with JSON column" {
    run dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ "$status" -eq 0 ]
}

@test "json: query JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
    [ "${lines[2]}" = '2,"{""b"":2}"' ]

    dolt sql <<SQL
    UPDATE js SET js = '{"c":3}' WHERE pk = 2;
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
    [ "${lines[2]}" = '2,"{""c"":3}"' ]

    dolt sql <<SQL
    DELETE FROM js WHERE pk = 2;
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
}

@test "json: JSON value printing" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL

    run dolt sql -q "SELECT * FROM js;"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '+----+---------+' ]
    [ "${lines[1]}" = '| pk | js      |' ]
    [ "${lines[2]}" = '+----+---------+' ]
    [ "${lines[3]}" = '| 1  | {"a":1} |' ]
    [ "${lines[4]}" = '| 2  | {"b":2} |' ]
    [ "${lines[5]}" = '+----+---------+' ]

    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = 'pk,js' ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
    [ "${lines[2]}" = '2,"{""b"":2}"' ]

    dolt sql -q "SELECT * FROM js;" -r json
    run dolt sql -q "SELECT * FROM js;" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"js":{"a":1},"pk":1},{"js":{"b":2},"pk":2}]}' ]

    dolt sql <<SQL
insert into js values (3, '["abc", 123, 1.5, {"a": 123, "b":[456, "def"]}]');
SQL
    
    dolt sql -q "SELECT * FROM js where pk = 3" -r json
    run dolt sql -q "SELECT * FROM js where pk = 3" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"js":["abc",123,1.5,{"a":123,"b":[456,"def"]}],"pk":3}]}' ]
    
}

@test "json: JSON value printing HTML characters" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"<>&":"<>&"}');
SQL

    dolt sql -q "SELECT * FROM js" -r json
    run dolt sql -q "SELECT * FROM js" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"js":{"<>&":"<>&"},"pk":1}]}' ]

}

@test "json: diff JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
    DELETE FROM js WHERE pk = 2;
    INSERT INTO js VALUES (3, '{"c":3}');
SQL

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}"  = 'diff --dolt a/js b/js' ]
    [ "${lines[3]}"  = '+---+----+----------+' ]
    [ "${lines[4]}"  = '|   | pk | js       |' ]
    [ "${lines[5]}"  = '+---+----+----------+' ]
    [ "${lines[6]}"  = '| < | 1  | {"a":1}  |' ]
    [ "${lines[7]}"  = '| > | 1  | {"a":11} |' ]
    [ "${lines[8]}"  = '| - | 2  | {"b":2}  |' ]
    [ "${lines[9]}"  = '| + | 3  | {"c":3}  |' ]
    [ "${lines[10]}" = '+---+----+----------+' ]
}

@test "json: merge JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"
    dolt branch other
    dolt branch another

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
SQL
    dolt commit -am "made changes on branch main"

    dolt checkout other
    dolt sql <<SQL
    UPDATE js SET js = '{"b":22}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch other"

    dolt checkout main
    dolt merge other --no-commit
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":11}"' ]
    [ "${lines[2]}" = '2,"{""b"":22}"' ]
    dolt commit -am "merged other into main"

    # test merge conflicts
    dolt checkout another
    dolt sql <<SQL
    UPDATE js SET js = '{"b":99}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch another"

    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt conflicts resolve --ours js
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
    [ "${lines[2]}" = '2,"{""b"":99}"' ]
}

@test "json: merge JSON values with stored procedure" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"
    dolt branch other
    dolt branch another

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
SQL
    dolt commit -am "made changes on branch main"

    dolt checkout other
    dolt sql <<SQL
    UPDATE js SET js = '{"b":22}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch other"

    dolt checkout main
    dolt merge other --no-commit
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":11}"' ]
    [ "${lines[2]}" = '2,"{""b"":22}"' ]
    dolt commit -am "merged other into main"

    # test merge conflicts
    dolt checkout another
    dolt sql <<SQL
    UPDATE js SET js = '{"b":99}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch another"

    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'js')"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":1}"' ]
    [ "${lines[2]}" = '2,"{""b"":99}"' ]
}

@test "json: insert value with special characters" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":"<>&"}');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"":""<>&""}"' ]
}


@test "json: insert array with special characters" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '[{"a":"<>&"}]');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"[{""a"":""<>&""}]"' ]
}

@test "json: insert large string value (> 1MB)" {
    dolt sql <<SQL
    CREATE TABLE t (
        pk int PRIMARY KEY,
        j1 json
    );
SQL

    dolt sql -f $BATS_TEST_DIRNAME/json-large-value-insert.sql

    # TODO: Retrieving the JSON errors with a JSON truncated message
    #       Unskip this once the JSON truncation issue is fixed and
    #       fill in the expected length below.
    skip "Function Support is currently disabled"

    run dolt sql -q "SELECT pk, length(j1) FROM t;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,???' ]
}

# This test inserts a large JSON document with the `dolt_dont_optimize_json` flag set.
# We expect that the document gets stored as a blob.
@test "json: Test dolt_optimize_json system variable" {
    run dolt sql <<SQL
    set @@dolt_optimize_json = 0;
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    insert into js values (1, "[[[[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]]],[[[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]]],[[[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]]],[[[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]], [[[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]]]]");
SQL
    # If the document isn't put into an IndexedJsonDocument, it will have the following hash.
    run dolt show qivuleqpbin1eise78h5u8k1hqe4f07g
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Blob" ]] || false
}

# Older clients have a bug that prevents them from reading indexed JSON documents with strings split across chunks,
# And making one large chunk can causes issues with the journal.
# Thus, we expect that the document gets stored as a blob.
@test "json: document with large string value doesn't get indexed" {
    run dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    insert into js values (1, '{"key": "`head -c 2097152 < /dev/zero | tr '\0' '\141'`" }');
    insert into js values (2, '{"`head -c 2097152 < /dev/zero | tr '\0' '\141'`": "value" }');
SQL
    # If the documents aren't put into an IndexedJsonDocument, they will contain the following hashes.
    run dolt show 9erat0qpsqrmmr53fhu6b7gnjj5n1v9i
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Blob" ]] || false
    run dolt show 69cm31n0k392sch3snf5jc7ndfiaund8
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Blob" ]] || false
}