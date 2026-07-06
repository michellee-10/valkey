start_server {tags {"datastats"}} {
    # Wait for the cron scan to complete at least one full pass.
    proc wait_for_datastats_update {} {
        after 500
    }

    test {DATASTATS KEYCOUNTS - empty, multiple types, and deletion} {
        r flushall
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 0
        assert_equal [dict get $stats list_count] 0
        assert_equal [dict get $stats set_count] 0
        assert_equal [dict get $stats zset_count] 0
        assert_equal [dict get $stats hash_count] 0
        assert_equal [dict get $stats stream_count] 0

        # Add multiple types
        r set str1 val
        r set str2 val
        r lpush list1 a
        r sadd set1 a
        r zadd zset1 1 a
        r hset hash1 f v
        r xadd stream1 "*" f v
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 2
        assert_equal [dict get $stats list_count] 1
        assert_equal [dict get $stats set_count] 1
        assert_equal [dict get $stats zset_count] 1
        assert_equal [dict get $stats hash_count] 1
        assert_equal [dict get $stats stream_count] 1

        # Delete and verify decrement
        r del str1
        r del list1
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 1
        assert_equal [dict get $stats list_count] 0
    }

    test {DATASTATS KEYCOUNTS handles type change on overwrite} {
        r flushall
        r sadd mykey a b c
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats set_count] 1
        assert_equal [dict get $stats string_count] 0

        r set mykey "now a string"
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats set_count] 0
        assert_equal [dict get $stats string_count] 1
    }

    test {DATASTATS KEYCOUNTS consistent with DBSIZE} {
        r flushall
        r set s1 v
        r set s2 v
        r lpush l1 a
        r sadd set1 x
        r zadd z1 1 a
        r hset h1 f v
        r xadd stream1 "*" f v
        wait_for_datastats_update
        set stats [r datastats keycounts]
        set total 0
        dict for {type count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS with invalid subcommand returns error} {
        catch {r datastats invalid} err
        assert_match "*unknown subcommand*" $err
    }

    test {DATASTATS ENCODINGS tracks types and conversions} {
        r flushall
        r config set hash-max-listpack-entries 5
        r config set set-max-intset-entries 5
        r config set set-max-listpack-entries 5
        r config set zset-max-listpack-entries 5
        r config set list-max-listpack-size 5

        # Create keys with compact encodings
        r set num_key 12345
        r set short_str "hi"
        r set long_str [string repeat "x" 200]
        r hset small_hash f1 v1
        r hset big_hash f1 v1 f2 v2 f3 v3 f4 v4 f5 v5 f6 v6
        r sadd small_int_set 1 2 3
        r sadd big_int_set 1 2 3 4 5 6
        r sadd small_set "a" "b" "c"
        r sadd big_set a b c d e f
        r zadd small_zset 1 a
        r zadd big_zset 1 a 2 b 3 c 4 d 5 e 6 f
        r lpush small_list a
        r lpush big_list a b c d e [string repeat "x" 200]
        r xadd my_stream "*" f v
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats int] 1
        assert_equal [dict get $stats embstr] 1
        assert_equal [dict get $stats raw] 1
        assert_equal [dict get $stats listpack] 4
        assert_equal [dict get $stats hashtable] 3
        assert_equal [dict get $stats intset] 1
        assert_equal [dict get $stats skiplist] 1
        assert_equal [dict get $stats quicklist] 1
        assert_equal [dict get $stats stream] 1
    }

    test {DATASTATS ENCODINGS reflects encoding conversions} {
        r flushall
        r config set hash-max-listpack-entries 10
        r config set set-max-intset-entries 10
        r config set zset-max-listpack-entries 10
        r config set list-max-listpack-size 1

        r hset my_hash f1 v1
        r sadd my_set 1 2 3
        r zadd my_zset 1 a
        r lpush my_list "short"
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats listpack] 3
        assert_equal [dict get $stats intset] 1

        # Force conversions by exceeding thresholds
        for {set i 0} {$i < 20} {incr i} {
            r hset my_hash "field_$i" "value_$i"
        }
        for {set i 0} {$i < 20} {incr i} {
            r zadd my_zset $i "member_$i"
        }
        r sadd my_set "not_an_integer"
        for {set i 0} {$i < 10} {incr i} {
            r lpush my_list [string repeat "x" 200]
        }
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats hashtable] 1
        assert_equal [dict get $stats skiplist] 1
        assert_equal [dict get $stats quicklist] 1
        assert_equal [dict get $stats listpack] 1
        assert_equal [dict get $stats intset] 0
    }

    test {DATASTATS MEMORY reports non-zero and reflects changes} {
        r flushall
        r set mystr "hello world"
        r lpush mylist a b c
        r sadd myset x y z
        r zadd myzset 1 a 2 b
        r hset myhash f1 v1 f2 v2
        r xadd mystream "*" field value
        wait_for_datastats_update
        set stats [r datastats memory]
        assert {[dict get $stats string_bytes] > 0}
        assert {[dict get $stats list_bytes] > 0}
        assert {[dict get $stats set_bytes] > 0}
        assert {[dict get $stats zset_bytes] > 0}
        assert {[dict get $stats hash_bytes] > 0}
        assert {[dict get $stats stream_bytes] > 0}

        # Larger value uses more memory
        set small_mem [dict get $stats string_bytes]
        r flushall
        r set big_str [string repeat "x" 10000]
        wait_for_datastats_update
        set stats [r datastats memory]
        assert {[dict get $stats string_bytes] > $small_mem}
    }

    test {DATASTATS KEYSIZES buckets keys correctly} {
        r flushall
        r set short val
        r set [string repeat "k" 20] val
        r set [string repeat "m" 100] val
        r set [string repeat "z" 5000] val
        wait_for_datastats_update
        set stats [r datastats keysizes]
        assert_equal [dict get $stats 0B-16B] 1
        assert_equal [dict get $stats 16B-64B] 1
        assert_equal [dict get $stats 64B-256B] 1
        assert_equal [dict get $stats 4096B-plus] 1

        # Total matches DBSIZE
        set total 0
        dict for {bucket count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS VALUESIZES buckets values correctly} {
        r flushall
        r set tiny "hi"
        r set medium [string repeat "x" 500]
        r set large [string repeat "y" 100000]
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        assert_equal [dict get $stats 0B-64B] 1
        assert_equal [dict get $stats 64B-1024B] 1
        assert_equal [dict get $stats 16384B-262144B] 1

        # Total matches DBSIZE
        set total 0
        dict for {bucket count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS ALL returns all sections consistently} {
        r flushall
        r set s1 "val"
        r set s2 12345
        r lpush l1 a b c
        r hset h1 f v
        wait_for_datastats_update
        set all [r datastats all]
        assert {[dict exists $all keycounts]}
        assert {[dict exists $all encodings]}
        assert {[dict exists $all memory]}
        assert {[dict exists $all keysizes]}
        assert {[dict exists $all valuesizes]}

        # Verify consistency with individual subcommands
        assert_equal [dict get $all keycounts] [r datastats keycounts]
        assert_equal [dict get $all encodings] [r datastats encodings]
        assert_equal [dict get $all memory] [r datastats memory]
        assert_equal [dict get $all keysizes] [r datastats keysizes]
        assert_equal [dict get $all valuesizes] [r datastats valuesizes]
    }

    test {DATASTATS KEYCOUNTS handles large dataset} {
        r flushall
        for {set i 0} {$i < 500} {incr i} {
            r set "str_$i" "val"
        }
        for {set i 0} {$i < 300} {incr i} {
            r lpush "list_$i" "val"
        }
        for {set i 0} {$i < 200} {incr i} {
            r hset "hash_$i" f v
        }
        after 2000
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 500
        assert_equal [dict get $stats list_count] 300
        assert_equal [dict get $stats hash_count] 200
    }

    r config set hash-max-listpack-entries 512
    r config set set-max-intset-entries 512
    r config set set-max-listpack-entries 128
    r config set zset-max-listpack-entries 128
    r config set list-max-listpack-size -2
}
