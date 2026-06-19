start_server {tags {"datastats"}} {
    # Wait for the cron scan to complete at least one full pass.
    proc wait_for_datastats_update {} {
        after 500
    }

    test {DATASTATS KEYCOUNTS returns zero counts on empty database} {
        r flushall
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 0
        assert_equal [dict get $stats list_count] 0
        assert_equal [dict get $stats set_count] 0
        assert_equal [dict get $stats zset_count] 0
        assert_equal [dict get $stats hash_count] 0
        assert_equal [dict get $stats module_count] 0
        assert_equal [dict get $stats stream_count] 0
    }

    test {DATASTATS KEYCOUNTS tracks multiple types simultaneously} {
        r flushall
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
    }

    test {DATASTATS KEYCOUNTS reflects key deletion with DEL} {
        r flushall
        r set key1 val1
        r set key2 val2
        r lpush mylist a
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 2
        assert_equal [dict get $stats list_count] 1

        r del key1
        r del mylist
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 1
        assert_equal [dict get $stats list_count] 0
    }

    test {DATASTATS KEYCOUNTS reflects key deletion with UNLINK} {
        r flushall
        r set key1 val1
        r set key2 val2
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 2

        r unlink key1
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 1
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

    test {DATASTATS KEYCOUNTS reflects key expiry} {
        r flushall
        r set mykey val
        r pexpire mykey 1500
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 1

        after 1600
        # Access to trigger lazy expire
        r get mykey
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 0
    }

    test {DATASTATS KEYCOUNTS reflects FLUSHDB} {
        r flushall
        r set key1 val1
        r lpush list1 a
        r sadd set1 x
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 1
        assert_equal [dict get $stats list_count] 1
        assert_equal [dict get $stats set_count] 1

        r flushdb
        wait_for_datastats_update
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 0
        assert_equal [dict get $stats list_count] 0
        assert_equal [dict get $stats set_count] 0
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

    test {DATASTATS with no subcommand returns error} {
        catch {r datastats} err
        assert_match "*wrong number of arguments*" $err
    }

    test {DATASTATS KEYCOUNTS handles large dataset across multiple cron ticks} {
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
        # Give the scan more time to complete on a larger dataset
        after 2000
        set stats [r datastats keycounts]
        assert_equal [dict get $stats string_count] 500
        assert_equal [dict get $stats list_count] 300
        assert_equal [dict get $stats hash_count] 200
    }

    test {DATASTATS ENCODINGS tracks multiple encodings simultaneously} {
        r flushall
        r config set hash-max-listpack-entries 5
        r config set set-max-intset-entries 5
        r config set set-max-listpack-entries 5
        r config set zset-max-listpack-entries 5
        r config set list-max-listpack-size 5
        # Strings: int, embstr, raw
        r set num_key 12345
        r set short_str "hi"
        r set long_str [string repeat "x" 200]
        # Hash: small (listpack) and large (hashtable)
        r hset small_hash f1 v1
        r hset big_hash f1 v1 f2 v2 f3 v3 f4 v4 f5 v5 f6 v6
        # Set: small int-only (intset), large int-only (listpack), small string (listpack), large string (hashtable)
        r sadd small_int_set 1 2 3
        r sadd big_int_set 1 2 3 4 5 6
        r sadd small_set "a" "b" "c"
        r sadd big_set a b c d e f
        # ZSet: small (listpack) and large (skiplist)
        r zadd small_zset 1 a
        r zadd big_zset 1 a 2 b 3 c 4 d 5 e 6 f
        # List: small (listpack) and large (quicklist)
        r lpush small_list a
        r lpush big_list a b c d e [string repeat "x" 200]
        # Stream
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
        # Create keys with compact encodings
        r hset my_hash f1 v1
        r sadd my_set 1 2 3
        r zadd my_zset 1 a
        r lpush my_list "short"
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats listpack] 3
        assert_equal [dict get $stats intset] 1
        assert_equal [dict get $stats hashtable] 0
        assert_equal [dict get $stats skiplist] 0
        assert_equal [dict get $stats quicklist] 0

        # Force conversions by exceeding thresholds
        for {set i 0} {$i < 20} {incr i} {
            r hset my_hash "field_$i" "value_$i"
        }
        for {set i 0} {$i < 20} {incr i} {
            r zadd my_zset $i "member_$i"
        }
        # Adding a string to intset forces conversion to listpack
        r sadd my_set "not_an_integer"
        # Adding large elements forces quicklist
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

    test {DATASTATS ENCODINGS total consistent with DBSIZE} {
        r flushall
        r set s1 v
        r set s2 12345
        r lpush l1 a b c
        r sadd set1 1 2 3
        r hset h1 f v
        r xadd stream1 "*" f v
        wait_for_datastats_update
        set stats [r datastats encodings]
        set total 0
        dict for {enc count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS ENCODINGS reflects deletion} {
        r flushall
        r config set hash-max-listpack-entries 1
        # Create keys with known encodings
        r set my_str "hello"
        r hset my_hash f1 v1 f2 v2
        r sadd my_set 1 2 3
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats embstr] 1
        assert_equal [dict get $stats hashtable] 1
        assert_equal [dict get $stats intset] 1

        # Delete them
        r del my_str
        r del my_hash
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats embstr] 0
        assert_equal [dict get $stats hashtable] 0
        assert_equal [dict get $stats intset] 1
    }

    test {DATASTATS ENCODINGS conversion is one-way} {
        r flushall
        r config set hash-max-listpack-entries 5
        # Create a hash that exceeds listpack threshold
        r hset my_hash f1 v1 f2 v2 f3 v3 f4 v4 f5 v5 f6 v6
        wait_for_datastats_update
        set stats [r datastats encodings]
        assert_equal [dict get $stats hashtable] 1
        assert_equal [dict get $stats listpack] 0

        # Remove fields so it's back under the threshold
        r hdel my_hash f2 f3 f4 f5 f6
        wait_for_datastats_update
        set stats [r datastats encodings]
        # Still hashtable — encoding does not downgrade
        assert_equal [dict get $stats hashtable] 1
        assert_equal [dict get $stats listpack] 0
    }

    test {DATASTATS MEMORY reports non-zero for each type} {
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
    }

    test {DATASTATS MEMORY larger values use more memory} {
        r flushall
        r set small_str "hi"
        wait_for_datastats_update
        set stats_before [r datastats memory]
        set small_mem [dict get $stats_before string_bytes]

        r flushall
        r set big_str [string repeat "x" 10000]
        wait_for_datastats_update
        set stats_after [r datastats memory]
        set big_mem [dict get $stats_after string_bytes]

        assert {$big_mem > $small_mem}
    }

    test {DATASTATS MEMORY reflects deletion} {
        r flushall
        r set key1 "some value"
        r set key2 "another value"
        wait_for_datastats_update
        set stats [r datastats memory]
        set mem_before [dict get $stats string_bytes]
        assert {$mem_before > 0}

        r del key1 key2
        wait_for_datastats_update
        set stats [r datastats memory]
        assert_equal [dict get $stats string_bytes] 0
    }

    test {DATASTATS MEMORY reflects value update on same key} {
        r flushall
        r set mykey "small"
        wait_for_datastats_update
        set stats [r datastats memory]
        set mem_before [dict get $stats string_bytes]

        r set mykey [string repeat "x" 10000]
        wait_for_datastats_update
        set stats [r datastats memory]
        set mem_after [dict get $stats string_bytes]

        assert {$mem_after > $mem_before}
    }

    test {DATASTATS MEMORY reflects type change on overwrite} {
        r flushall
        r sadd mykey a b c
        wait_for_datastats_update
        set stats [r datastats memory]
        assert {[dict get $stats set_bytes] > 0}
        assert_equal [dict get $stats string_bytes] 0

        r set mykey "now a string"
        wait_for_datastats_update
        set stats [r datastats memory]
        assert_equal [dict get $stats set_bytes] 0
        assert {[dict get $stats string_bytes] > 0}
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
        assert_equal [dict get $stats 256B-1024B] 0
        assert_equal [dict get $stats 1024B-4096B] 0
        assert_equal [dict get $stats 4096B-plus] 1
    }

    test {DATASTATS KEYSIZES total consistent with DBSIZE} {
        r flushall
        r set tiny val
        r set [string repeat "x" 30] val
        r set [string repeat "y" 100] val
        r set [string repeat "z" 500] val
        wait_for_datastats_update
        set stats [r datastats keysizes]
        set total 0
        dict for {bucket count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS KEYSIZES reflects deletion} {
        r flushall
        r set short val
        r set [string repeat "k" 20] val
        wait_for_datastats_update
        set stats [r datastats keysizes]
        assert_equal [dict get $stats 0B-16B] 1
        assert_equal [dict get $stats 16B-64B] 1

        r del short
        wait_for_datastats_update
        set stats [r datastats keysizes]
        assert_equal [dict get $stats 0B-16B] 0
        assert_equal [dict get $stats 16B-64B] 1
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
    }

    test {DATASTATS VALUESIZES total consistent with DBSIZE} {
        r flushall
        r set s1 "small"
        r set s2 [string repeat "a" 300]
        r set s3 [string repeat "b" 5000]
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        set total 0
        dict for {bucket count} $stats {
            incr total $count
        }
        assert_equal $total [r dbsize]
    }

    test {DATASTATS VALUESIZES reflects value update} {
        r flushall
        r set mykey "small"
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        assert_equal [dict get $stats 0B-64B] 1
        assert_equal [dict get $stats 16384B-262144B] 0

        r set mykey [string repeat "x" 100000]
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        assert_equal [dict get $stats 0B-64B] 0
        assert_equal [dict get $stats 16384B-262144B] 1
    }

    test {DATASTATS VALUESIZES reflects deletion} {
        r flushall
        r set mykey [string repeat "x" 500]
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        assert_equal [dict get $stats 64B-1024B] 1

        r del mykey
        wait_for_datastats_update
        set stats [r datastats valuesizes]
        assert_equal [dict get $stats 64B-1024B] 0
    }

    test {DATASTATS ALL returns all sections} {
        r flushall
        r set mystr "hello"
        r lpush mylist a b c
        r sadd myset x y z
        wait_for_datastats_update
        set stats [r datastats all]
        assert {[dict exists $stats keycounts]}
        assert {[dict exists $stats encodings]}
        assert {[dict exists $stats memory]}
        assert {[dict exists $stats keysizes]}
        assert {[dict exists $stats valuesizes]}
        set keycounts [dict get $stats keycounts]
        assert_equal [dict get $keycounts string_count] 1
        assert_equal [dict get $keycounts list_count] 1
        assert_equal [dict get $keycounts set_count] 1
        set memory [dict get $stats memory]
        assert {[dict get $memory string_bytes] > 0}
        assert {[dict get $memory list_bytes] > 0}
        assert {[dict get $memory set_bytes] > 0}
    }

    test {DATASTATS ALL consistent with individual subcommands} {
        r flushall
        r set s1 "val"
        r set s2 12345
        r lpush l1 a b c
        r hset h1 f v
        wait_for_datastats_update
        set all [r datastats all]
        set keycounts [r datastats keycounts]
        set encodings [r datastats encodings]
        set memory [r datastats memory]
        set keysizes [r datastats keysizes]
        set valuesizes [r datastats valuesizes]
        assert_equal [dict get $all keycounts] $keycounts
        assert_equal [dict get $all encodings] $encodings
        assert_equal [dict get $all memory] $memory
        assert_equal [dict get $all keysizes] $keysizes
        assert_equal [dict get $all valuesizes] $valuesizes
    }
}
