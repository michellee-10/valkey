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
}
