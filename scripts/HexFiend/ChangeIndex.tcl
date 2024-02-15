proc varint {name {delta 0}} {
    # loop through bytes reading protobuf varint
    set value 0
    set shift 0
    set start_offset [pos]
    while {1} {
        set byte [uint8]
        set value [expr {$value | (($byte & 0x7f) << $shift)}]
        set shift [expr {$shift + 7}]
        if {($byte & 0x80) == 0} {
            if {$delta != 0} {
                set value [expr {$delta - $value}]
            }
            if {$name ne ""} {
                entry $name $value [expr {[pos] - $start_offset}] $start_offset
            }

            return $value
        }
    }
}

proc account_id {name} {
    set length [varint ""]
    if {$length == 0} {
        return ""
    }
    set value [str $length "utf8" $name]
    return $value
}

proc buffer {name} {
    set length [varint ""]
    if {$length == 0} {
        return ""
    }
    set value [hex $length $name]
    return $value
}

while {![end]} {
    section -collapsed "Account" {
        set account [account_id "account"]
        if {$account eq ""} {
            endsection
            break
        }
        sectionvalue $account

        while {![end]} {
            section "Key" {
                set key [buffer "key"]
                if {[string length $key] == 0} {
                    endsection
                    break
                }
                sectionvalue $key
                section -collapsed "Changes" {
                    set changes_count [varint "count"]
                    sectionvalue $changes_count
                    set prev_change [varint "0"]
                    for {set i 1} {$i < $changes_count} {incr i} {
                        set cur_change [varint "$i" $prev_change]
                        set prev_change $cur_change
                    }
                }
            }
        }
    }
}