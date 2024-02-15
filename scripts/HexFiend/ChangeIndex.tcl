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
    if {$name ne ""} {
       return [str $length "utf8" $name]
    } else {
       return [str $length "utf8"]
    }
}

proc buffer {name} {
    set length [varint ""]
    if {$length == 0} {
        return ""
    }
    if {$name ne ""} {
       return [hex $length $name]
    } else {
       return [hex $length]
    }
}

set PAGE_SIZE [expr {64 * 1024}]

while {![end]} {
    set saved_pos [pos]
    set account [account_id ""]
    if {$account eq ""} {
        # if pos not at the end of the page, round it up
        if {[pos] % $PAGE_SIZE != 0} {
            goto [expr {int([pos] / $PAGE_SIZE) * $PAGE_SIZE + $PAGE_SIZE}]
        }

        continue
    }
    goto $saved_pos

    section -collapsed "Account" {
        sectionvalue $account
        account_id "account_id"

        while {![end]} {
            set saved_pos [pos]
            set key [buffer ""]
            if {[string length $key] == 0} {
                break
            }
            goto $saved_pos
            section "Key" {
                sectionvalue $key
                buffer "key"
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