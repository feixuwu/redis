# Tcl client library - used by the Redis test
#
# Copyright (C) 2014-Present, Redis Ltd.
# All Rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2) or the Server Side Public License v1 (SSPLv1).
#
# Example usage:
#
# set r [redis 127.0.0.1 6379]
# $r lpush mylist foo
# $r lpush mylist bar
# $r lrange mylist 0 -1
# $r close
#
# Non blocking usage example:
#
# proc handlePong {r type reply} {
#     puts "PONG $type '$reply'"
#     if {$reply ne "PONG"} {
#         $r ping [list handlePong]
#     }
# }
#
# set r [redis]
# $r blocking 0
# $r get fo [list handlePong]
#
# vwait forever

package require Tcl 8.5
package provide redis 0.1

source [file join [file dirname [info script]] "response_transformers.tcl"]

namespace eval redis {}
set ::redis::id 0
array set ::redis::fd {}
array set ::redis::addr {}
array set ::redis::blocking {}
array set ::redis::deferred {}
array set ::redis::readraw {}
array set ::redis::attributes {} ;# Holds the RESP3 attributes from the last call
array set ::redis::reconnect {}
array set ::redis::tls {}
array set ::redis::callback {}
array set ::redis::state {} ;# State in non-blocking reply reading
array set ::redis::statestack {} ;# Stack of states, for nested mbulks
array set ::redis::curr_argv {} ;# Remember the current argv, to be used in response_transformers.tcl
array set ::redis::testing_resp3 {} ;# Indicating if the current client is using RESP3 (only if the test is trying to test RESP3 specific behavior. It won't be on in case of force_resp3)
array set ::redis::mux_enabled {} ;# Whether this client is in MUX mode
array set ::redis::mux_stream_id {} ;# The MUX stream ID for this client
array set ::redis::mux_read_buf {} ;# MUX frame reassembly buffer (raw bytes from socket)
array set ::redis::mux_resp_buf {} ;# Decoded RESP payload extracted from MUX frames

set ::force_resp3 0
set ::log_req_res 0
set ::mux 0

proc redis {{server 127.0.0.1} {port 6379} {defer 0} {tls 0} {tlsoptions {}} {readraw 0}} {
    if {$tls} {
        package require tls
        ::tls::init \
            -cafile "$::tlsdir/ca.crt" \
            -certfile "$::tlsdir/client.crt" \
            -keyfile "$::tlsdir/client.key" \
            {*}$tlsoptions
        set fd [::tls::socket $server $port]
    } else {
        set fd [socket $server $port]
    }
    fconfigure $fd -translation binary
    set id [incr ::redis::id]
    set ::redis::fd($id) $fd
    set ::redis::addr($id) [list $server $port]
    set ::redis::blocking($id) 1
    set ::redis::deferred($id) $defer
    set ::redis::readraw($id) $readraw
    set ::redis::reconnect($id) 0
    set ::redis::curr_argv($id) 0
    set ::redis::testing_resp3($id) 0
    set ::redis::tls($id) $tls
    set ::redis::mux_enabled($id) 0
    set ::redis::mux_stream_id($id) 1
    set ::redis::mux_read_buf($id) {}
    set ::redis::mux_resp_buf($id) {}
    ::redis::redis_reset_state $id
    interp alias {} ::redis::redisHandle$id {} ::redis::__dispatch__ $id
}

# On recent versions of tcl-tls/OpenSSL, reading from a dropped connection
# results with an error we need to catch and mimic the old behavior.
proc ::redis::redis_safe_read {fd len} {
    if {$len == -1} {
        set err [catch {set val [read $fd]} msg]
    } else {
        set err [catch {set val [read $fd $len]} msg]
    }
    if {!$err} {
        return $val
    }
    if {[string match "*connection abort*" $msg]} {
        return {}
    }
    error $msg
}

proc ::redis::redis_safe_gets {fd} {
    if {[catch {set val [gets $fd]} msg]} {
        if {[string match "*connection abort*" $msg]} {
            return {}
        }
        error $msg
    }
    return $val
}

# This is a wrapper to the actual dispatching procedure that handles
# reconnection if needed.
proc ::redis::__dispatch__ {id method args} {
    set errorcode [catch {::redis::__dispatch__raw__ $id $method $args} retval]
    if {$errorcode && $::redis::reconnect($id) && $::redis::fd($id) eq {}} {
        # Try again if the connection was lost.
        # FIXME: we don't re-select the previously selected DB, nor we check
        # if we are inside a transaction that needs to be re-issued from
        # scratch.
        set errorcode [catch {::redis::__dispatch__raw__ $id $method $args} retval]
    }
    return -code $errorcode $retval
}

proc ::redis::__dispatch__raw__ {id method argv} {
    set fd $::redis::fd($id)

    # Reconnect the link if needed.
    if {$fd eq {} && $method ne {close}} {
        set was_mux $::redis::mux_enabled($id)
        lassign $::redis::addr($id) host port
        if {$::redis::tls($id)} {
            set ::redis::fd($id) [::tls::socket $host $port]
        } else {
            set ::redis::fd($id) [socket $host $port]
        }
        fconfigure $::redis::fd($id) -translation binary
        set fd $::redis::fd($id)

        # Re-enable MUX mode if it was previously enabled
        if {$was_mux} {
            set ::redis::mux_enabled($id) 0
            set ::redis::mux_resp_buf($id) {}
            set ::redis::mux_read_buf($id) {}
            # Send HELLO 3 MULTIPLEX to re-establish MUX on the new connection
            set hello_cmd "*3\r\n\$5\r\nHELLO\r\n\$1\r\n3\r\n\$9\r\nMULTIPLEX\r\n"
            redis_write $fd $hello_cmd
            flush $fd
            if {[catch {redis_read_reply_logic $id $fd} hello_reply]} {
                # If re-enable fails (e.g. NOAUTH), stay in non-MUX mode
            } else {
                set ::redis::mux_enabled($id) 1
                set ::redis::mux_stream_id($id) 1
                set ::redis::mux_resp_buf($id) {}
                set ::redis::testing_resp3($id) 0
            }
        }
    }

    # Transform HELLO 2 to HELLO 3 if force_resp3
    # All set the connection var testing_resp3 in case of HELLO 3
    if {[llength $argv] > 0 && [string compare -nocase $method "HELLO"] == 0} {
        if {[lindex $argv 0] == 3} {
            set ::redis::testing_resp3($id) 1
        } else {
            set ::redis::testing_resp3($id) 0
            if {$::force_resp3} {
                # If we are in force_resp3 we run HELLO 3 instead of HELLO 2
                lset argv 0 3
            }
        }
    }

    set blocking $::redis::blocking($id)
    set deferred $::redis::deferred($id)
    if {$blocking == 0} {
        if {[llength $argv] == 0} {
            error "Please provide a callback in non-blocking mode"
        }
        set callback [lindex $argv end]
        set argv [lrange $argv 0 end-1]
    }
    if {[info command ::redis::__method__$method] eq {}} {
        catch {unset ::redis::attributes($id)}
        set cmd "*[expr {[llength $argv]+1}]\r\n"
        append cmd "$[string length $method]\r\n$method\r\n"
        foreach a $argv {
            append cmd "$[string length $a]\r\n$a\r\n"
        }
        if {$::redis::mux_enabled($id)} {
            ::redis::redis_mux_write $id $fd $cmd
        } else {
            ::redis::redis_write $fd $cmd
        }
        if {[catch {flush $fd}]} {
            catch {close $fd}
            set ::redis::fd($id) {}
            return -code error "I/O error reading reply"
        }

        set ::redis::curr_argv($id) [concat $method $argv]
        if {!$deferred} {
            if {$blocking} {
                if {$::redis::mux_enabled($id)} {
                    ::redis::redis_mux_read_reply $id $fd
                } else {
                    ::redis::redis_read_reply $id $fd
                }
            } else {
                # Every well formed reply read will pop an element from this
                # list and use it as a callback. So pipelining is supported
                # in non blocking mode.
                lappend ::redis::callback($id) $callback
                fileevent $fd readable [list ::redis::redis_readable $fd $id]
            }
        }
    } else {
        uplevel 1 [list ::redis::__method__$method $id $fd] $argv
    }
}

proc ::redis::__method__blocking {id fd val} {
    set ::redis::blocking($id) $val
    fconfigure $fd -blocking $val
}

proc ::redis::__method__reconnect {id fd val} {
    set ::redis::reconnect($id) $val
}

proc ::redis::__method__read {id fd} {
    if {$::redis::mux_enabled($id)} {
        ::redis::redis_mux_read_reply $id $fd
    } else {
        ::redis::redis_read_reply $id $fd
    }
}

proc ::redis::__method__rawread {id fd {len -1}} {
    return [redis_safe_read $fd $len]
}

proc ::redis::__method__write {id fd buf} {
    if {$::redis::mux_enabled($id)} {
        ::redis::redis_mux_write $id $fd $buf
    } else {
        ::redis::redis_write $fd $buf
    }
}

proc ::redis::__method__flush {id fd} {
    flush $fd
}

proc ::redis::__method__close {id fd} {
    catch {close $fd}
    catch {unset ::redis::fd($id)}
    catch {unset ::redis::addr($id)}
    catch {unset ::redis::blocking($id)}
    catch {unset ::redis::deferred($id)}
    catch {unset ::redis::readraw($id)}
    catch {unset ::redis::attributes($id)}
    catch {unset ::redis::reconnect($id)}
    catch {unset ::redis::tls($id)}
    catch {unset ::redis::state($id)}
    catch {unset ::redis::statestack($id)}
    catch {unset ::redis::callback($id)}
    catch {unset ::redis::curr_argv($id)}
    catch {unset ::redis::testing_resp3($id)}
    catch {unset ::redis::mux_enabled($id)}
    catch {unset ::redis::mux_stream_id($id)}
    catch {unset ::redis::mux_read_buf($id)}
    catch {unset ::redis::mux_resp_buf($id)}
    catch {interp alias {} ::redis::redisHandle$id {}}
}

proc ::redis::__method__channel {id fd} {
    return $fd
}

proc ::redis::__method__mux_enabled {id fd} {
    return $::redis::mux_enabled($id)
}

proc ::redis::__method__deferred {id fd val} {
    set ::redis::deferred($id) $val
}

proc ::redis::__method__readraw {id fd val} {
    set ::redis::readraw($id) $val
}

proc ::redis::__method__readingraw {id fd} {
    return $::redis::readraw($id)
}

proc ::redis::__method__attributes {id fd} {
    set _ $::redis::attributes($id)
}

proc ::redis::redis_write {fd buf} {
    puts -nonewline $fd $buf
}

# Write data through the MUX framing layer for a specific client id.
# Wraps the RESP data in a MUX DATA frame before sending.
proc ::redis::redis_mux_write {id fd buf} {
    set stream_id $::redis::mux_stream_id($id)
    set payload_len [string length $buf]
    set frame [mux_encode_frame 0x01 $stream_id $buf]
    puts -nonewline $fd $frame
}

# ========================== MUX Frame Encoding/Decoding ==========================

# MUX frame header: Magic(1) + Flags(1) + StreamID(8) + PayloadLen(4) = 14 bytes
# Magic = 0xAA

proc ::redis::mux_encode_frame {flags stream_id payload} {
    set payload_len [string length $payload]
    # Build 14-byte header: Magic(1) + Flags(1) + StreamID(8) + PayloadLen(4)
    set hdr {}
    append hdr [binary format c 170] ;# 0xAA magic byte
    append hdr [binary format c $flags]
    # Stream ID: 8 bytes big-endian
    append hdr [binary format II \
        [expr {($stream_id >> 32) & 0xFFFFFFFF}] \
        [expr {$stream_id & 0xFFFFFFFF}]]
    # Payload length: 4 bytes big-endian
    append hdr [binary format I $payload_len]
    append hdr $payload
    return $hdr
}

# Read exactly n bytes from fd, handling partial reads
proc ::redis::mux_read_bytes {fd n} {
    set data {}
    set remaining $n
    while {$remaining > 0} {
        set chunk [redis_safe_read $fd $remaining]
        if {$chunk eq {}} {
            error "I/O error reading MUX data"
        }
        append data $chunk
        set remaining [expr {$n - [string length $data]}]
    }
    return $data
}

# Read a single MUX frame from fd.
# Returns a list: {flags stream_id payload}
# For DATA frames (type 0x01), payload contains raw RESP data.
# For control frames, payload may be empty.
proc ::redis::mux_read_frame {fd} {
    # Read 14-byte header
    set hdr [mux_read_bytes $fd 14]
    binary scan $hdr c magic
    set magic [expr {$magic & 0xFF}]
    if {$magic != 0xAA} {
        error "MUX: bad magic byte 0x[format %02x $magic]"
    }
    binary scan $hdr x1c flags_raw
    set flags [expr {$flags_raw & 0xFF}]
    # Stream ID: bytes 2-9, 8 bytes big-endian unsigned
    binary scan $hdr x2II sid_hi sid_lo
    set sid_hi [expr {$sid_hi & 0xFFFFFFFF}]
    set sid_lo [expr {$sid_lo & 0xFFFFFFFF}]
    set stream_id [expr {(wide($sid_hi) << 32) | $sid_lo}]
    # Payload length: bytes 10-13, 4 bytes big-endian
    binary scan $hdr x10I payload_len
    set payload_len [expr {$payload_len & 0xFFFFFFFF}]

    set payload {}
    if {$payload_len > 0} {
        set payload [mux_read_bytes $fd $payload_len]
    }
    return [list $flags $stream_id $payload]
}

# Read MUX frames until we get a DATA frame for our stream, accumulating
# RESP data. Skip control frames (PONG, STREAM_CLOSE, etc.).
proc ::redis::mux_read_resp_data {id fd needed} {
    # First check if we already have enough in resp_buf
    while {[string length $::redis::mux_resp_buf($id)] < $needed || $needed == 0} {
        set frame [mux_read_frame $fd]
        lassign $frame flags stream_id payload
        set frame_type [expr {$flags & 0x0F}]
        if {$frame_type == 0x01} {
            # DATA frame
            append ::redis::mux_resp_buf($id) $payload
            if {$needed == 0} {
                # Caller just wants any data
                break
            }
        } elseif {$frame_type == 0x03} {
            # STREAM_CLOSE - server closed the stream
            error "I/O error reading reply"
        }
        # Silently skip PONG (0x05), GOAWAY (0x06), etc.
    }
}

# Enable MUX mode on an existing client connection.
# Sends HELLO 3 MULTIPLEX and switches the client to MUX framing mode.
# Optional args: ?password? ?username? - for servers that require authentication.
# If NOAUTH error is received, MUX mode is silently skipped (graceful degradation).
proc ::redis::__method__enable_mux {id fd args} {
    set stream_id 1
    set password {}
    set username {}

    # Parse args: enable_mux ?stream_id? ?password? ?username?
    if {[llength $args] >= 1} {
        set stream_id [lindex $args 0]
    }
    if {[llength $args] >= 2} {
        set password [lindex $args 1]
    }
    if {[llength $args] >= 3} {
        set username [lindex $args 2]
    }

    # Build HELLO command with optional AUTH
    if {$password ne {}} {
        if {$username ne {}} {
            # HELLO 3 MULTIPLEX AUTH <username> <password>
            set ulen [string length $username]
            set plen [string length $password]
            set cmd "*6\r\n\$5\r\nHELLO\r\n\$1\r\n3\r\n\$9\r\nMULTIPLEX\r\n\$4\r\nAUTH\r\n\$$ulen\r\n$username\r\n\$$plen\r\n$password\r\n"
        } else {
            # HELLO 3 MULTIPLEX AUTH default <password>
            set plen [string length $password]
            set cmd "*6\r\n\$5\r\nHELLO\r\n\$1\r\n3\r\n\$9\r\nMULTIPLEX\r\n\$4\r\nAUTH\r\n\$7\r\ndefault\r\n\$$plen\r\n$password\r\n"
        }
    } else {
        set cmd "*3\r\n\$5\r\nHELLO\r\n\$1\r\n3\r\n\$9\r\nMULTIPLEX\r\n"
    }

    # Send HELLO using raw RESP (before MUX is active)
    redis_write $fd $cmd
    flush $fd

    # Read HELLO reply (still in RESP3 mode, before MUX framing starts)
    if {[catch {redis_read_reply_logic $id $fd} reply]} {
        # If we got a NOAUTH error, gracefully degrade - don't enable MUX
        if {[string match "NOAUTH*" $reply] || [string match "*NOAUTH*" $reply]} {
            return
        }
        # Re-throw other errors
        error $reply
    }

    # Check if reply is an error string (RESP error replies start with -)
    if {[string match "NOAUTH*" $reply] || [string match "ERR*" $reply]} {
        # Server returned an error, don't enable MUX
        return
    }

    # Now the server is in MUX mode - all subsequent data is framed
    set ::redis::mux_enabled($id) 1
    set ::redis::mux_stream_id($id) $stream_id
    set ::redis::mux_resp_buf($id) {}
    set ::redis::testing_resp3($id) 0
    return $reply
}

proc ::redis::redis_writenl {fd buf} {
    redis_write $fd $buf
    redis_write $fd "\r\n"
    flush $fd
}

proc ::redis::redis_readnl {fd len} {
    set buf [redis_safe_read $fd $len]
    redis_safe_read $fd 2 ; # discard CR LF
    return $buf
}

proc ::redis::redis_bulk_read {fd} {
    set count [redis_read_line $fd]
    if {$count == -1} return {}
    set buf [redis_readnl $fd $count]
    return $buf
}

proc ::redis::redis_multi_bulk_read {id fd} {
    set count [redis_read_line $fd]
    if {$count == -1} return {}
    set l {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            lappend l [redis_read_reply_logic $id $fd]
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $l
}

proc ::redis::redis_read_map {id fd} {
    set count [redis_read_line $fd]
    if {$count == -1} return {}
    set d {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            set k [redis_read_reply_logic $id $fd] ; # key
            set v [redis_read_reply_logic $id $fd] ; # value
            dict set d $k $v
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $d
}

proc ::redis::redis_read_line fd {
    string trim [redis_safe_gets $fd]
}

proc ::redis::redis_read_null fd {
    redis_safe_gets $fd
    return {}
}

proc ::redis::redis_read_bool fd {
    set v [redis_read_line $fd]
    if {$v == "t"} {return 1}
    if {$v == "f"} {return 0}
    return -code error "Bad protocol, '$v' as bool type"
}

proc ::redis::redis_read_double {id fd} {
    set v [redis_read_line $fd]
    # unlike many other DTs, there is a textual difference between double and a string with the same value,
    # so we need to transform to double if we are testing RESP3 (i.e. some tests check that a
    # double reply is "1.0" and not "1")
    if {[should_transform_to_resp2 $id]} {
        return $v
    } else {
        return [expr {double($v)}]
    }
}

proc ::redis::redis_read_verbatim_str fd {
    set v [redis_bulk_read $fd]
    # strip the first 4 chars ("txt:")
    return [string range $v 4 end]
}

proc ::redis::redis_read_reply_logic {id fd} {
    if {$::redis::readraw($id)} {
        return [redis_read_line $fd]
    }

    while {1} {
        set type [redis_safe_read $fd 1]
        switch -exact -- $type {
            _ {return [redis_read_null $fd]}
            : -
            ( -
            + {return [redis_read_line $fd]}
            , {return [redis_read_double $id $fd]}
            # {return [redis_read_bool $fd]}
            = {return [redis_read_verbatim_str $fd]}
            - {return -code error [redis_read_line $fd]}
            $ {return [redis_bulk_read $fd]}
            > -
            ~ -
            * {return [redis_multi_bulk_read $id $fd]}
            % {return [redis_read_map $id $fd]}
            | {
                set attrib [redis_read_map $id $fd]
                set ::redis::attributes($id) $attrib
                continue
            }
            default {
                if {$type eq {}} {
                    catch {close $fd}
                    set ::redis::fd($id) {}
                    return -code error "I/O error reading reply"
                }
                return -code error "Bad protocol, '$type' as reply type byte"
            }
        }
    }
}

proc ::redis::redis_read_reply {id fd} {
    set response [redis_read_reply_logic $id $fd]
    ::response_transformers::transform_response_if_needed $id $::redis::curr_argv($id) $response
}

# MUX-aware reply reader.
# In MUX mode, we first read MUX frames from the socket, extract the RESP
# payload, then parse it using a virtual channel backed by the resp_buf.
proc ::redis::redis_mux_read_reply {id fd} {
    # Read MUX frames until we get at least some RESP data
    if {[string length $::redis::mux_resp_buf($id)] == 0} {
        mux_read_resp_data $id $fd 1
    }
    # Now parse RESP from the mux_resp_buf using a buffered approach.
    # We create a temporary read interface that reads from mux_resp_buf
    # instead of the raw fd, refilling from MUX frames as needed.
    set response [redis_mux_read_reply_logic $id $fd]
    ::response_transformers::transform_response_if_needed $id $::redis::curr_argv($id) $response
}

# Read a single byte from the MUX RESP buffer, refilling from frames if needed
proc ::redis::mux_buf_read {id fd len} {
    while {[string length $::redis::mux_resp_buf($id)] < $len} {
        mux_read_resp_data $id $fd 1
    }
    set data [string range $::redis::mux_resp_buf($id) 0 [expr {$len - 1}]]
    set ::redis::mux_resp_buf($id) [string range $::redis::mux_resp_buf($id) $len end]
    return $data
}

# Read a line (terminated by \n, with optional \r stripped) from MUX RESP buffer.
# This mimics Tcl's [gets] behavior for readraw mode, where lines are
# delimited by \n (not \r\n). Needed because bulk string payloads may
# contain bare \n inside the data (e.g. verbatim strings).
proc ::redis::mux_buf_gets_lf {id fd} {
    while {1} {
        set idx [string first "\n" $::redis::mux_resp_buf($id)]
        if {$idx >= 0} {
            set line [string range $::redis::mux_resp_buf($id) 0 [expr {$idx - 1}]]
            set ::redis::mux_resp_buf($id) [string range $::redis::mux_resp_buf($id) [expr {$idx + 1}] end]
            # Strip trailing \r if present (like Tcl gets does)
            set line [string trimright $line "\r"]
            return $line
        }
        # Need more data
        mux_read_resp_data $id $fd 1
    }
}

# Read a line (terminated by \r\n) from MUX RESP buffer
proc ::redis::mux_buf_gets {id fd} {
    while {1} {
        set idx [string first "\r\n" $::redis::mux_resp_buf($id)]
        if {$idx >= 0} {
            set line [string range $::redis::mux_resp_buf($id) 0 [expr {$idx - 1}]]
            set ::redis::mux_resp_buf($id) [string range $::redis::mux_resp_buf($id) [expr {$idx + 2}] end]
            return $line
        }
        # Need more data
        mux_read_resp_data $id $fd 1
    }
}

proc ::redis::mux_buf_readnl {id fd len} {
    set buf [mux_buf_read $id $fd $len]
    mux_buf_read $id $fd 2 ;# discard \r\n
    return $buf
}

proc ::redis::mux_buf_bulk_read {id fd} {
    set count [string trim [mux_buf_gets $id $fd]]
    if {$count == -1} return {}
    set buf [mux_buf_readnl $id $fd $count]
    return $buf
}

proc ::redis::redis_mux_read_reply_logic {id fd} {
    if {$::redis::readraw($id)} {
        return [string trim [mux_buf_gets_lf $id $fd]]
    }
    while {1} {
        set type [mux_buf_read $id $fd 1]
        switch -exact -- $type {
            _ {
                mux_buf_gets $id $fd
                return {}
            }
            : - ( - + {
                return [string trim [mux_buf_gets $id $fd]]
            }
            , {
                set v [string trim [mux_buf_gets $id $fd]]
                if {[should_transform_to_resp2 $id]} {
                    return $v
                } else {
                    return [expr {double($v)}]
                }
            }
            \# {
                set v [string trim [mux_buf_gets $id $fd]]
                if {$v == "t"} {return 1}
                if {$v == "f"} {return 0}
                return -code error "Bad protocol, '$v' as bool type"
            }
            = {
                set v [mux_buf_bulk_read $id $fd]
                return [string range $v 4 end]
            }
            - {
                return -code error [string trim [mux_buf_gets $id $fd]]
            }
            \$ {
                return [mux_buf_bulk_read $id $fd]
            }
            > - ~ - * {
                set count [string trim [mux_buf_gets $id $fd]]
                if {$count == -1} return {}
                set l {}
                set err {}
                for {set i 0} {$i < $count} {incr i} {
                    if {[catch {
                        lappend l [redis_mux_read_reply_logic $id $fd]
                    } e] && $err eq {}} {
                        set err $e
                    }
                }
                if {$err ne {}} {return -code error $err}
                return $l
            }
            % {
                set count [string trim [mux_buf_gets $id $fd]]
                if {$count == -1} return {}
                set d {}
                set err {}
                for {set i 0} {$i < $count} {incr i} {
                    if {[catch {
                        set k [redis_mux_read_reply_logic $id $fd]
                        set v [redis_mux_read_reply_logic $id $fd]
                        dict set d $k $v
                    } e] && $err eq {}} {
                        set err $e
                    }
                }
                if {$err ne {}} {return -code error $err}
                return $d
            }
            | {
                set count [string trim [mux_buf_gets $id $fd]]
                if {$count == -1} continue
                set d {}
                for {set i 0} {$i < $count} {incr i} {
                    set k [redis_mux_read_reply_logic $id $fd]
                    set v [redis_mux_read_reply_logic $id $fd]
                    dict set d $k $v
                }
                set ::redis::attributes($id) $d
                continue
            }
            default {
                if {$type eq {}} {
                    return -code error "I/O error reading reply"
                }
                return -code error "Bad protocol, '$type' as reply type byte"
            }
        }
    }
}

proc ::redis::redis_reset_state id {
    set ::redis::state($id) [dict create buf {} mbulk -1 bulk -1 reply {}]
    set ::redis::statestack($id) {}
}

proc ::redis::redis_call_callback {id type reply} {
    set cb [lindex $::redis::callback($id) 0]
    set ::redis::callback($id) [lrange $::redis::callback($id) 1 end]
    uplevel #0 $cb [list ::redis::redisHandle$id $type $reply]
    ::redis::redis_reset_state $id
}

# Read a reply in non-blocking mode.
proc ::redis::redis_readable {fd id} {
    if {[eof $fd]} {
        redis_call_callback $id eof {}
        ::redis::__method__close $id $fd
        return
    }
    if {[dict get $::redis::state($id) bulk] == -1} {
        set line [gets $fd]
        if {$line eq {}} return ;# No complete line available, return
        switch -exact -- [string index $line 0] {
            : -
            + {redis_call_callback $id reply [string range $line 1 end-1]}
            - {redis_call_callback $id err [string range $line 1 end-1]}
            ( {redis_call_callback $id reply [string range $line 1 end-1]}
            $ {
                dict set ::redis::state($id) bulk \
                    [expr [string range $line 1 end-1]+2]
                if {[dict get $::redis::state($id) bulk] == 1} {
                    # We got a $-1, hack the state to play well with this.
                    dict set ::redis::state($id) bulk 2
                    dict set ::redis::state($id) buf "\r\n"
                    ::redis::redis_readable $fd $id
                }
            }
            * {
                dict set ::redis::state($id) mbulk [string range $line 1 end-1]
                # Handle *-1
                if {[dict get $::redis::state($id) mbulk] == -1} {
                    redis_call_callback $id reply {}
                }
            }
            default {
                redis_call_callback $id err \
                    "Bad protocol, $type as reply type byte"
            }
        }
    } else {
        set totlen [dict get $::redis::state($id) bulk]
        set buflen [string length [dict get $::redis::state($id) buf]]
        set toread [expr {$totlen-$buflen}]
        set data [read $fd $toread]
        set nread [string length $data]
        dict append ::redis::state($id) buf $data
        # Check if we read a complete bulk reply
        if {[string length [dict get $::redis::state($id) buf]] ==
            [dict get $::redis::state($id) bulk]} {
            if {[dict get $::redis::state($id) mbulk] == -1} {
                redis_call_callback $id reply \
                    [string range [dict get $::redis::state($id) buf] 0 end-2]
            } else {
                dict with ::redis::state($id) {
                    lappend reply [string range $buf 0 end-2]
                    incr mbulk -1
                    set bulk -1
                }
                if {[dict get $::redis::state($id) mbulk] == 0} {
                    redis_call_callback $id reply \
                        [dict get $::redis::state($id) reply]
                }
            }
        }
    }
}

# when forcing resp3 some tests that rely on resp2 can fail, so we have to translate the resp3 response to resp2
proc ::redis::should_transform_to_resp2 {id} {
    if {$::redis::testing_resp3($id)} {
        return 0
    }
    # MUX virtual clients use RESP2 protocol on the server side,
    # so the RESP data in MUX frames is already in RESP2 format.
    # No transformation is needed for MUX mode.
    if {$::force_resp3} {
        return 1
    }
    return 0
}
