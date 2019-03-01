package MariaDB::NonBlocking::AE;
use parent 'MariaDB::NonBlocking';

use v5.18.2; # needed for __SUB__, implies strict
use warnings;
use Sub::StrictDecl;

use constant DEBUG => $ENV{MariaDB_NonBlocking_DEBUG} // 0;
sub TELL (@) {
    say STDERR __PACKAGE__, ': ', join " ", @_;
}

use Carp (); # for confess

# Better to import this, since it is a custom op
use Ref::Util qw(is_ref is_arrayref is_coderef);
use Scalar::Util qw(weaken);

use AE;

use MariaDB::NonBlocking ':all';

sub new {
    my $class = shift;
    my $obj = $class->init;
    return $obj;
}

sub __clean_object {
    delete $maria->{previous_wait_for};
    delete $maria->{pending};
    delete $maria->{watcher_storage};
}

sub __set_timer {
    my ($storage, $watcher_type, $timeout_s, $cb) = @_;
    AE::now_update();
    $storage->{$watcher_type} = AE::timer(
        after    => $timeout_s,
        interval => 0,
        cb       => sub { $cb->(MYSQL_WAIT_TIMEOUT) },
    );
}

sub __set_io_watcher {
    my ($storage, $fd, $wait_for, $cb) = @_;

    # We might need a read watcher, we might need
    # a write watcher.. we might need both : (

    # drop any previous watchers
    delete @{$storage}{qw/io_r io_w/};

    # amusingly, this is broken in libuv, since
    # you cannot have two watchers on the same fd;
    DEBUG && TELL "Started new $watcher_type watcher ($wait_for)";
    $storage->{io_r} = AE::io(
        fh   => $fd,
        poll => "r",
        cb   => sub { $cb->(MYSQL_WAIT_READ) }
    ) if $wait_for & MYSQL_WAIT_READ;
    $storage->{io_w} = AE::io(
        fh   => $fd,
        poll => "w",
        cb   => sub { $cb->(MYSQL_WAIT_WRITE) }
    ) if $wait_for & MYSQL_WAIT_WRITE;
    return;
}

sub ____run {
    my (
        $outside_maria,
        $start_work_cb,
        $grab_results_cb,
        $extras,
    ) = @_;

    $extras //= {};

    my $perl_timeout      = $extras->{perl_timeout};
    my $success_cb_orig   = $extras->{success_cb};
    my $failure_cb_orig   = $extras->{failure_cb};

    if ( !is_coderef($success_cb_orig) ) {
        Carp::confess(ref($outside_maria) . " was not given a coderef to success_cb");
    }

    if ( !is_coderef($failure_cb_orig) ) {
        Carp::confess(ref($outside_maria) . " was not given a coderef to failure_cb");
    }

    # $maria is weakened here, as otherwise we would
    # have this cycle:
    # $maria->{watchers}{any}{cb} => sub { ...; $maria; ...; }
    my $maria = $outside_maria; # DO NOT USE $outside_maria AFTER THIS LINE
    weaken($maria);

    my $success_cb = sub {
        __clean_object($maria) if $maria;
        goto &$success_cb_orig;
    };
    my $failure_cb = sub {
        __clean_object($maria) if $maria;
        goto &$failure_cb_orig;
    };

    $maria->{previous_wait_for} = 0;

    my $error;
    my @per_query_results;
    my $watcher_ready_cb = sub { eval {
        return 1 if $error; # Something previously went wrong, we should not be back here!
        die "Connection object went away" unless $maria;

        my ($events_for_mysql) = @_;

        # Always stop the timer
        delete $maria->{watcher_storage}{timer};

        my $wait_for = $maria->{previous_wait_for}
                     ? $maria->cont($events_for_mysql)
                     : $start_work_cb->($maria)
        ;

        # If we still don't need to wait for anything, that
        # means we are done with the query! Grab the results.
        if ( !$wait_for ) {
            # query we were waiting on finished!
            # Get the results
            push @per_query_results, $grab_results_cb->($maria);

            # Ran all the queries! We can resolve and go home
            $success_cb->(@per_query_results);
            return 1;
        }

        if ( $wait_for & MYSQL_WAIT_TIMEOUT ) {
            # If we get here, a timeout was set for this connection.

            # remove for the next check if()
            $wait_for &= ~MYSQL_WAIT_TIMEOUT;
=begin
# Implemented via the global timeout

            # A timeout was specified with the connection.
            # This will call this same callback;
            # query_cont will eventually call
            # the relevant _cont method with MYSQL_WAIT_TIMEOUT,
            # and let the driver decide what to do next.
            my $timeout_ms = $maria->get_timeout_value_ms();
            __grab_watcher({
                watcher_type => 'timer',
                storage      => $maria->{watchers},
                watcher_args => [
                    # AE wants (fractional) seconds
                    $timeout_ms/1000,
                    0, # do not repeat
                    __SUB__,
                ],
            # Bug in the client lib makes the no-timeout case come
            # back as 0 timeout.  So only create the timer if we
            # actually have a timeout.
            # https://lists.launchpad.net/maria-developers/msg09971.html
            }) if $timeout_ms;
=cut
        }

        if ( $wait_for != $previous_wait_for ) {
            $previous_wait_for = $wait_for;
            # Server wants us to wait on something else, so
            # we can't reuse the previous mask.
            # e.g. we had a watcher waiting on the socket
            # being readable, but we need to wait for it to
            # become writeable (or both) instead.
            # This almost never happens, but we need to
            # support it for SSL renegotiation.
            __set_io_watcher(
                $maria->{watcher_storage} //= {},
                $maria->mysql_socket_fd,
                $wait_for & ~MYSQL_WAIT_TIMEOUT,
                __SUB__,
            );
        }

        return 1;
    } or do {
        return if $error;
        $error = $@ || 'Zombie error';
        $failure_cb->($error);
    }};

    # Start the query/connect/etc
    $watcher_ready_cb->();

    # If either of these is set, we finished that query immediately
    return if $error || @per_query_results;

    __set_timer(
        $maria->{watcher_storage} //= {},
        'global_timer',
        $perl_timeout,
        sub {
            return if $error;
            $error = "Global timeout reached";
            DEBUG && TELL "Global timeout reached";

            $failure_cb->(
                "execution was interrupted by perl, maximum execution time exceeded (timeout=$perl_timeout)"
            );
        },
    ) if $perl_timeout;

    $maria->{pending} = $failure_cb;

    return;
}

sub DESTROY {
    my $self = shift;

    my $pending_num = 0;
    if ( my $reject_cb = delete $self->{pending} ) {
        $pending_num++;
        $reject_cb->("Connection object went away");
    }
    DEBUG && $pending_num && TELL "Had $pending_num operations still running when we were freed";
}

sub run_query {
    my ($conn, $sql_with_args, $extras) = @_;

    $sql_with_args = [ $sql_with_args ] unless is_arrayref($sql_with_args);

    $conn->____run(
        sub { $_[0]->run_query_start( @$sql_with_args ) },
        sub { $_[0]->query_results },
        $extras->{success_cb},
        $exteas->{failure_cb},
        $extras->{perl_timeout} || 0,
    );
}

sub ping {
    my ($conn, $extras) = @_;

    $conn->____run(
        sub { $_[0]->ping_start()  }, # start
        sub { $_[0]->ping_result() }, # end
        $extras->{success_cb},
        $exteas->{failure_cb},
        $extras->{perl_timeout} || 0,
    );
}

sub connect {
    my ($conn, $connect_args, $extras) = @_;

    $conn->____run(
        sub { $_[0]->connect_start($connect_args) }, # start
        sub { $_[0] },                               # end
        $extras->{success_cb},
        $exteas->{failure_cb},
        $extras->{perl_timeout} || 0,
    );
}

1;
