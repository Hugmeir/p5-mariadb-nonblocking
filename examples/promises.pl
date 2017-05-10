use v5.18.1;
use warnings;

use blib;
use MariaDB::NonBlocking;

use EV;
use AnyEvent;
use Promises qw/collect deferred/;

use Data::Dumper;

sub _decide_what_watchers_we_need {
    my ($status) = @_;

    my $wait_on = 0;
    $wait_on |= EV::READ  if $status & MariaDB::NonBlocking::MYSQL_WAIT_READ;
    $wait_on |= EV::WRITE if $status & MariaDB::NonBlocking::MYSQL_WAIT_WRITE;

    return $wait_on;
}

sub ev_event_to_mysql_event {
    return MariaDB::NonBlocking::MYSQL_WAIT_TIMEOUT
        if $_[0] & EV::TIMER;

    my $events = 0;
    $events |= MariaDB::NonBlocking::MYSQL_WAIT_READ  if $_[0] & EV::READ;
    $events |= MariaDB::NonBlocking::MYSQL_WAIT_WRITE if $_[0] & EV::WRITE;

    return $events;
}

sub run_queries_promise {
    my @queries  = @_;
    my $deferred = deferred;
    my $promise  = $deferred->promise;

    my @connections = map MariaDB::NonBlocking->connect(
                          {
                              host         => 'localhost',
                              user         => 'root',
                              port         => 0,
                              password     => "",
                              database     => undef,
                              mysql_socket => undef,
                          }
                      ), 1..3;

    my (@query_results, @errors); # array of arrayrefs
    my $cv = AnyEvent->condvar;
    $cv->begin(sub {
        if ( @errors ) {
            $deferred->reject(@errors);
        }
        else {
            $deferred->resolve(@query_results)
        }
    });
    foreach my $maria ( @connections ) {
        last if !@queries; # Huh.
        $cv->begin; # Increase the counter in the condvar

        # We need to either wait for reads or writes.  Currently assuming
        # all wait are going to be for reading

        my $socket_fd = $maria->mysql_socket_fd;

        my $run_query = sub {
            return unless @queries;
            my $query = pop @queries;
            my $wait_for;
            local $@;
            eval {
                $wait_for = $maria->run_query_start($query);
                1;
            } or do {
                my $e = $@ || 'zombie error';
                push @errors, $e;
            };

            return if @errors;

            if ( !$wait_for ) {
                push @query_results, $maria->query_results;
                goto &{ __SUB__() }; # tail call optimization
            }

            return $wait_for;
        };

        my %watchers;
        my $ev_mask;
        my $cb = sub {
            my (undef, $ev_event) = @_;

            delete $watchers{timer}; # Always release the timer.

            my $events_for_mysql = ev_event_to_mysql_event($ev_event);

            my $wait_for;
            local $@;
            eval {
                $wait_for = $maria->run_query_cont($events_for_mysql);
                1;
            } or do {
                my $e = $@ || 'zombie error';
                push @errors, $e;
            };

            if ( !$wait_for && !@errors ) { # query we were waiting on finished!
                do {
                    # Get the results
                    push @query_results, $maria->query_results;

                    # And schedule another!
                    $wait_for = $run_query->();
                    # Loop will keep going until we either run a query
                    # we need to block on, in which case $wait_for will
                    # be true, or we exhaust all @queries, in which case
                    # joy to the world.
                } while (!$wait_for && @queries && !@errors);
            }

            # If we still don't need to wait for anything, that
            # means we are done with all queries for this dbh,
            # so decrease the condvar counter
            if ( !$wait_for || @errors ) {
                undef %watchers; # BOI!!
                $cv->end;
            }
            else {
                my $new_ev_mask = _decide_what_watchers_we_need($wait_for);
                if ( $new_ev_mask != $ev_mask ) {
                    # Server wants us to wait on something else, so
                    # we can't reuse the previous watcher.
                    # e.g. we had a watcher waiting on the socket
                    # being readable, but we need to wait for it to
                    # become writeable (or both) instead.
                    # This almost never happens.
                    delete $watchers{io};
                    say "new mask!!";
                    $ev_mask = $new_ev_mask;
                    $watchers{io} = EV::io(
                                        $socket_fd,
                                        $ev_mask,
                                        __SUB__,
                                      );
                }

                if ( $wait_for & MariaDB::NonBlocking::MYSQL_WAIT_TIMEOUT ) {
                    my $time_in_seconds = $maria->get_timeout_value_ms() * 1000;
                    # Bug in the client lib makes the no-timeout case come
                    # back as 0 timeout.  So only create the timer if we
                    # actually have a timeout.
                    # https://lists.launchpad.net/maria-developers/msg09971.html
                    $watchers{timer} = EV::timer(
                                            $time_in_seconds,
                                            0, # do not repeat
                                            __SUB__,
                                       ) if $time_in_seconds;
                }
            }
            return;
        };
    
        my $wait_for = $run_query->();

        if ( !$wait_for ) {
            $cv->end; # Did not have to wait for anything!
            next;
        }

        $ev_mask = _decide_what_watchers_we_need($wait_for);
        $watchers{io} = EV::io(
            $socket_fd,
            $ev_mask,
            $cb,
        );
    }
    $cv->end;
    return $promise;
}

my @queries = (
    q{SELECT REPEAT("pity da foo", 5634), RAND()*100}
) x 6;
#    $queries[3] = 'SELECT REPEAT(';

my $promise = run_queries_promise(@queries);

my $cv = AnyEvent->condvar;
say "Going to wait on the promise!";
$promise->then(
    sub {
        say "Done!";
        say Dumper([map $_->[0][1], @_]);
        $cv->send();
    },
    sub {
        say "Reject!";
        say Dumper(\@_);
        $cv->send;
    },
);
$cv->recv();

