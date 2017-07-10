#!perl
use v5.10.1;
use strict;
use warnings;

use Devel::Peek;
use Data::Dumper;

use blib;
use Promises qw/collect/;
use Scalar::Util qw/weaken/;
use MariaDB::NonBlocking::Promises;

sub MariaDB::NonBlocking::Promises::DESTROY { say STDERR "$_[0]: OH HAI"; }

my @queries = (
    "SELECT CONNECTION_ID(), RAND(6959)",
    "select coord(), rand(464)",
    "select 1, rand(464)",
);


my @promises;
my @all_connections;
my $extras = {}; # Used to cancel all promises in case of an error
for (1..1) {
    my $init_connections = [ map MariaDB::NonBlocking::Promises->init, 1..2 ];
    say join "\n", @$init_connections;

    # Used during error handling to disconnect everything early
    push @all_connections, $init_connections;

    my $handle_errors = sub {
        # If we already handled an error then we have nothing to do
        return if exists $extras->{cancel};
        $extras->{cancel} = "Error in related promise";

        # Disconnect should not die -- if it does, then
        # that takes precedence to whatever else happened!
        $_->disconnect for map @{$_//[]}, @all_connections;
        @all_connections = ();
        # ^ At this point, the connections for this promise will
        # be freed.

        my $e = $_[0];
        die $e; # rethrow
    };

    push @promises, MariaDB::NonBlocking::Promises::connect(
        $init_connections,
        {
            host     => "127.0.0.1",
            user     => "root",
            password => "",
        },
        $extras,
    )->then(
        sub {
            return if exists $extras->{cancel};

            my ($connections) = @_;

            # returns a promise that will be resolved once the
            # query is run once on each connection
            my $promise
                = MariaDB::NonBlocking::Promises::run_multiple_queries(
                    $connections,
                    [@queries],
                    $extras,
                )->catch($handle_errors);
            return $promise;
        },
        $handle_errors
    );
}

{
    use AnyEvent;
    my $cv = AnyEvent->condvar;
    collect(@promises)->then(
        sub { say "hello! ", Dumper(\@_); $cv->send },
        sub { say "error! @_";            $cv->send },
    );
    $cv->recv;
}
say "promise wait END";

say "END";
