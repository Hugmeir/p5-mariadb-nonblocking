#!perl
use v5.10.1;
use strict;
use warnings;

use blib;
use MariaDB::NonBlocking::Poll;


my $pool = [ map MariaDB::NonBlocking::Poll->init, 1..5 ];
eval {
    MariaDB::NonBlocking::Poll::connect(
        $pool,
        {
            host => "127.0.0.1",
            user => "root",
            password => ""
        },
    );
    1;
} or do {
    my $e = $@;
    warn "error: <$e>";
};

my $res = MariaDB::NonBlocking::Poll::query_once_per_connection(
            $pool,
            q{SELECT 1, CONNECTION_ID(), RAND(50)},
          );
use Data::Dumper; say Dumper($res);
$_->disconnect for @$pool;
undef $pool;

