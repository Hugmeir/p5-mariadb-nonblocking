package MariaDB::NonBlocking::Event;
use v5.18.2; # needed for __SUB__, implies strict
use warnings;

use AnyEvent ();

# is anyevent using EV? If so, cut the middleman and
# use EV directly, since we can reuse/reset watchers,
# as well as use IO watchers for both read and write
# polling.
# EV lets us cut down the number of watchers created
# per connection significantly.
AnyEvent::post_detect {
    my $IS_EV = ($AnyEvent::MODEL//'') eq 'AnyEvent::Impl::EV' ? 1 : 0;
    if ( $IS_EV && 0 ) {
        require MariaDB::NonBlocking::EV;
        our @ISA = 'MariaDB::NonBlocking::EV';
    }
    else {
        require MariaDB::NonBlocking::AE;
        our @ISA = 'MariaDB::NonBlocking::AE';
    }
};

1;
