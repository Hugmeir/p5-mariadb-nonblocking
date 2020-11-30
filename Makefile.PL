use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

use File::Spec;
use Config       qw/%Config/;
use Getopt::Long qw/GetOptions/;
use Alien::libmariadbclient;
use Text::ParseWords qw(shellwords);

my $opt = {};
GetOptions(
    $opt,
    "static_link_to_mariadbclient",
    "mariadb_path=s",
    "testdb=s",
    "testhost=s",
    "testport=s",
    "testuser=s",
    "testpassword=s",
    "testsocket=s",
);
my $mariadb_path = $opt->{mariadb_path};

sub write_config_for_tests {
    my $opt = $_[0];
    my @opts_from_file = eval { require "t/mysql.mtest" };
    @opts_from_file    = eval { require "../t/mysql.mtest" }
        if @opts_from_file;

    my @ordered_options = qw/
        testhost
        testport
        testuser
        testsocket
        testpassword
        testdb
        force-embedded
        mysql_config
    /;

    for (0..$#ordered_options) {
        my $opt_name        = $ordered_options[$_];
        my $value           = $opts_from_file[$_];
        $opt->{$opt_name} ||= $value || '';
    }

    my $test_lib = File::Spec->catfile("t", "lib.pl");
    my $source_to_print = <<"EOPERL";
\$::test_host     = q\0$opt->{testhost}\0;
\$::test_port     = q\0$opt->{testport}\0;
\$::test_user     = q\0$opt->{testuser}\0 || \$ENV{'DBI_USER'} || '';
\$::test_socket   = q\0$opt->{testsocket}\0;
\$::test_password = q\0$opt->{testpassword}\0 || \$ENV{'DBI_PASS'} || '';
\$::test_db       = q\0$opt->{testdb}\0;
\$::test_force_embedded = 0+q\0$opt->{'force-embedded'}\0 if 0+q\0$opt->{'force-embedded'}\0;
\$::test_mysql_config = q\0$opt->{mysql_config}\0;

1;
EOPERL

    open my $fh, '>', $test_lib;
    print $fh $source_to_print;
    close $fh || die "Failed to create $test_lib: $!";
}

write_config_for_tests($opt);

$opt->{static_link_to_mariadbclient} //= 1;

my ($ccflags, $lddlflags) = map $_//'', @Config{qw/ccflags lddlflags/};

my ($libs, $cflags);
if ( $opt->{static_link_to_mariadbclient} ) {
    $libs   = Alien::libmariadbclient->libs_static   || Alien::libmariadbclient->libs;
    $cflags = Alien::libmariadbclient->cflags_static || Alien::libmariadbclient->cflags;
    $libs =~ s/\B-lmariadb\b/-lmariadbclient/g; # static library has a different name!
}
else {
    $libs   = Alien::libmariadbclient->libs;
    $cflags = Alien::libmariadbclient->cflags;
}

# Work around a cmake/OSX bug; -lz gets transformed into
#   -l/path/to/libz.tbd
# on systems that have Apple's "built-in dynamic linker cache";
# so transform it into just a full path.
$libs =~ s<\B-l/></>g;

my $makemaker_libs = [];
my $makemaker_inc  = '';



my $static_libmariadb;
foreach my $maybe_lib ( shellwords($libs) ) {
    if ( $maybe_lib =~ m/\B-l(\S+)/ ) {
        # -lfoo
        push @$makemaker_libs, $maybe_lib;
    }
    elsif ( $maybe_lib =~ m<-L(\S+)> ) {
        my $lib_dir = $1;
        # /path/to/libs
        # add '-Wl,-rpath=$dir'
        $libs = '-Wl,-rpath,' . $lib_dir . ' ' . $libs;
        my $libmariadb = File::Spec->catfile($lib_dir, 'libmariadbclient' . $Config{lib_ext});
        $libs .= ' ' .$libmariadb if -e $libmariadb;
    }
}
$lddlflags .= ' ' . $libs;

foreach my $maybe_inc ( shellwords($cflags) ) {
    next unless $maybe_inc =~ m/\B-I(\S+)/;
    my $inc = $1;
    $makemaker_inc .= $inc;
}
$ccflags .= ' ' . $cflags;

WriteMakefile(
    NAME             => 'MariaDB::NonBlocking',
    AUTHOR           => q{Brian Fraser <fraserbn@gmail.com>},
    VERSION_FROM     => 'lib/MariaDB/NonBlocking.pm',
    ABSTRACT_FROM    => 'lib/MariaDB/NonBlocking.pm',
    LICENSE          => 'artistic_2',
    PL_FILES         => {},
    MIN_PERL_VERSION => '5.006',
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => '0',
        'Alien::libmariadbclient' => '0.01',
    },
    BUILD_REQUIRES => {
        'Test::More' => '0',
    },
    PREREQ_PM => {
        #'ABC'              => '1.6',
        #'Foo::Bar::Module' => '5.0401',
    },
    LIBS    => $makemaker_libs,
    INC     => $makemaker_inc,
    CCFLAGS   => $ccflags,
    LDDLFLAGS => $lddlflags,
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => [
        'MariaDB-NonBlocking-*',
        't/lib.pl',
        'deps/mariadb-connector-c-3.1.9-src',
    ] },
);
