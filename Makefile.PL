use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

use File::Spec;
use Config       qw/%Config/;
use IPC::Cmd     qw/can_run run/;
use Getopt::Long qw/GetOptions/;

sub get_paths_out_of_config_and_split {
    my @paths;
    foreach my $config_key ( @_ ) {
        next unless defined($Config{$config_key})
                 && length($Config{$config_key});
        # TODO: Embedded spaces???
        push @paths, split /\s+/, $Config{$config_key};
    }
    return grep defined, @paths;
}

{
    my @libpaths = get_paths_out_of_config_and_split(qw/
                        libpth
                        libpath
                        libspath
                        loclibpth
                        xlibpth
                        glibpth
                        glibpath
                    /);

    sub find_libpath_for { grep defined, map find_file_in($_, @libpaths), @_ }
}

{
    my @incpaths = get_paths_out_of_config_and_split(qw/
                        incpath
                        incpth
                        locincpth
                        locincpath
                        usrinc
                    /);
    sub find_incpath_for { grep defined, map find_file_in($_, @incpaths), @_ }
}

sub find_file_in {
    my ($file, @dirs) = @_;

    foreach my $dir ( @dirs ) {
        my @try_in = (
            $dir,
            map scalar File::Spec->catdir($dir, $_), qw/mysql mysqldb maria mariadb/,
        );

        for my $path (@try_in) {
	        my $full_path = File::Spec->catfile($path, $file);
	        if (-f $full_path) {
	            return $path;
	        }
        }
    }

    return;
}


sub run_grab_output {
    my ($cmd) = @_;
    my( undef, undef, $full_buf, $stdout_buf, $stderr_buf )
        = run( command => $cmd, verbose => 0 );

    my $out = @{$stdout_buf // []}[0] // '';

    chomp($out);

    return $out;
}

my @wanted_libs = qw( ssl crypto iconv  );

GetOptions(
    'static_link_to_mariadbclient' => \my $statically_link_mariadbclient,
);

my ($libs, $inc, $ccflags, $lddlflags) = map $_//'',@Config{qw/libs inc ccflags lddlflags/};
if ( $statically_link_mariadbclient ) {
    my @needed_libs = qw( mariadbclient );

    my $ext = $Config{lib_ext};
    my @libfilenames = map {
        ( "lib${_}${ext}", "${_}${ext}" )
    } @needed_libs;

    my @libpaths = find_libpath_for(@libfilenames);

    LIB:
    foreach my $path (@libpaths) {
        PATH:
        foreach my $lib (@libfilenames) {
            my $file = File::Spec->catfile($path, $lib);
            next unless -f $file;
            $lddlflags .= " $file";
            next LIB;
        }
    }
}
else {
    push @wanted_libs, 'mariadbclient';
    if ( can_run('mariadb_config') ) {
        # woo!
        $libs    .= ' ' . run_grab_output([qw/mariadb_config --libs_r/]);
        $inc     .= ' ' . run_grab_output([qw/mariadb_config --include/]);
        $ccflags .= ' ' . run_grab_output([qw/mariadb_config --cflags/]);
    }
}

LIB:
foreach my $wanted_lib (@wanted_libs) {
    my @config_entries = qw/dlext so/;

    my @libs_as_paths = map +( $_, "lib$_" ),
                        map "${wanted_lib}$_",
                        map +(/\A\./ ? $_ : ".$_"),
                        grep +(defined($_) && length($_)),
                          @Config{@config_entries};

    foreach my $lib ( @libs_as_paths ) {
        my ($libpath) = find_libpath_for($lib);
        next unless $libpath;

        $ccflags .= ' -L'.$libpath;
        $libs    .= ' -l'.$wanted_lib;

        next LIB;
    }
}

$inc .= ' ' . join " ", map '-I'.$_, find_incpath_for('mysql.h');

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
    },
    BUILD_REQUIRES => {
        'Test::More' => '0',
    },
    PREREQ_PM => {
        #'ABC'              => '1.6',
        #'Foo::Bar::Module' => '5.0401',
    },
    LIBS    => $libs,
    INC     => $inc,
    CCFLAGS   => $ccflags,
    LDDLFLAGS => $lddlflags,
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'MariaDB-NonBlocking-*' },
);
