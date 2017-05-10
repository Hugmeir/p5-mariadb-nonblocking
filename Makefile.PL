use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

use File::Spec;
use Config   qw/%Config/;
use IPC::Cmd qw/can_run run/;

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
    sub find_libpath_for { map find_file_in($_, @libpaths), @_ }
}

{
    my @incpaths = get_paths_out_of_config_and_split(qw/
                        incpath
                        incpth
                        locincpth
                        locincpath
                        usrinc
                    /);
    sub find_incpath_for { map find_file_in($_, @incpaths), @_ }
}

sub find_file_in {
    my ($file, @dirs) = @_;

    foreach my $dir ( @dirs ) {
        my @try_in = (
            $dir,
            scalar File::Spec->catdir($dir, "mysql"),
            scalar File::Spec->catdir($dir, "maria"),
        );
        for my $path (@try_in) {
	        my $full_path = File::Spec->catfile($path, $file);
	        if (-f $full_path) {
	            return $path;
	        }
        }
    }
}


sub run_grab_output {
    my ($cmd) = @_;
    my( undef, undef, $full_buf, $stdout_buf, $stderr_buf )
        = run( command => $cmd, verbose => 0 );

    my $out = @{$stdout_buf // []}[0] // '';

    return $out;
}

my ($libs, $inc, $ccflags);
if ( can_run('mariadb_config') ) {
    # woo!
    $libs    = run_grab_output([qw/mariadb_config --libs_r/]);
    $inc     = run_grab_output([qw/mariadb_config --include/]);
    $ccflags = run_grab_output([qw/mariadb_config --cflags/]);
}

if ( !$ccflags ) {
    my @needed_libs = qw( mariadbclient ssl crypto iconv );
    $libs        .= " " . join " ", map "-l$_", @needed_libs;

    my @config_entries = qw/dlext lib_ext so/;
    my @needed_libs_as_paths = map {
        my $as_lib = "lib$_";
        my @extensions = map +(/\A\./ ? $_ : ".$_"),
                                grep +(defined($_) && length($_)),
                                @Config{@config_entries};
        map +($as_lib . $_), @extensions
    } @needed_libs;

    my @libpaths = find_libpath_for(@needed_libs_as_paths);
    if ( @libpaths ) {
        $ccflags .= ' ' . join " ", map "-L$_", @libpaths;
        $libs = $ccflags . ' ' . $libs;
    }

    $inc .= ' ' . join " ", find_incpath_for('mysql.h');
}

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
    CCFLAGS => $ccflags,
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'MariaDB-NonBlocking-*' },
);
