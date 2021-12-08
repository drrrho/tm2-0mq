use 5.006;
use strict;
use warnings;
use Module::Build;

$ENV{'PERL_MB_OPT'} = '--install-base=/usr';
$ENV{'DEB_BUILD_OPTIONS'}='nocheck';

use Module::Build;
my $debian = Module::Build->subclass(
    class => "Module::Build::Debian",
    code => <<'SUBCLASS' );

sub DEBVERSION {
    return 1;
}

sub ACTION_debian {
    my $self = shift;

    $self->depends_on("dist");
    $self->depends_on("distdir");
#   $self->depends_on("disttest");

    my $version = $self->dist_version;
    my $debversion = $self->DEBVERSION;
    $self->do_system(qq{ DEB_BUILD_OPTIONS=nocheck  dh-make-perl --build --version=$version-$debversion --no-network TM2-0MQ-$version } );
}

sub ACTION_inject {
    my $self = shift;

    $self->depends_on("debian");

    my $version = $self->dist_version;
    my $debversion = $self->DEBVERSION;
    $self->do_system(qq{ scp libtm2-0mq-perl_${version}-${debversion}_all.deb packages.devc.at:packages/ } );
    foreach (qw(squeeze wheezy jessie stretch) ) {
        $self->do_system(qq{ ssh packages.devc.at reprepro -b /var/www/packages/data/dists/ includedeb $_ packages/libtm2-0mq-perl_${version}-${debversion}_all.deb });
    }
}
SUBCLASS

my $builder = $debian->new(
    module_name         => 'TM2::0MQ',
    license             => 'artistic_2',
    dist_author         => q{Robert Barta <rho@devc.at>},
    dist_version_from   => 'lib/TM2/0MQ.pm',
    release_status      => 'stable',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More'        => 0,
        'Test::Exception'   => 0,
        'Test::Deep'        => 0,
    },
    requires => {
	'ZMQ::FFI'          => 0,
	'IO::Async'         => 0,
	'TM2::TempleScript' => 0,
    },
    onto_files          => {
        map { $_ => $_ }
        map { "ontologies/$_" }
        qw(
zmq.atm
zmq.ts
        )
    },
    install_path        => { 'ontologies' => '/usr/share/templescript/ontologies' },
    add_to_cleanup     => [ 'TM2::0MQ-*' ],
    create_makefile_pl => 'traditional',
);

$builder->add_build_element('onto');
$builder->create_build_script();
