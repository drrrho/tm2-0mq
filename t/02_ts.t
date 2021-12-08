use strict;
use Data::Dumper;
use Test::More;
use Test::Exception;
use Test::Deep;

use TM2;

use constant DONE   => 1;
use constant STRESS => 0;

my $warn = shift @ARGV;
unless ($warn) {
    close STDERR;
    open (STDERR, ">/dev/null");
    select (STDERR); $| = 1;
}

use TM2;
use Log::Log4perl::Level;
$TM2::log->level($warn ? $DEBUG : $ERROR); # one of DEBUG, INFO, WARN, ERROR, FATAL

use lib '../tm2_dbi/lib';
use lib '../templescript/lib';

#== TESTS ========================================================================

require_ok( 'TM2::TS::Stream::zeromq' );

my $file     = "zmq-ffi-$$";
my $endpoint = "ipc://$file";

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
my $zmq_ctx      = $TM2::TS::Stream::zeromq::CTX; # ZMQ::FFI->new();

if (DONE) {
    my $AGENDA = q{sending via stream: };

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    use Net::Async::0MQ::Socket;
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => ZMQ_REP,
	context  => $zmq_ctx,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
#warn "hb received ".Dumper \@c;
	    is_deeply (\@c, [ 'aaa', 'bbb' ], $AGENDA.'received');
	}
	);
    $loop->add( $hb );

    my $tail = [];
    my $this = [];

    my $req = $zmq_ctx->socket(ZMQ_REQ);
    $req->connect( $endpoint );

    use TM2::TS::Stream::perlcode;
    tie @$this, 'TM2::TS::Stream::perlcode', [ ], sub {
	my ($s, @t) = @_;
	$s->send_multipart( \@t );
	return \@t;
    }, { tuple => 1 }, $tail;

    $loop->watch_time( after => 1, code => sub {
	push @$this, [ $req, 'aaa', 'bbb' ];
        } );

    $loop->watch_time( after => 4, code => sub {
        $loop->stop; } ); diag ("collapsing in 4 secs") if $warn;
    $loop->run;

    is_deeply ($tail, [ [ 'aaa', 'bbb' ] ], $AGENDA.'push thru');
#warn Dumper $tail;

}

if (DONE) {
    my $AGENDA = q{socket errors: };

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    use TM2::TS::Stream::zeromq;
    {
	my $cc = TM2::TS::Stream::zeromq::factory->new (loop => $loop, uri => "0mqqq-$endpoint;type=ROUTER");

	my $t = [];
	my $c = $cc->prime ($t);
	throws_ok {
	    push @$c, bless [], 'ts:kickoff';
	} qr/endpoint/, $AGENDA.'wrong endpoint'
    }
    {
	my $cc = TM2::TS::Stream::zeromq::factory->new (loop => $loop, uri => "0mq-$endpoint;type=ROUTERXXX");

	my $t = [];
	my $c = $cc->prime ($t);
	throws_ok {
	    push @$c, bless [], 'ts:kickoff';
	} qr/type/, $AGENDA.'wrong type'
    }
}

if (DONE) {
    my $AGENDA = q{simple router socket ping: };

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    use TM2::TS::Stream::zeromq;
    my $cc = TM2::TS::Stream::zeromq::factory->new (loop => $loop, uri => "0mq-$endpoint;type=ROUTER");

    my $t = [];
    my $c = $cc->prime ($t);
    push @$c, bless [], 'ts:kickoff';

    my $req = $zmq_ctx->socket(ZMQ_DEALER);
    $req->connect( $endpoint );

    use IO::Async::Timer::Periodic;
    my $timer = IO::Async::Timer::Periodic->new(
	interval => 3,
	on_tick => sub {
	    $req->send_multipart( [ qw[aaa 3 http://www.com ] ] );
	},
	);
    $timer->start;
    $loop->add( $timer );


    $loop->watch_time( after => 14, code => sub {
        push @$c, bless [], 'ts:collapse'; 
        $loop->stop; } ); diag ("collapsing in 14 secs") if $warn;
    $loop->run;

    $timer->stop; $loop->remove( $timer );

    is ((scalar @$t), 4, $AGENDA.'nr ticks');
    map { is( (scalar @$_), 5, $AGENDA.'tuple length') } @$t;
    map { isa_ok(    $_->[0], 'ZMQ::FFI::ZMQ3::Socket',              $AGENDA.'socket' )}  @$t;
    map { is_deeply( $_->[2], TM2::Literal->new( 'aaa' ),            $AGENDA.'string' )}  @$t;
    map { is_deeply( $_->[3], TM2::Literal->new( 3 ),                $AGENDA.'integer' )} @$t;
    map { is_deeply( $_->[4], TM2::Literal->new( 'http://www.com' ), $AGENDA.'uri' )} @$t;
#warn Dumper $t;
}

if (DONE) {
    my $AGENDA = q{receiving & sending via stream: };

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    use Net::Async::0MQ::Socket;
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => ZMQ_REP,
	context  => $zmq_ctx,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
#warn "hb received ".Dumper \@c;
	    is_deeply (\@c, [ 'aaa', 'bbb' ], $AGENDA.'received');
	}
	);
    $loop->add( $hb );

    my $tail = [];
    my $this = [];



    use TM2::TS::Stream::zeromq;
    my $zz = TM2::TS::Stream::zeromq::factory->new (loop => $loop, uri => "0mq-$endpoint;type=REP");

    my $tail = [];

    my $back = [];
    use TM2::TS::Stream::perlcode;
    tie @$back, 'TM2::TS::Stream::perlcode', [ ], sub {
	my ($s, @t) = @_;
#warn "sending back to $s".Dumper \@t;
	map { $TM2::log->logdie( "cannot process non TM2::Literals for serialization" ) 
		  unless ref($_) eq 'TM2::Literal' } 
	    @t;
	$s->send_multipart( [ map { $_->[0] } @t ] );
	return \@t;
    }, { tuple => 1 }, $tail;

    my $hb= $zz->prime ($back);

    push @$hb, bless [], 'ts:kickoff'; # wake it up


    my $req = $zmq_ctx->socket(ZMQ_REQ);
    $req->connect( $endpoint );

    use IO::Async::Timer::Periodic;
    my $sender = IO::Async::Timer::Periodic->new(
	interval => 3,
	on_tick => sub {
	    $req->send_multipart( [ qw[aaa] ] );
	},
	);
    $sender->start;
    $loop->add( $sender );

    my $back_ctr = 0;
    my $receiver = IO::Async::Timer::Periodic->new(
	interval => 1,
	on_tick => sub {
#warn "testing recv";
	    if ( $req->has_pollin ) {
		my @c = $req->recv_multipart( );
		is_deeply( \@c, [ 'aaa' ], $AGENDA.'serialized sent back');
		$back_ctr++;
#warn "got back ".Dumper \@c;
	    }
	},
	);
    $receiver->start;
    $loop->add( $receiver );

    $loop->watch_time( after => 14, code => sub {
        push @$hb, bless [], 'ts:collapse'; 
        $loop->stop; } ); diag ("collapsing in 14 secs") if $warn;
    $loop->run;

    $sender->stop;   $loop->remove( $sender );
    $receiver->stop; $loop->remove( $receiver );

    is ($back_ctr, 4, $AGENDA.'all sent back');

    is ((scalar @$tail), 4, $AGENDA.'all sent forward');
}

done_testing();

__END__

