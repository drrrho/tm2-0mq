use strict;
use warnings;

use Test::More;
use Test::Exception;
use Test::Moose;
use TM2::TS::Test;

use Data::Dumper;
$Data::Dumper::Indent = 1;

my $warn = shift @ARGV;
unless ($warn) {
    close STDERR;
    open (STDERR, ">/dev/null");
    select (STDERR); $| = 1;
}

use constant DONE => 1;

use lib '../tm2_dbi/lib/';
use lib '../templescript/lib/';
use lib '../tm2_base/lib/';

use TM2;
use Log::Log4perl::Level;
$TM2::log->level($warn ? $DEBUG : $ERROR); # one of DEBUG, INFO, WARN, ERROR, FATAL

use TM2::TempleScript;

sub _parse {
    my $t = shift;
    use TM2::Materialized::TempleScript;
    my $tm = TM2::Materialized::TempleScript->new (baseuri => 'tm:')
	->extend ('TM2::ObjectAble')
	->establish_storage ('*/*' => {})
	->extend ('TM2::ImplementAble')
	;

    $tm->deserialize ($t);
    return $tm;
}

sub _mk_ctx {
    my $stm = shift;
    return [ { '$_'  => $stm, '$__' => $stm } ];
}

#-- TESTS ----------------------------------------------------------

require_ok( 'TM2::0MQcapable' );

unshift  @TM2::TempleScript::Parser::TS_PATHS, './ontologies/', '../templescript/ontologies/', '../../templescript/ontologies/', '/usr/share/templescript/ontologies/';

# my $UR_PATH = '../templescript/ontologies/';
my $UR_PATH = $warn ? '../templescript/ontologies/' : '/usr/share/templescript/ontologies/';
#my $UR_PATH = 1 ? '../../templescript/ontologies/' : '../templescript/ontologies/';
$TM2::TempleScript::Parser::UR_PATH = $UR_PATH;

use TM2::Materialized::TempleScript;
my $env = TM2::Materialized::TempleScript->new (
    file    => $TM2::TempleScript::Parser::UR_PATH . 'env.ts',                # then the processing map
    baseuri => 'ts:')
	->extend ('TM2::ObjectAble')
	->extend ('TM2::ImplementAble')
    ->sync_in;

my $file     = "zmq-ffi-$$";
my $endpoint = "ipc://$file";

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);


if (DONE) {
    my $AGENDA = q{socket incoming: };

    my $tm = _parse (q{

s isa ts:stream
return
    <~ 0mq-}.$endpoint.q{;type=ROUTER ~> | ( $2, $3, $4 ) |->> ts:tap ($a)

})->extend ('TM2::Executable');

    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $tsx = [];

    $ctx = [ { '$loop'    => $loop,
#	       '$zmq:ctx' => $TM2::TS::Stream::zeromq::CTX,
	       '$a'       => $tsx },
	     @$ctx ];

#    use TM2::Materialized::TempleScript;

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 10, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 11, code => sub { diag "stopping loop" if $warn; $loop->stop; } );

    my $req = $TM2::TS::Stream::zeromq::CTX->socket(ZMQ_DEALER);
    $req->connect( $endpoint );

    use IO::Async::Timer::Periodic;
    my $timer = IO::Async::Timer::Periodic->new(
	interval => 3,
	on_tick => sub {
#warn "tick $req send";
	    $req->send_multipart( [ qw[aaa 3 http://www.com ] ] );
	},
	);
    $timer->start;
    $loop->add( $timer );

    $loop->run;

    is( (scalar @$tsx), 3, $AGENDA.'total received');
    map { is_deeply( $_->[0], TM2::Literal->new( 'aaa' ),            $AGENDA.'string' )}  @$tsx;
    map { is_deeply( $_->[1], TM2::Literal->new( 3 ),                $AGENDA.'integer' )} @$tsx;
    map { is_deeply( $_->[2], TM2::Literal->new( 'http://www.com' ), $AGENDA.'uri' )}     @$tsx;

#warn Dumper $tsx;

    $req->disconnect( $endpoint );
    $timer->stop; $loop->remove( $timer );
    unlink $file;
}




if (DONE) {
    my $AGENDA = q{socket outgoing: };

    my $tm = _parse (q{

%include file:zmq.ts

s isa ts:stream
return
      <+ every 3 secs
   | ( "aaa", 3, "http://www.com" )
   |->> zmq:send ( "}.$endpoint.q{;type=DEALER" )

})->extend ('TM2::Executable');

    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    $ctx = [ { '$loop'    => $loop,
#	       '$zmq:ctx' => $TM2::0MQcapable::CTX,
	     },
	     @$ctx ];

    my $tsx = [];

    use Net::Async::0MQ::Socket;
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => ZMQ_ROUTER,
	context  => $TM2::TS::Stream::zeromq::CTX,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
#warn "hb received ".Dumper \@c;
	    push @$tsx, \@c;
	}
	);
    $loop->add( $hb );

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 10, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 11, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
    $loop->run;

    is( (scalar @$tsx), 3, $AGENDA.'total received');
    map { is_deeply( $_->[1], 'aaa',            $AGENDA.'string' )}  @$tsx;
    map { is_deeply( $_->[2], 3,                $AGENDA.'integer' )} @$tsx;
    map { is_deeply( $_->[3], 'http://www.com', $AGENDA.'uri' )}     @$tsx;

    $loop->remove( $hb );
    unlink $file;
}

if (DONE) {
    my $AGENDA = q{heartbeat: };

    my $tm = _parse (q{

%include file:zmq.ts

s isa ts:stream
return
   <~ 0mq-}.$endpoint.q{;type=REP ~> |->> zmq:send

})->extend ('TM2::Executable');

    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    $ctx = [ { '$loop'    => $loop,
#	       '$zmq:ctx' => $TM2::0MQcapable::CTX,
#	       '$a'       => $tsx 
	     },
	     @$ctx ];

    my $req = $TM2::TS::Stream::zeromq::CTX->socket(ZMQ_REQ);
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

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 11, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 12, code => sub { diag "stopping loop" if $warn; $loop->stop; } );

    $loop->run;

    is( $back_ctr, 3, $AGENDA.'total received');
    # map { is_deeply( $_->[1], 'aaa',            $AGENDA.'string' )}  @$tsx;
    # map { is_deeply( $_->[2], 3,                $AGENDA.'integer' )} @$tsx;
    # map { is_deeply( $_->[3], 'http://www.com', $AGENDA.'uri' )}     @$tsx;

    $receiver->stop; $loop->remove( $receiver );
    $sender->stop;   $loop->remove( $sender );
    unlink $file;
}

done_testing;

__END__

if (0&&DONE) {
    my $AGENDA = q{context stream prefixing: };

    my $tm = _parse (q{

%include file:zmq.ts

s isa ts:stream
return
    ( $zmq:ctx ) |->> ts:tap($a)

})->extend ('TM2::Executable');

    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $tsx = [];
    $ctx = [ { '$loop'    => $loop,
	       '$a'       => $tsx },
	     @$ctx ];

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 5, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 6, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
    $loop->run;

#warn Dumper $tsx;
    is_singleton ($tsx, undef, $AGENDA.'context singleton');
#    isa_ok ($tsx->[0]->[0], 'ZMQ::FFI::Context', $AGENDA.'context type');
    is ($tsx->[0]->[0], $TM2::0MQcapable::CTX, $AGENDA.'context value');
}


if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, rampup: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 3 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 1 ) | -))) s1 (((-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

#    my (undef, $dd) = %{ $tm->object ('templescript/query', 'tm:s1') };
#    my (undef, $s1) = %$dd;
#warn "after parse ".$s1->toString; exit;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    use TM2::Materialized::TempleScript;

    my $ts;
    my $tsx;

    for (1..1) {
        @$tsx = ();
	$ctx = [ { '$loop' => $loop, '$a'    => $tsx }, @$ctx ];
        my $cpr = $tm->main_stream ($ctx);
        {
#warn $cpr->toString;
            my $s = TM2::TempleScript::PE::cpr2stream ({}, {}, $ctx, $cpr->decanonicalize ($ctx));

            $ts = TM2::TS::Stream::last3 ($s);
# #warn "last final ts $ts";
# #warn "    ".tied @$ts;
            push @$s, bless [], 'ts:kickoff';
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$s, bless [], 'ts:collapse'; } );
         }
         $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
         $loop->run;
         is ((scalar @$ts), 0, $AGENDA.'empty result count');
         ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 3, 3 ]), $AGENDA.'side tunneled result');
#warn Dumper $tsx;
    }
#--
    for (1..STRESS) {
        @$tsx = ();
        {
#warn "--stress $_--";
            (my $ss, $ts) = $tm->execute ([ { '$a' => $tsx, '$loop' => $loop }, @$ctx ]);
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 3, 3 ]), $AGENDA."side tunneled result ($_/10)");
    }
}

if (DONE) {
    my $AGENDA = q{1:N, stream async push, compile+execute, rampup: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 1, 2 ) | zigzag | -)) s1 ((-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    use TM2::Materialized::TempleScript;

    my $ts;
    my $tsx = [];

    for (1..1) {
	$ctx = [ { '$loop' => $loop, '$a'    => $tsx }, @$ctx ];
        my $cpr = $tm->main_stream ($ctx);
        {
#warn $cpr->toString; exit;
            my $s = TM2::TempleScript::PE::cpr2stream ({}, {}, $ctx, $cpr->decanonicalize ($ctx));
            $ts = TM2::TS::Stream::last3 ($s);
#warn "last final ts $ts";
            push @$s, bless [], 'ts:kickoff';
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$s, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA.'result count');
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 2 ]), $AGENDA.'result');
#warn Dumper $tsx;
    }

#--
    for (1..STRESS) {
        @$tsx = ();
	$ctx = [ { '$loop' => $loop, '$a'    => $tsx }, @$ctx ];
        {
            (my $ss, $ts) = $tm->execute ([ { '$a' => $tsx, '$loop' => $loop }, @$ctx ]);
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 2 ]), $AGENDA."side tunneled result ($_/10)");
    }
}

if (DONE) {
    my $AGENDA = q{stream sync push, compile+execute, rampup: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 2 ) | -( s2 )- | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 1 )

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx } ];

    my $ts;

    for (1..1) {
	$ctx = [ { '$loop' => $loop, '$a'    => $tsx }, @$ctx ];
        my $cpr = $tm->main_stream ($ctx);
        {
#warn $cpr->toString; exit;
            my $s = TM2::TempleScript::PE::cpr2stream ({}, {}, $ctx , $cpr->decanonicalize ($ctx));
            $ts = TM2::TS::Stream::last3 ($s);
#warn "last final ts $ts";
#warn "   tied ".tied @$ts;
            push @$s, bless [], 'ts:kickoff';
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$s, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA.'result count');
        is_deeply ($tsx, [ [ map { TM2::Literal->new ($_) } (2, 1) ] ], $AGENDA.'side tunneled result');
#        warn Dumper $tsx; exit;
    }
#--
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        is_deeply ($tsx, [ [ map { TM2::Literal->new ($_) } (2, 1) ] ], $AGENDA."side tunneled result ($_/10)");
    }
}

if (DONE) {
    my $AGENDA = q{stream tuple block, compile+execute, standalone, 1 on 1: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 1 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 2 ) | -) s1 (-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx }, ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA.'result count');
        is_deeply ($tsx, [ [ map { TM2::Literal->new ($_) } (1) ] ], $AGENDA.'side tunneled result');
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream tuple block, compile+execute, standalone, 2 on 1: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 2 ) | -) s1 (-

s3 isa ts:stream
return
   ( 3 ) | -) s1 (-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

#    my (undef, $dd) = %{ $tm->object ('templescript/query', 'tm:s1') };
#    my (undef, $s1) = %$dd;
#warn "after parse ".$s1->toString; exit;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx } ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
#        is_deeply ($tsx, [ map { [ TM2::Literal->new ($_) ] } (1, 1)  ], $AGENDA."side tunneled result ($_/STRESS)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 2, 3 ]), $AGENDA."side tunneled result ($_/10)");
#warn Dumper $tsx;
#        exit;
    }
}

if (DONE) {
    my $AGENDA = q{stream tuple block, compile+execute, standalone, 1 on 2: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 1 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 2 ) | ->> ts:tap ($a)

s3 isa ts:stream
return
   ( 3 ) | -) s1 (- | -) s2 (-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx } ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 2 ]), $AGENDA."side tunneled result ($_/10)");
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream tuple block, compile+execute, standalone, 1 on 1 on 1: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( $0 ) | -) s1 (-

s3 isa ts:stream
return
   ( 3 ) | -) s2 (-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx } ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 3 ]), $AGENDA."side tunneled result ($_/10)");
    }
}

if (DONE) {
    my $AGENDA = q{stream tuple block, compile+execute, standalone, 1 from 2: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -( s2 )- | -( s3 )- | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 2 )

s3 isa ts:stream
return
   ( 3 )

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx }, @$ctx ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts),  0, $AGENDA."result count ($_/10)");
        is ((scalar @$tsx), 1, $AGENDA."tunneled result count");
        ok (eq_set ([ map { $_->[0] } @{$tsx->[0] } ], [ 1, 2, 3 ]), $AGENDA."side tunneled result ($_/10)");
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, 2 on 1: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( 2 ) | -))) s1 (((-

s3 isa ts:stream
return
   ( 3 ) | -))) s1 (((-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

#    my (undef, $dd) = %{ $tm->object ('templescript/query', 'tm:s1') };
#    my (undef, $s1) = %$dd;
#    warn $s1->toString; exit;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx }, @$ctx];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA."result count ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 3, 4 ]), $AGENDA."side tunneled result ($_/10)");
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, 1 on 1 on 1: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) | ->> ts:tap ($a)

s2 isa ts:stream
return
   ( $0 + 1 ) | -))) s1 (((- | ->> ts:tap ($b)

s3 isa ts:stream
return
   ( 3 ) | -))) s2 (((-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy }, @$ctx ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        @$tsy = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA.'result count');
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 5, 2 ]), $AGENDA."side tunneled result x ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsy], [ 1, 4 ]),    $AGENDA."side tunneled result y ($_/10)");
#        warn Dumper $tsx, $tsy;
    }

}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, 1 on 2: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) |->> ts:tap ($a)

s2 isa ts:stream
return
   ( $0 + 2 ) |->> ts:tap ($b)

s3 isa ts:stream
return
   ( 3 ) | -))) s2 (((- | -))) s1 (((-

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy }, @$ctx ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        @$tsy = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 3, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 4, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is ((scalar @$ts), 0, $AGENDA.'result count');
        ok (eq_set ([ map { $_->[0]->[0] } @$tsx], [ 1, 4 ]), $AGENDA."side tunneled result x ($_/10)");
        ok (eq_set ([ map { $_->[0]->[0] } @$tsy], [ 2, 5 ]), $AGENDA."side tunneled result y ($_/10)");
#        warn Dumper $tsx, $tsy;
    }

}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, ourhoboros, tapped: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) | -))) s1 (((- | ->> ts:tap ($a)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx }, @$ctx ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is_deeply ($ts,  [], $AGENDA."result ($_/10)");
        my $max = $tsx->[-1]->[0]->[0];
        ok ($max > 100, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ($tsx, [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result ($_/10)");
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, pure ourhoboros, with observer: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) | -))) s1 (((-

obs isa ts:stream
return
   -((( s1 )))- |->> ts:tap ($a)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$loop' => $loop, '$a' => $tsx }, @$ctx ];

    my $ts;
    for (1..STRESS) {
        @$tsx = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 2, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 3, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is_deeply ($ts,  [], $AGENDA."result ($_/10)");
        shift @$tsx; # skip undef
        my $max = $tsx->[-1]->[0]->[0];
        ok ($max > 100, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ($tsx, [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result ($_/10)");
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, pure ourhoboros, with ticking observer: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( $0 + 1 ) | -))) s1 (((- | ->> ts:tap ($b)

obs isa ts:stream
return
   <++ every 1 secs ++> | -( s1 )- |->> ts:tap ($a)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy } ];

    my $ts;
    for (1..STRESS / 3) {
        @$tsx = ();
        @$tsy = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 20, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
            $loop->watch_time( after => 21, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        }
        $loop->run;
        is_deeply ($ts,  [], $AGENDA."result ($_/10)");
        my $max = $tsy->[-1]->[0]->[0];
        ok ($max > 100, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ($tsy, [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result y ($_/10)");

        $max = $tsx->[-1]->[1]->[0];
        ok (10 < $max, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ([ map { [ $_->[1] ] } @$tsx ], [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result x ($_/10)");
        is (int ($tsx->[$_]->[0]->[0] - $tsx->[$_-1]->[0]->[0] + 0.1), 1, $AGENDA.'timestamps x') for 1..$#$tsx;

#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, pure ourhoboros (reverted), with ticking observer: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   -((( s1 )))- | ( $0 + 1 ) |->> ts:tap ($b)

obs isa ts:stream
return
   <++ every 1 secs ++> | -( s1 )- |->> ts:tap ($a)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy } ];

    my $ts;
    for (1..1) {
        @$tsx = ();
        @$tsy = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 10, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 11, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;

#        warn Dumper $tsx;
#        warn Dumper $tsy;

        is_deeply ($ts,  [], $AGENDA."result ($_/10)");
        my $max = $tsy->[-1]->[0]->[0];
        ok (5 < $max && $max < 15, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ($tsy, [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result y ($_/10)");

        $max = $tsx->[-1]->[1]->[0];
        ok (8 < $max && $max < 12, $AGENDA."peano reasonable result $max ($_/10)");
        is_deeply ([ map { [ $_->[1] ] } @$tsx ], [ map {[ TM2::Literal->new($_) ]} 1..$max], $AGENDA."side tunneled result x ($_/10)");
        is (int ($tsx->[$_]->[0]->[0] - $tsx->[$_-1]->[0]->[0] + 0.1), 1, $AGENDA.'timestamps x') for 1..$#$tsx;

#        warn "tsx ".scalar @$tsx;
#        warn "tsy ".scalar @$tsy;
#        warn Dumper $tsx;
    }
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, 1 on 1 ourhoboros, forward push: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -))) s2 (((- |->> ts:tap ($b)

s2 isa ts:stream
return
   ( 2 ) | -))) s1 (((- |->> ts:tap ($b)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy } ];

    my $ts;
    for (1..1) {
        @$tsx = ();
        @$tsy = ();
        {
            (my $ss, $ts) = $tm->execute ($ctx);
            $loop->watch_time( after => 5, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
        }
        $loop->watch_time( after => 6, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
        $loop->run;
        is_deeply ($ts,  [], $AGENDA.'result');
        is_deeply ($tsx, [], $AGENDA.'tunneled result');
    }
#    warn "tsy ".scalar @$tsy;
    my $max = scalar @$tsy;
    ok (800 < $max, $AGENDA."reasonable result $max (/10)");
    while (scalar @$tsy >= 4) {
        my @a = splice @$tsy,0,4;
        if ($a[0]->[0]->[0] == 1) {
            is_deeply ([ map { $_->[0]->[0] } @a ], [ 1, 2, 2, 1 ], $AGENDA.'sideline result');
        } else {
            is_deeply ([ map { $_->[0]->[0] } @a ], [ 2, 1, 1, 2 ], $AGENDA.'sideline result');
        }
    }
    # warn Dumper [ @$tsy[0..10] ];
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, standalone, 1 on 1 ourhoboros, reversed pull, with observer: };

    my $tm = _parse (q{

s1 isa ts:stream
return
   -((( s2 )))- | ( 1 ) |->> ts:tap ($b)

s2 isa ts:stream
return
   -((( s1 )))- | ( 2 ) |->> ts:tap ($b)

obs isa ts:stream
return
   <++ every 1 secs ++> | -( s1 )- |->> ts:tap ($a)

})->extend ('TM2::Executable');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    
    my $tsx = [];
    my $tsy = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, { '$loop' => $loop, '$a' => $tsx, '$b' => $tsy } ];

    my $ts;
    for (1..STRESS/3) {
        {
            @$tsx = ();
            @$tsy = ();
            {
                (my $ss, $ts) = $tm->execute ($ctx);
                $loop->watch_time( after => 5, code => sub { diag "collapsing"    if $warn; push @$ss, bless [], 'ts:collapse'; } );
            }
            $loop->watch_time( after => 6, code => sub { diag "stopping loop" if $warn; $loop->stop; } );
            $loop->run;
            is_deeply ($ts,  [], $AGENDA.'result');
        }
#        warn "tsx ".scalar @$tsx;
        my $max = scalar @$tsx;
        ok (3 < $max && $max < 10, $AGENDA."reasonable result $max ($_/10)"); # 6 would be good
        is_deeply ([ map { $_->[1]->[0] } @$tsx ], [ ( 1 ) x $max ], $AGENDA."side tunneled result x ($_/10)");
        is (int ($tsx->[$_]->[0]->[0] - $tsx->[$_-1]->[0]->[0] + 0.1), 1, $AGENDA.'timestamps x') for 1..$#$tsx;

#        warn Dumper $tsx;
#        warn "tsy ".scalar @$tsy;
        $max = scalar @$tsy;
        ok (10 < $max, $AGENDA."reasonable result $max ($_/10)");
        while (scalar @$tsy >= 2) {
            my @a = splice @$tsy,0,2;
            if ($a[0]->[0]->[0] == 1) {
                is_deeply ([ map { $_->[0]->[0] } @a ], [ 1, 2 ], $AGENDA."sideline result ($_/10)");
            } else {
                is_deeply ([ map { $_->[0]->[0] } @a ], [ 2, 1 ], $AGENDA."sideline result ($_/10)");
            }
        }
    }
    #warn Dumper $tsy;
}

if (DONE) {
    my $AGENDA = q{stream async push, compile+execute, errors: };

    throws_ok {
        _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -))) s2 (((-

})->extend ('TM2::Executable');
    } qr/no ts:stream/, $AGENDA.'missing target topic';

    throws_ok {
        _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -))) s2 (((-

s2 isa rumsti

})->extend ('TM2::Executable');
    } qr/no ts:stream/, $AGENDA.'no stream';

    throws_ok {
        _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -))) s2 (((-

s2 isa ts:stream

})->extend ('TM2::Executable');
    } qr/any content/, $AGENDA.'target no content';

    throws_ok {
        _parse (q{

s1 isa ts:stream
return
   ( 1 ) | -))) s2 (((-

s2 isa ts:stream
holds "aaa"

})->extend ('TM2::Executable');
    } qr/any content/, $AGENDA.'target holds no code';

}

