use strict;
use warnings;

use Data::Dumper;

use Test::More;


require_ok( 'Net::Async::0MQ::Socket' );

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
my $ctx      = ZMQ::FFI->new();

my $file     = "zmq-ffi-$$";

if (1) {
    my $AGENDA = 'router: ';
    
    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $endpoint = "ipc://$file";

    my $router = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => ZMQ_ROUTER,
	context  => $ctx,
	on_recv  => sub {
	    my $s = shift;
	    my @d = $s->recv_multipart();
	    is ((scalar @d), 4, $AGENDA.'dealer prefix');
	    shift @d;
#    warn "".Dumper \@d;
	    ok (eq_array( \@d, [ qw(aaa bbb ccc) ]), $AGENDA.'data');
    }
    );
    $loop->add( $router );

    my $req = $ctx->socket(ZMQ_DEALER);
    $req->connect( $endpoint );
    
    use IO::Async::Timer::Periodic;
    my $timer = IO::Async::Timer::Periodic->new(
	interval => 3,
	on_tick => sub {
	    $req->send_multipart( [ qw[aaa bbb ccc] ] );
	},
	);
    $timer->start;
    $loop->add( $timer );

    $loop->watch_time( after => 10, code => sub {
	$loop->stop; } );

    $loop->run;
}

if (1) {
    my $AGENDA = 'publisher: ';
    
    my $endpoint = "ipc://$file";

    my $pub = $ctx->socket(ZMQ_PUB);
    $pub->bind( $endpoint );

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $sub = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => ZMQ_SUB,
	context  => $ctx,
	on_recv  => sub {
	    my $s = shift;
	    my @d = $s->recv_multipart();
#warn "".Dumper \@d;
	    is ((scalar @d), 3, $AGENDA.'no prefix');
	    ok (eq_array( \@d, [ qw(aaa bbb ccc) ]), $AGENDA.'data');
    }
    );
    $sub->{socket}->subscribe( 'aaa' );
    $loop->add( $sub );

    
    use IO::Async::Timer::Periodic;
    my $timer = IO::Async::Timer::Periodic->new(
	interval => 3,
	on_tick => sub {
	    $pub->send_multipart( [ qw[aaa bbb ccc] ] );
	    $pub->send_multipart( [ qw[bbb ccc aaa] ] );
	},
	);
    $timer->start;
    $loop->add( $timer );

    $loop->watch_time( after => 10, code => sub {
	$loop->stop; } );

    $loop->run;

}

unlink $file;

done_testing;



#           $context->destroy();    
__END__


#--


__END__



my $server = $connection->{transport} .'://'. $connection->{ip};



#-- IOPub channel to broadcast
my $iopub_ctr = 0;
my $iopub = $ctx->socket(ZMQ_PUB);
$iopub->bind( $server .':'. $connection->{iopub_port} );

#--
my $shell_ctr = 0;
my $execution_ctr = 0;

#--
sub _mk_header {
    my ($req, $type, $msg_id) = @_;

    use POSIX qw(strftime);
    use Time::HiRes;
    my $now = Time::HiRes::time();

    return {
	'msg_id'   => ( sprintf "%010d", $msg_id ),
	'session'  => $req->{header}->{session},
	'msg_type' => $type,
	'version'  => '5.0',
	'date'     => ( strftime('%Y-%m-%dT%H:%M:%sZ', gmtime($now)) ),
    };
}

sub sign_msg {
    unless ($connection->{key}) {
	return ('', @_);
	
    } else {
	use Digest::SHA qw(hmac_sha256_hex);
	my $data = join ('', @_);
	my $sig  = hmac_sha256_hex( $data, $connection->{key} );
	return ( $sig, @_ );
    }
}


sub jupyter_status {
    my $req    = shift;
    my $status = shift;
    my $msg_id = shift;

    return {
	header        => _mk_header ($req, 'status', $msg_id),
	parent_header => $req->{header},
	metadata => {},
	content => {
	    execution_state => $status,
	},
    };
}

sub jupyter_response {
    my $req = shift;
    my $msg_id = shift;

    if ($req->{header}->{msg_type} eq 'kernel_info_request') {
	return {
	    header        => _mk_header ($req, 'kernel_info_reply', $msg_id),
	    parent_header => $req->{header},
	    metadata      => {},
	    content       => {
		'status' => 'ok',
		'protocol_version' => '5.0',
		'implementation' => 'itemplescript',
		'implementation_version' => '0.0.1',
		
		'language_info' => {
		    'name' => 'templescript',
		    'version' =>  '1.0',
		    'mimetype' => 'text/templescript',
		    'file_extension' => '.ts',
		},
		'banner' => 'TempleScript',
		'help_links' => [
		    {'text' => "TempleScript.org", 'url' => 'http://templescript.org/'},
		    ],
	    },
	};

    } elsif ($req->{header}->{msg_type} eq 'execute_request') {
	use constant DELIMITER => '<IDS|MSG>';

	# 1) signal to the clients that we are busy
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'status', DELIMITER,
	    sign_msg(
	    encode_jupyter( jupyter_status ($req, 'busy', $iopub_ctr++) ) )
			   ]);
	# 2) signal to the clients the data we have
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'execute_result', DELIMITER,
	    sign_msg(
	    encode_jupyter( {
		header        => _mk_header ($req, 'execute_result', $iopub_ctr++),
		parent_header => $req->{header},
		metadata      => {},
		content       => {
		    metadata        => {},
		    data            => { 'text/plain' => 'ZZZZZZZZZZZZZZZ', 'text/html' => 'HHHHHHHHHHHHHHH' },
		    execution_count => $execution_ctr,
		},
			    } ) )
					 ]);
	# 3) signal to the clients that we are idle again
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'status', DELIMITER,
	    sign_msg(
	    encode_jupyter( jupyter_status ($req, 'idle', $iopub_ctr++) ) )
					 ]);
	# 4) prepare message to be returned on shell channel
	return {
	    header        => _mk_header ($req, 'execute_reply', $shell_ctr++),
	    parent_header => $req->{header},
	    metadata      => {},
	    content => {
		'status' => 'ok',
		'execution_count' => $execution_ctr++,
		    metadata        => {},
		    data            => { 'text/plain' => 'ZZZZZZZZZZZZZZZ', 'text/html' => 'HHHHHHHHHHHHHHH' },
		# 'status' => 'error',
		# 'ename'  =>  'XXXXXXXXXXXXXXXXXX',
		# 'evalue' =>  'foo',
		# 'traceback' => 'YYYYYYYYYYYYYYY',
	    },
	};

    } elsif ($req->{header}->{msg_type} eq 'shutdown_request') {
	if ($req->{content}->{restart} eq "true") {
	    warn "cannot restart";
	}
	return {
	    header        => _mk_header ($req, 'shutdown_reply', $shell_ctr++),
	    parent_header => $req->{header},
	    metadata      => {},
	    content       => $req->{content},
	};
    }
}

#--
my $hb = Net::Async::ZMQ::Socket->new(
    endpoint => $server .':'. $connection->{hb_port},
    type     => ZMQ_REP,
    context  => $ctx,
    on_recv => sub {
	my $s = shift;
	my @c = $s->recv_multipart();
warn "hb received ".Dumper \@c;
	$s->send_multipart(\@c);
    }
    );
$loop->add( $hb );

# my $hb = $ctx->socket( ZMQ_ROUTER );
# $ctx->proxy( $hb, $hb );


#--
my $control_ctr = 0;
my $control = Net::Async::ZMQ::Socket->new(
    endpoint => $server .':'. $connection->{control_port},
    type     => ZMQ_ROUTER,
    context  => $ctx,
    on_recv => sub {
	my $s = shift;
	my ($sender, $delimiter, undef, @c) = $s->recv_multipart();
warn "control received ".Dumper \@c;
	my $req  = decode_jupyter (@c);
warn Dumper $req;
	$s->send_multipart([
	    $sender, $delimiter,
map { (warn "control back: ".Dumper $_) && $_ }
	    sign_msg(
	    encode_jupyter( jupyter_response ($req, $control_ctr++) ) )
			   ]);
	$loop->stop;
    }
    );
$loop->add( $control );

#--
use IO::Async::Timer::Periodic;
my $timer = IO::Async::Timer::Periodic->new(
    interval => 5,
    on_tick => sub {
	warn "tick";
    },
    );
$timer->start;
$loop->add( $timer );


$loop->run;

__END__

# my $iopub = Net::Async::ZMQ::Socket->new(
#     endpoint => $server .':'. $connection->{iopub_port},
#     type     => ZMQ_ROUTER,
#     context  => $ctx,
# #     on_recv => sub {
# # 	my $s = shift;
# # 	my @c = $s->recv_multipart();
# # warn "iopub received ".Dumper \@c;
# # #	$s->send_multipart(\@c);
# #     }
#     );
# $loop->add( $iopub );

