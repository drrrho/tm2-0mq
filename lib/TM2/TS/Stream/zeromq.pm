package TM2::TS::Stream::zeromq::factory;

use Moose;
extends 'TM2::TS::Stream::factory';

use Data::Dumper;

has 'loop' => (
    is        => 'rw',
    isa       => 'IO::Async::Loop',
    );

has 'uri' => (
    is => 'ro',
    isa => 'Str'
    );

around 'BUILDARGS' => sub {
    my $orig = shift;
    my $class = shift;
#warn "0MQ factory new $_[0]";
#warn "params $class $orig ".Dumper \@_;

    if (scalar @_ % 2 == 0) {
        return $class->$orig (@_);
    } else {
        my $uri = shift;
        return $class->$orig ({ uri => $uri, @_ });
    }
};

sub prime {
    my $self = shift;
    my $out = [];

    tie @$out, 'TM2::TS::Stream::zeromq', $self->loop, $self->uri, @_;
    return $out;
}

# sub provisioning {
#     my $self = shift;
#     my $cs   = shift;

# warn "0MQ context".$zmq_ctx;
#     $self->_ctx ($zmq_ctx);
# }

1;

package TM2::TS::Stream::zeromq;

use strict;
use warnings;

use Data::Dumper;

use vars qw( @ISA );

use TM2;
use TM2::TS::Shard;
@ISA = ('TM2::TS::Shard');


#== ARRAY interface ==========================================================

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
our $CTX = ZMQ::FFI->new(); # shared in this process

sub new_socket {
    my $uri = shift;
    my $loop = shift;
#    my $ctx  = shift;
    my $tail = shift;

    $uri =~ /0mq-(.+?);(.+)/;
    my $endpoint = $1
	// $TM2::log->logdie ("no endpoint detected inside '$uri'");
    my %params = map { split (/=/, $_ ) }
		       split (/;/, $2);
    $params{type} = { # convert string into constant
		      'ROUTER' => ZMQ_ROUTER,
		      'DEALER' => ZMQ_DEALER,
		      'PUB'    => ZMQ_PUB,
		      'REQ'    => ZMQ_REQ,
		      'REP'    => ZMQ_REP,
	            }->{ uc( $params{type} ) }
        // $TM2::log->logdie ("unknown/unhandled 0MQ socket type inside '$endpoint'");
#warn "endpoint $endpoint ".Dumper \%params;

    use Net::Async::0MQ::Socket;
    my $socket = Net::Async::0MQ::Socket->new(
	endpoint => $endpoint,
	type     => $params{type},
	context  => $CTX,
	on_recv  => sub {
	    my $s = shift;
	    my @d = $s->recv_multipart();
#warn "zeromq recv on $s ".Dumper \@d;
	    use TM2::Literal;
	    push @$tail, [ $s, map { TM2::Literal->new( $_ ) } @d ];
	}
	);
    $loop->add( $socket );
    return $socket;
}

sub TIEARRAY {
    my $class = shift;
    my ($loop, $uri, $tail) = @_;
#warn "0mq TIEARRAY $loop tail $tail";

    return bless {
        creator   => $$, # we can only be killed by our creator (not some fork()ed process)
        stopper   => undef,
        loop      => $loop,
#	ctx       => $CTX, # ZMQ context
	uri       => $uri,
	socket    => undef, # only after kickoff
        tail      => $tail,
    }, $class;
}

sub DESTROY {
    my $elf = shift;
    return unless $$ == $elf->{creator};
    $elf->{loop}->await( $elf->{stopper} ) if $elf->{stopper};                                      # we do not give up that easily
    $elf->{loop}->remove( $elf->{socket} );
#    $elf->{socket} = undef;
}

sub FETCH {
    my $elf = shift;
    return undef;
}

sub PUSH {
    my $elf = shift;
#warn "zeromq PUSH ".Dumper \@_;

    my $tail = $elf->{tail}; # handle

    if (ref ($_[0]) eq 'ts:collapse') {
	$elf->{stopper}->done;                                                   # allow self DESTROY

    } else {
	$elf->{socket}  //= new_socket ($elf->{uri}, $elf->{loop}, $tail);
	$elf->{stopper} //= $elf->{loop}->new_future;
    }
}

sub FETCHSIZE {
    my $elf = shift;
    return 0;
}

sub CLEAR {
    my $elf = shift;
    $elf->{size} = 0;
}

1;

__END__

