package Net::Async::0MQ::Socket;

use strict;
use warnings;

use ZMQ::FFI qw(ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_REQ ZMQ_ROUTER);

use base qw( IO::Async::Handle );

sub configure {
    my ($self, %params) = @_;

    for (qw(endpoint context socket fileno on_recv type)) {
	$self->{$_} = delete $params{$_} if exists $params{$_};
    }

    if ( exists $self->{endpoint}) { # derive everything from here
	my $s = $self->{context}->socket( $self->{type} );
	if ( $self->{type} eq ZMQ_SUB ) {
	    $s->connect( $self->{endpoint} );
	} elsif ( $self->{type} eq ZMQ_REQ ) {
	    $s->connect( $self->{endpoint} );
	} else {
	    $s->bind( $self->{endpoint} );
	}
	$self->{socket} = $s;
    }

    if ( exists $self->{fileno}) { # that's it
	$params{read_fileno} = $self->{fileno};
    } elsif ( exists $self->{socket} ) {
	$params{read_fileno} = $self->{socket}->get_fd();
    }

    $params{on_read_ready} = sub {
	my ($self) = @_;
	my $s = $self->{socket};
#warn "read ready $s";
	while ( $s->has_pollin ) {
	    &{$self->{on_recv}} ($s);
	}
    };

    $self->SUPER::configure(%params);
}

sub DESTROY {
    my $self = shift;
#warn "IO socket DESTROY $self";
    $self->{socket}->close();
}

1;
