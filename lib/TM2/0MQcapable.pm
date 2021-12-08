package TM2::0MQcapable;

use strict;
use warnings;

use Moose::Role;
with 'TM2::Executable';

use TM2::TempleScript::Parser;

BEGIN { # we extend the TS grammar
$TM2::TempleScript::Parser::grammar .= q|

     listener_factory         : zmq_listener

     zmq_listener             : '0mq-' uri                           {
                                                                        use TM2::TS::Stream::zeromq;
                                                                        $return = TM2::TS::Stream::zeromq::factory->new (uri => '0mq-'.$item[2]); # loop can only be determined later
                                                                      }
   
|;
}

our $VERSION = "0.02";

1;

__END__

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);



around 'main_stream' => sub {
    my $orig = shift;
    my $self = shift;
    my $ctx  = shift;
    my $ms = $self->$orig ($ctx, @_);
    
    use TM2::TempleScript::Parser;
    my $ap   = new TM2::TempleScript::Parser ();                                         # quickly clone a parser
    my $prelude = $ap->parse_query (' ( @_, 
                                        ( 
                                          ( "$TM2::0MQcapable::CTX" ^^ lang:perl ! ) |=> $zmq:ctx |->> ts:kickoff
                                         )
                                      ) ' , $self->stack);
    $prelude->append ($ms); # and continue with the rest
#warn "mainstream 0mq for $self ".$prelude->toString;
    return $prelude->resolve( {}, $ctx );
};

