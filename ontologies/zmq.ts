%include file:zmq.atm

isa implementation @ lang:perl :
   ts:role     : zmq
   ts:software : urn:x-perl:TM2::0MQcapable

zmq:send isa ts:function
         isa ts:side-effect
return """
   my $something = shift @_;
#use Data::Dumper;
#warn "sending".Dumper $something;

   if (ref( $something ) eq 'TM2::Literal') {
      $something->[0] =~ /(.+?);type=(.+)/
          || $TM2::log->logdie( "expected endpoint to be of the form '(ipc|...):....;type=<ROUTER-whatever>'" );
      my ($endpoint, $type) = ($1, $2);
#warn "$endpoint $type";

      use TM2::TempleScript::PE;
      my $zmq_ctx = $TM2::TS::Stream::zeromq::CTX       # empleScript::PE::lookup_var ($cs, '$zmq:ctx')
          || $TM2::log->logdie( "no ZMQ context defined via a variable '\$zmq:ctx'" );

      use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
      $type = { # convert string into constant
		      'ROUTER' => ZMQ_ROUTER,
		      'DEALER' => ZMQ_DEALER,
		      'PUB'    => ZMQ_PUB,
		      'REQ'    => ZMQ_REQ,
		      'REP'    => ZMQ_REP,
	       }->{ uc( $type ) };
#warn "$endpoint $type";
      my $s = $zmq_ctx->socket($type);
      $s->connect( $endpoint );
#warn Dumper \@_;
      map { $TM2::log->logdie( "cannot process non TM2::Literals for serialization" ) 
		  unless ref($_) eq 'TM2::Literal' } 
	    @_;
      $s->send_multipart( [ map { $_->[0] } @_ ] );
      $s->disconnect( $endpoint );

   } elsif (ref( $something ) =~ /Socket/) {
      my $s = $something;
      map { $TM2::log->logdie( "cannot process non TM2::Literals for serialization" ) 
		  unless ref($_) eq 'TM2::Literal' } 
	    @_;
      $s->send_multipart( [ map { $_->[0] } @_ ] );

   } else {
      $TM2::log->logdie ("first parameter to zmq:send is neither endpoint string nor ZMQ socket");
   }
   return \@_;
""" ^^ lang:perl !

