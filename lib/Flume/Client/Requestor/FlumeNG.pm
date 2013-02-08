#!/usr/bin/perl

package Flume::Client::Requestor::FlumeNG;
our @ISA = qw/Flume::Client::Requestor/;

=head2 Flume::Client::Requestor::FlumeNG

Requestor implementation for FlumeNG.

=cut

=head3 new

Constructor takes the following arguments:

	client - instance of Flume::Client::Transceiver::Socket
	
	protocol - avro protocol as json text
	           avpr, may be parsed from avdl files, the relevant namespace is:
	           org.apache.flume.source.avro / AvroSourceProtocol (set by default)

=cut

sub new {
	my $class = shift;
	my (%params) = @_;
	if (not exists $params{protocol}) {
		$params{protocol} = <<'EOJ';
{
  "protocol" : "AvroSourceProtocol",
  "namespace" : "org.apache.flume.source.avro",
  "doc" : "* Licensed to the Apache Software Foundation (ASF) under one\n * or more contributor license agreements.  See the NOTICE file\n * distributed with this work for additional information\n * regarding copyright ownership.  The ASF licenses this file\n * to you under the Apache License, Version 2.0 (the\n * \"License\"); you may not use this file except in compliance\n * with the License.  You may obtain a copy of the License at\n *\n * http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing,\n * software distributed under the License is distributed on an\n * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n * KIND, either express or implied.  See the License for the\n * specific language governing permissions and limitations\n * under the License.",
  "types" : [ {
    "type" : "enum",
    "name" : "Status",
    "symbols" : [ "OK", "FAILED", "UNKNOWN" ]
  }, {
    "type" : "record",
    "name" : "AvroFlumeEvent",
    "fields" : [ {
      "name" : "headers",
      "type" : {
	"type" : "map",
	"values" : "string"
      }
    }, {
      "name" : "body",
      "type" : "bytes"
    } ]
  } ],
  "messages" : {
    "append" : {
      "request" : [ {
	"name" : "event",
	"type" : "AvroFlumeEvent"
      } ],
      "response" : "Status"
    },
    "appendBatch" : {
      "request" : [ {
	"name" : "events",
	"type" : {
	  "type" : "array",
	  "items" : "AvroFlumeEvent"
	}
      } ],
      "response" : "Status"
    }
  }
}
EOJ
	}
	my $self = $class->SUPER::new(%params);
	return $self;
}

=head3 to_netty_frame

Takes a list of strings and serializes them into a scalar using the format used by the NettyTransceiver.

Format is:

4 bytes big-endian	serial number of request/response
4 bytes big-endian	number of following frames
4 bytes big-endian	number of bytes in first frame
n bytes			first frame payload
4 bytes big-endian	number of bytes in next frame
n bytes			next frame payload
...

See java implementation of org.apache.avro.ipc.Requestor.

=cut

sub to_netty_frame {
	my $self = shift;
	$self->{_serial}++;
	return pack('N N', $self->{_serial}, scalar(@_)).$self->to_frame(@_);
}

=head3 from_netty_frame

Takes a frame buffer in the format used by NettyTransceiver and returns a list of frame payloads.

=cut

sub from_netty_frame {
	my $self = shift;
	my ($serial, $framecount) = unpack('N N', $_[0]);
	return $self->from_frame(substr($_[0], 8));
}

=head3 request

Execute the actual request.

Arguments:

	method	-	scalar with method name (append, appendBatch)
	
	event	-	data structure with event data
	
	sendproto -	used internally to signal sending of protocol data

Returns two values as a list:

	first value is 0 on success or higher on client error.
	
	second value is response returned by server (for FlumeNG this is a string of either 'OK', 'FAILED' or 'UNKNOWN').

=cut

sub request {
	my ($self, $method, $event, $sendproto) = @_;
	
	my $enc = defined $sendproto? $self->{_handshake_prefix_with_proto} : $self->{_handshake_prefix};

	Avro::BinaryEncoder->encode(
		schema => 'string',
		data => $method,
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	
	my $encbody = '';
	Avro::BinaryEncoder->encode(
		schema => $self->{_proto_map}->[1]->messages->{$method}->request->[0]->{type},
		data => $event,
		emit_cb => sub { $encbody .= ${ $_[0] } },
	);

	# content is serialized using NettyTransceiver format (see to_netty_frame)
	# first frame contains handshake request, meta data, method name
	# second frame contains event data
	my $content = $self->to_netty_frame($enc, $encbody);

	my ($result, $call_response) = $self->parse_response($method, $self->{client}->request($content));
	if ($result == 2 && not defined $sendproto) {
		return $self->request($method, $event, 1);
	}
	return ($result, $call_response);
}

=head3 parse_response

Method used internally to parse the received response.

Takes the called method name and the response (scalar) and returns an integer (zero on success, higher on error) and the server response.

=cut

sub parse_response {
	my ($self, $method, $response) = @_;
	if ($response ne '') {
		my ($handshakeframe, $metaframe, $responseframe) = $self->from_netty_frame($response);
		my $handshakereader = IO::String->new($handshakeframe);
		# decode handshake response
		my $handshake_response = Avro::BinaryDecoder->decode(
			writer_schema => $self->{_handshake_response},
			reader_schema => $self->{_handshake_response},
			reader => $handshakereader
		);
		if (ref($handshake_response) eq 'HASH') {
			# store protocol if match is not BOTH
			if ($handshake_response->{'match'} ne 'BOTH') {
				push(@{$self->{_proto_map}}, $handshake_response->{serverProtocol});
				$self->parse_proto_schemas();
			}
			# resend if match is NONE
			if ($handshake_response->{'match'} eq 'NONE') {
				return (2, undef);
			}
			# hack: there seems to be an additional byte between handshake and response (perhaps something like meta)
			# as a workaround we just take the last byte of the frame (we make sure that it isnt empty first)
			if (length($responseframe) == 0) {
				return (1, undef);
			}
			my $responsereader = IO::String->new(substr($responseframe, -1));
			# decode call response
			my $call_response = Avro::BinaryDecoder->decode(
				writer_schema => $self->{_proto_map}->[$self->{_server_proto}+1]->messages->{$method}->response,
				reader_schema => $self->{_proto_map}->[$self->{_server_proto}+1]->messages->{$method}->response,
				reader => $responsereader
			);
			return (0, $call_response);
		}
	}
	return (1, undef);
}

1;
