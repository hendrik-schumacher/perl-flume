#!/usr/bin/perl

package Flume::Client::Requestor::FlumeOG;
our @ISA = qw/Flume::Client::Requestor/;

=head2 Flume::Client::Requestor::FlumeOG

Requestor implementation for FlumeOG.

=cut

=head3 new

Constructor takes the following arguments:

	client - instance of Flume::Client::Transceiver::HTTP
	
	protocol - avro protocol as json text
	           avpr, may be parsed from avdl files, the relevant namespaces are:
	           com.cloudera.flume.handlers.avro / FlumeOGEventAvroServer (set by default)

=cut

sub new {
	my $class = shift;
	my (%params) = @_;
	if (not exists $params{protocol}) {
		$params{protocol} = <<'EOJ';
{
  "protocol" : "FlumeOGEventAvroServer",
  "namespace" : "com.cloudera.flume.handlers.avro",
  "doc" : "Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the 'License'); you may not use this file except in compliance with the License.  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.",
  "types" : [ {
    "type" : "enum",
    "name" : "Priority",
    "symbols" : [ "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE" ]
  }, {
    "type" : "record",
    "name" : "AvroFlumeOGEvent",
    "fields" : [ {
      "name" : "timestamp",
      "type" : "long"
    }, {
      "name" : "priority",
      "type" : "Priority"
    }, {
      "name" : "body",
      "type" : "bytes"
    }, {
      "name" : "nanos",
      "type" : "long"
    }, {
      "name" : "host",
      "type" : "string"
    }, {
      "name" : "fields",
      "type" : {
	"type" : "map",
	"values" : "bytes"
      }
    } ]
  } ],
  "messages" : {
    "append" : {
      "request" : [ {
	"name" : "evt",
	"type" : "AvroFlumeOGEvent"
      } ],
      "response" : "null"
    }
  }
}
EOJ
	}
	my $self = $class->SUPER::new(%params);
	return $self;
}

=head3 request

Execute the actual request.

Arguments:

	method	-	scalar with method name (append)
	
	event	-	data structure with event data
	
	sendproto -	used internally to signal sending of protocol data

Returns two values as a list:

	first value is 0 on success or higher on client error.
	
	second value is response returned by server (for FlumeOG nothing is returned).

For protocol format see http://avro.apache.org/docs/1.3.2/spec.html#Protocol+Wire+Format

=cut

sub request {
	my ($self, $method, $event, $sendproto) = @_;
	
	my $enc = defined $sendproto? $self->{_handshake_prefix_with_proto} : $self->{_handshake_prefix};

	Avro::BinaryEncoder->encode(
		schema => 'string',
		data => $method,
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	Avro::BinaryEncoder->encode(
		schema => $self->{_proto_map}->[1]->messages->{$method}->request->[0]->{type},
		data => $event,
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	
	# content is one frame containing handshake request, meta data, method name, event data
	# and a second empty frame
	my $content = $self->to_frame($enc, '');

	my ($result, $call_response) = $self->parse_response($method, $self->{client}->request($content));
	if ($result == 2 && not defined $sendproto) {
		return $self->request($method, $event, 1);
	}
	return ($result, $call_response);
}

=head3 parse_response

Method used internally to parse the received response.

Takes the called method name and the response (hash) and returns an integer (zero on success, higher on error) and the server response.

=cut

sub parse_response {
	my ($self, $method, $response) = @_;
	# call succeeded (200 OK)
	if ($response->{success}) {
		# response body not empty
		if ($response->{content} ne '') {
			foreach my $frame ($self->from_frame($response->{content})) {
				my $reader = IO::String->new($frame);
				# decode handshake response
				my $handshake_response = Avro::BinaryDecoder->decode(
					writer_schema => $self->{_handshake_response},
					reader_schema => $self->{_handshake_response},
					reader => $reader
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
					# decode call response
					my $call_response = Avro::BinaryDecoder->decode(
						writer_schema => $self->{_proto_map}->[$self->{_server_proto}+1]->messages->{$method}->response,
						reader_schema => $self->{_proto_map}->[$self->{_server_proto}+1]->messages->{$method}->response,
						reader => $reader
					);
					return (0, $call_response);
				}
			}
		}
	}
	return (1, undef);
}

1;
