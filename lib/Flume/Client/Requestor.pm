#!/usr/bin/perl

package Flume::Client::Requestor;

use Flume::Client::Requestor::FlumeNG;
use Flume::Client::Requestor::FlumeOG;

=head2 Flume::Client::Requestor

Base class for the Flume Client requestors.

=cut

=head3 new

Constructor takes the following arguments:

	client - instance of Flume::Client::Transceiver::HTTP (FlumeOG) or Flume::Client::Transceiver::Socket (FlumeNG)
	
	protocol - avro protocol as json text
	           avpr, may be parsed from avdl files, the relevant namespaces are:
	           com.cloudera.flume.handlers.avro / FlumeOGEventAvroServer (FlumeOG)
	           org.apache.flume.source.avro / AvroSourceProtocol (FlumeNG)

Needs:

	Avro::Schema

	Avro::Protocol

	Avro::BinaryEncoder

	Avro::BinaryDecoder

	Digest::MD5

	IO::String

=cut

sub new {
	my $class = shift;
	my (%params) = @_;
	if (not exists $params{client} || not exists $params{protocol}){
		return undef;
	}
	require Avro::Schema;
	require Avro::Protocol;
	require Avro::BinaryEncoder;
	require Avro::BinaryDecoder;
	require Digest::MD5;
	require IO::String;

	$params{_proto_map} = [$params{protocol}];
	$params{_server_proto} = 0;
	$params{_serial} = 0;
	my $self = bless \%params, $class;
	$self->parse_proto_schemas();
	return $self;
}

=head3 to_frame

Takes a list of strings and returns a scalar with concatted frames.

=cut

sub to_frame {
	my $self = shift;
	my $frame = '';
	foreach my $payload (@_) {
		$frame .= pack('N/A*', $payload);
	}
	return $frame;
}

=head3 from_frame

Takes a frame string and returns the contained payload strings as a list.

=cut

sub from_frame {
	#my $self = shift;
	my @payloads = ();
	my $offset = 0;
	my $len = length($_[1]);
	while ($offset < $len) {
		my $framelen = unpack('N', substr($_[1], $offset, 4));
		push(@payloads, substr($_[1], $offset+4, $framelen));
		$offset += $framelen + 4;
	}
	return @payloads;
}

=head3 parse_proto_schemas

Internal function used to process the neccessary avro protocols and schemas.

=cut

sub parse_proto_schemas {
	my $self = shift;
	
	# avro rpc handshake request schema
	$self->{_handshake_request} = Avro::Schema->parse(<<EOJ);
{
  "type": "record",
  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
  "fields": [
    {"name": "clientHash",
     "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "clientProtocol", "type": ["null", "string"]},
    {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
EOJ
	# avro rpc handshake response schema
	$self->{_handshake_response} = Avro::Schema->parse(<<EOJ);
{
  "type": "record",
  "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
  "fields": [
    {"name": "match",
     "type": {"type": "enum", "name": "HandshakeMatch",
              "symbols": ["BOTH", "CLIENT", "NONE"]}},
    {"name": "serverProtocol",
     "type": ["null", "string"]},
    {"name": "serverHash",
     "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
     "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
EOJ
	# avro rpc meta data schema
	$self->{_meta_schema} = Avro::Schema->parse(<<EOJ);
{
  "type": "map",
  "values": "bytes"
}
EOJ

	# go through all known protocols and put their parsed representation and MD5-hash into an arry for easier use
	for (my $i = 0; $i < scalar(@{$self->{_proto_map}}); $i+=3) {
		$self->{_proto_map}->[$i+1] = Avro::Protocol->parse($self->{_proto_map}->[$i]);
		$self->{_proto_map}->[$i+2] = Digest::MD5::md5($self->{_proto_map}->[$i]);
		$self->{_server_proto} = $i;
	}
	
	# prepare handshake request without protocol, this wont change so it makes sense to not do this live
	my $enc = '';
	Avro::BinaryEncoder->encode(
		schema => $self->{_handshake_request},
		data => { clientHash => $self->{_proto_map}->[2], serverHash => $self->{_proto_map}->[$self->{_server_proto}+2], clientProtocol => undef, meta => {} },
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	Avro::BinaryEncoder->encode(
		schema => $self->{_meta_schema},
		data => {},
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	$self->{_handshake_prefix} = $enc;

	# prepare handshake request with protocol, this wont change so it makes sense to not do this live
	$enc = '';
	Avro::BinaryEncoder->encode(
		schema => $self->{_handshake_request},
		data => { clientHash => $self->{_proto_map}->[2], serverHash => $self->{_proto_map}->[$self->{_server_proto}+2], clientProtocol => $self->{_proto_map}->[0], meta => {} },
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	Avro::BinaryEncoder->encode(
		schema => $self->{_meta_schema},
		data => {},
		emit_cb => sub { $enc .= ${ $_[0] } },
	);
	$self->{_handshake_prefix_with_proto} = $enc;
}

1;
