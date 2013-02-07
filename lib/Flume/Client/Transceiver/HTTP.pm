#!/usr/bin/perl

package Flume::Client::Transceiver::HTTP;

=head2 Flume::Client::Transceiver::HTTP

HTTP Transceiver class for the FlumeOG client.

It uses HTTP::Tiny.

=cut

=head3 new

Constructor takes the following arguments:

	host	-	server hostname (default is 127.0.0.1)

	port	-	server port (default is 3452)

	path	-	path to request on server (default is /)
	
	timeout	-	timeout in seconds (default is 10)

	secure	-	if set, does a request using ssl

=cut

sub new {
	my $class = shift;
	my (%params) = @_;
	if (not exists $params{host}) {
		$params{host} = '127.0.0.1';
	}
	if (not exists $params{port}) {
		$params{port} = 3452;
	}
	if (not exists $params{url}) {
		$params{path} = '/';
	}
	if (not exists $params{timeout}) {
		$params{timeout} = 10;
	}
	require HTTP::Tiny;
	$params{_http} = HTTP::Tiny->new(timeout => $params{timeout});
	$params{_url} = (exists $params{secure}? 'https://' : 'http://').$params{host}.':'.$params{port}.$params{path};
	bless \%params, $class;
}

=head3 request

Execute HTTP POST-request on server.

Takes the POST request body and returns a response (hash with result values, look them up in HTTP::Tiny).

=cut

sub request {
	my ($self, $content) = @_;
	return $self->{_http}->request('POST', $self->{_url}, {headers => {'Accept-Encoding' => 'identity', 'Content-Type' => 'avro/binary', 'Content-Length' => length($content)}, content => $content});
}

1;
