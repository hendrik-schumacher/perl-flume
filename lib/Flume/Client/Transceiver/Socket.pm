#!/usr/bin/perl

package Flume::Client::Transceiver::Socket;

=head2 Flume::Client::Transceiver::Socket

Socket Transceiver class for the FlumeNG client.

It uses IO::Socket.

=cut

=head3 new

Constructor takes the following arguments:

	host	-	server hostname (default is 127.0.0.1)

	port	-	server port (default is 3452)

	timeout	-	timeout in seconds (default is 10)

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
	if (not exists $params{timeout}) {
		$params{timeout} = 10;
	}
	require IO::Socket;
	require IO::Select;
	bless \%params, $class;
}

=head3 request

Connect to the server on a TCP socket, send the given content and return the response.

The response sent by the server has to be in Netty Transceiver format so we know how much
data to receive.

The implementation of the Netty Transceiver protocol is very basic. It does not support
fragmented frames nor does it any error checking. It also sits there waiting for the
response (blocking until timeout) while it would be possible to send further requests to the server.
Since it sends the requests one by one it also does not check the serial number of the
responses. For a better (eg asynchronous) implementation it would be neccessary to hold
a list of sent requests and match the received responses to them (so it would be possible
to re-send with protocol if required and call a specific callback given with the request).

=cut

sub request {
	my ($self, $content) = @_;
	my $response = '';
	eval {
		my $remote = IO::Socket::INET->new(
			Proto    => "tcp",
			PeerAddr => $self->{host},
			PeerPort => $self->{port},
			Timeout  => $self->{timeout}
		) or die "connect failed";
		my $timeout = time()+$self->{timeout};
		$remote->autoflush(1);
		#print $remote $content;
		my $bytes_to_write = length($content);
		my $selector = IO::Select->new($remote);
		while (($bytes_to_write > 0) && (time() < $timeout)) {
			my @ready = $selector->can_write($self->{timeout});
			if ($ready[0] == $remote) {
				my $bytes_written = syswrite($remote, $content, $bytes_to_write < 8192? $bytes_to_write : 8192, length($content) - $bytes_to_write);
				if ($bytes_written) {
					$bytes_to_write -= $bytes_written;
				}
				else {
					die "write failed: $!";
				}
			}
		}
		
		my $frames_expected = 0;
		my $bytes_expected = 4;
		my $bytes_read = 0;
		my $state = 0;
		my $offset = 0;
		while (($state < 4) && (time() < $timeout)) {
			my @ready = $selector->can_read($self->{timeout});
			if ($ready[0] == $remote) {
				$bytes_read += sysread($remote, $response, $bytes_expected, $offset) or die "read failed";
			}
			if ($bytes_read >= $bytes_expected) {
				$bytes_read -= $bytes_expected;
				my $new_offset = $offset + $bytes_expected;
				$bytes_expected = 4;	# default value
				if ($state == 0) {
					# just got serial, doesnt interest us here
				}
				elsif ($state == 1) {
					# just got number of frames
					$frames_expected = unpack('N', substr($response, $offset));
				}
				elsif ($state == 2) {
					# just got length of next frame
					$bytes_expected = unpack('N', substr($response, $offset));
				}
				elsif ($state == 3) {
					# just got frame payload
					$frames_expected--;
					if ($frames_expected > 0) {
						$state = 1; # set to 1 so it will be 2 after increment
					}
					# otherwise state will be 4 so we will exit the loop
				}
				$state++;
				$offset = $new_offset;
			}
		}
		if (time() >= $timeout) {
			die "timeout";
		}
	};
	if ($@) {
		return undef;
		#die unless $@ eq "alarm\n";   # propagate unexpected errors
	}
	return $response;
}

1;
