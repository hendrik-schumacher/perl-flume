#!/usr/bin/perl

package Flume::Client;

use strict;
use 5.008_001;
our $VERSION = '0.01';

use Flume::Client::Requestor;
use Flume::Client::Transceiver;

# Flume::Client.pm
#
# Copyright (c) 2012 Hendrik Schumacher <hendrik.schumacher@meetrics.de>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.

=head1 Flume::Client

Avro RPC classes making up a sample Flume Client working with FlumeOG and FlumeNG Avro Sources.

This is a very rough but fully functional implementation. Especially the socket transceiver may still be
improved.

=cut

1;

__END__

=head2 Usage:

=head3 FlumeNG Client

	use Flume::Client;

	# Sample call for FlumeNG
	my $ng_client = Flume::Client::Transceiver::Socket->new(port => 41414);
	my $ng_requestor = Flume::Client::Requestor::FlumeNG->new(client => $ng_client);
	my ($result, $response) = $ng_requestor->request('appendBatch', [{ headers => {}, body => "hello, this is sent from perl (using FlumeNG)"}, { headers => {}, body => "this is also sent from perl"}, { headers => {}, body => "and this too"}]);
	print "$response\n";	# response will be 'OK' on success

=head3 FlumeOG Client

	use Flume::Client;

	# Sample call for FlumeOG
	my $og_client = Flume::Client::Transceiver::HTTP->new(port => 41415);
	my $og_requestor = Flume::Client::Requestor::FlumeOG->new(client => $og_client);
	$og_requestor->request('append', { timestamp => 123, nanos => 234, priority => "FATAL", host => "myhost", body => "hello, this is sent from perl (using FlumeOG)", fields => {test => "test"} });

=head1 Setup

To use this module you need

Digest::MD5 and HTTP::Tiny available from CPAN

the perl-avro package (see https://github.com/yannk/perl-avro)

To successfully install the perl-avro package I needed to install the following modules from CPAN:

inc::Module::Install, Module::Install::ReadmeFromPod, Module::Install::Repository, Regexp::Common, Object::Tiny, JSON::XS, Test::Exception, Try::Tiny, Error::Simple

=head1 AUTHOR

Hendrik Schumacher <hendrik.schumacher@meetrics.de>

=head1 COPYRIGHT

Copyright (c) 2012 Hendrik Schumacher <hendrik.schumacher@meetrics.de>. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
