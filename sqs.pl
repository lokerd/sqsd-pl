#!/usr/bin/env perl
use AWS::SQSD;
use strict;
use warnings;
use lib qw(.);
use Data::Dumper;

# Fast pipes
$| = 1;

# Create SQSD - sets up AWS::CLIWrapper, SQS Queue, Forks
my $sqsd = AWS::SQSD->new();

# Daemon Process
while (1) 
{
	# Try pop a message off the queue
	my $res = $sqsd->get_message();
	# Process it if we find one
        if(@{$res->{Messages}}[0]) {
            $sqsd->process(@{$res->{Messages}}[0]->{Body});
		    $sqsd->rm(@{$res->{Messages}}[0]->{ReceiptHandle});
        }
	# Time till we try again
	sleep 20;
}
