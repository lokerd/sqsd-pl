#!/usr/bin/env perl
use strict;
use warnings;
use JSON::Parse qw(json_file_to_perl parse_json);

my $file = "message.json";

my $href = json_file_to_perl($file);

my $message = parse_json($href->{Message});

for my $key (keys $message) {
	print "$key - $message->{$key}\n";
}
