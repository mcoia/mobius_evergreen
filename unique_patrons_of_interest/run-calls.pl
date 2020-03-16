#!/usr/bin/perl
use strict; use warnings;
use OpenSRF::AppSession;
use OpenSRF::System;
use OpenSRF::Utils::JSON;
require '/openils/bin/oils_header.pl';
use vars qw/$authtoken/;

my $days_back = 15;
my $fine_limit = '25';
my $method = 'open-ils.collections.users_of_interest.retrieve';


die "usage: $0 <config> <username> <password> <lib>" unless $ARGV[3];
osrf_connect($ARGV[0]);
oils_login($ARGV[1], $ARGV[2]);
my $lib = $ARGV[3];

my $ses = OpenSRF::AppSession->create('open-ils.collections');
my $req = $ses->request($method, $authtoken, $days_back, $fine_limit, $lib);
my @data;

while(my $resp = $req->recv(timeout=>7200)) {
        push(@data, $resp->content);
}

open(F, ">data-$lib");
print F OpenSRF::Utils::JSON->perl2JSON(\@data);
close(F);
