package GreenBuckets::Agent::Dummy;

use strict;
use warnings;
use GreenBuckets;
use Furl::Response;
use Log::Minimal;
use Data::Dumper;
use Class::Accessor::Lite (
    new => 1,
    ro  => [qw/user passwd/]
);

sub get {
    my $self = shift;
    my $urls = shift;
    debugf "method:get args:%s", $urls;
    Furl::Response->new(1,200,"OK",['date','Wed, 27 Apr 2011 06:13:30 GMT','server','Apache','last-modified','Thu, 30 Oct 2008 01:12:04 GMT','etag','"42734-1b2-45a6e2b2b4100"','accept-ranges','bytes','content-length','434','cache-control','max-age=31536000','expires','Thu, 26 Apr 2012 06:13:30 GMT','vary','Accept-Encoding','keep-alive','timeout=15, max=100','connection','Keep-Alive','content-type','text/html; charset=UTF-8'], Dumper($urls));
}

sub put {
    my $self = shift;
    my $urls = shift;
    my $content_ref = shift;
    debugf "method:put args:%s", $urls;
    1;
}

sub delete {
    my $self = shift;
    my $urls = shift;
    debugf "method:del args:%s", $urls;
    1;
}

1;

