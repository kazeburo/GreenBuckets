package GreenBuckets::Agent::Dummy;

use strict;
use warnings;
use GreenBuckets;
use Furl::Response;
use Log::Minimal;
use Data::Dumper;
use URI::Escape;
use Class::Accessor::Lite (
    new => 1,
    ro  => [qw/user passwd/]
);

sub get {
    my $self = shift;
    my $urls = shift;
    my ($bucket_name, $filename, $query_string)  = @_;

    my $original_path;
    if ( $bucket_name && $filename ) {
        $original_path = uri_escape_utf8($bucket_name . '/' . $filename);
    }

    my @urls = map {
        my $url = $_;
        $url .= '?' . $query_string if $query_string;
        $url;
    } ref $urls ? @$urls : ($urls);
    debugf "method:get args:%s %s", \@urls, $original_path;
    return (Furl::Response->new(1,200,"OK",['date','Wed, 27 Apr 2011 06:13:30 GMT','server','Apache','last-modified','Thu, 30 Oct 2008 01:12:04 GMT','etag','"42734-1b2-45a6e2b2b4100"','accept-ranges','bytes','content-length','434','cache-control','max-age=31536000','expires','Thu, 26 Apr 2012 06:13:30 GMT','vary','Accept-Encoding','keep-alive','timeout=15, max=100','connection','Keep-Alive','content-type','text/html; charset=UTF-8'], ''),Dumper([\@urls,$original_path]));
}

sub put {
    my $self = shift;
    my $urls = shift;
    my $content_ref = shift;
    my @urls = ref $urls ? @$urls : ($urls);
    return if map { m!^http://127\.0\.0\.1:8080/4! } @urls;
    debugf "method:put args:%s", \@urls;
    1;
}

sub delete {
    my $self = shift;
    my $urls = shift;
    my @urls = ref $urls ? @$urls : ($urls);
    debugf "method:del args:%s", \@urls;
    1;
}

1;

