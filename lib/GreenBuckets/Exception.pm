package GreenBuckets::Exception;

use strict;
use warnings;
use HTTP::Exception;
use Log::Minimal;
use parent qw/Exporter/;

our @EXPORT = qw/http_croak/;

sub http_croak {
    my $code = shift;
    if ( @_ ) {
        local $Log::Minimal::TRACE_LEVEL = $Log::Minimal::TRACE_LEVEL + 1;
        $code =~ m!^5! ? critf @_ : warnf @_;
    }
    HTTP::Exception->throw($code);
}

1;
