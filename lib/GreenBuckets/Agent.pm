package GreenBuckets::Agent;

use strict;
use warnings;
use GreenBuckets;
use Furl;
use Net::DNS::Lite qw//;
use Log::Minimal;
use MIME::Base64;
use URI::Escape;
use Plack::TempBuffer;
use Class::Accessor::Lite (
    new => 1,
    ro  => [qw/user passwd timeout_for_get timeout_for_put/]
);

sub furl_for_get {
    my $self = shift;

    my $user = $self->user;
    my $passwd = $self->passwd;

    my @headers;
    if ( $user && $passwd ) {
        push @headers , 'Authorization', 'Basic ' . MIME::Base64::encode("$user:$passwd", '');
    }

    $self->{furl_for_get} ||= Furl->new(
        inet_aton => \&Net::DNS::Lite::inet_aton,
        timeout   => $self->timeout_for_get,
        agent => 'GreenBucketAgent/$GreenBucket::VERSION',
        headers => \@headers,
    );
}


sub furl {
    my $self = shift;

    my $user = $self->user;
    my $passwd = $self->passwd;

    my @headers;
    if ( $user && $passwd ) {
        push @headers , 'Authorization', 'Basic ' . MIME::Base64::encode("$user:$passwd", '');
    }

    $self->{furl_for_put} ||= Furl->new(
        inet_aton => \&Net::DNS::Lite::inet_aton,
        timeout   => $self->timeout_for_put,
        agent => 'GreenBucketAgent/$GreenBucket::VERSION',
        headers => \@headers,
    );
}


sub get {
    my $self = shift;
    my $urls = shift;
    my ($bucket_name, $filename, $query_string)  = @_;
    my @urls = ref $urls ? @$urls : ($urls);
    
    my $original_path;
    if ( $bucket_name && $filename ) {
        $original_path = uri_escape_utf8($bucket_name . '/' . $filename);
    }

    my $res;
    my $buf;
    for my $url ( @urls ) {
        $url .= '?' . $query_string if $query_string;
        my @headers;
        push @headers, 'X-GreenBuckets-Original-Path', $original_path if $original_path;
        $buf = Plack::TempBuffer->new;
        $res = $self->furl_for_get->request(
            method => 'GET',
            url => $url,
            headers => \@headers,
            write_code => sub { $buf->print($_[3]) },
        );
        infof("failed get: %s / %s", $url, $res->status_line) if ! $res->is_success;
        last if $res->is_success; 
    }
    return ($res,$buf->rewind);
}

sub put {
    my $self = shift;
    my $urls = shift;
    my $content_fh = shift;
    my $success = shift;

    my @urls = ref $urls ? @$urls : ($urls);
    $success ||= scalar @urls;

    my @res;
    for my $url ( @urls ) {
        debugf("put: %s", $url);
        $content_fh->seek(0,0);
        my $res = $self->furl->put( $url, [], $content_fh );
        infof("failed put: %s / %s", $url, $res->status_line) if ! $res->is_success;
        push @res, $res;
    }

    my @success = grep { $_->is_success } @res;
    return @success == $success;
}

sub delete {
    my $self = shift;
    my $urls = shift;
    my @urls = ref $urls ? @$urls : ($urls);

    for my $url ( @urls ) {
        my $res = $self->furl->delete( $url );
        warnf("failed delete: %s / %s", $url, $res->status_line) if ! $res->is_success;
    }
    1;
}

1;


