package GreenBuckets::Agent;

use strict;
use warnings;
use GreenBuckets;
use Furl;
use Net::DNS::Lite qw//;
use Log::Minimal;
use MIME::Base64;
use Class::Accessor::Lite (
    new => 1,
    ro  => [qw/user passwd/]
);

sub furl {
    my $self = shift;

    my $user = $self->user;
    my $passwd = $self->passwd;

    my @headers;
    if ( $user && $passwd ) {
        push @headers , 'Authorization', 'Basic ' . MIME::Base64::encode("$user:$passwd", '');
    }

    $self->{furl} ||= Furl->new(
        inet_aton => \&Net::DNS::Lite::inet_aton,
        timeout   => 10,
        agent => 'GreenBucketAgent/$GreenBucket::VERSION',
        headers => \@headers,
    );
}

sub get {
    my $self = shift;
    my $urls = shift;
    my @urls = ref $urls ? @$urls : ($urls);

    my $res;
    for my $url ( @urls ) {
        $res = $self->furl->get($url);
        infof("failed get: %s / %s", $url, $res->status_line) if ! $res->is_success;
        last if $res->is_success; 
    }
    return $res;
}

sub put {
    my $self = shift;
    my $urls = shift;
    my $content_ref = shift;
 
    my @urls = ref $urls ? @$urls : ($urls);

    my @res;
    for my $url ( @urls ) {
        debugf("put: %s", $url);
        my $res = $self->furl->put( $url, [], $$content_ref );
        infof("failed put: %s / %s", $url, $res->status_line) if ! $res->is_success;
        push @res, $res;
    }

    my @success = grep { $_->is_success } @res;
    return @success == @urls;
}

sub delete {
    my $self = shift;
    my $urls = shift;
    my @urls = ref $urls ? @$urls : ($urls);

    for my $url ( @urls ) {
        my $res = $self->furl->delete( $url );
        debugf("delete: %s / %s", $url, $res->status_line);
    }
    1;
}

1;


