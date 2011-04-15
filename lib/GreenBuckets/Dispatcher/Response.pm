package GreenBucktes::Dispatcher::Response;

use strict;
use warnings;
use parent qw/Plack::Response/;
use Encode;

sub _body {
    my $self = shift;
    my $body = $self->body;
       $body = [] unless defined $body;
    if (!ref $body or Scalar::Util::blessed($body) && overload::Method($body, q("")) && !$body->can('getline')) {
        return [ Encode::encode_utf8($body) ];
    } else {
        return $body;
    }
}

sub redirect {
    my $self = shift;
    if ( @_ ) {
        $self->SUPER::redirect(@_);
        return $self;
    }
    $self->SUPER::redirect();
}

sub server_error {
    my $self = shift;
    my $error = shift;
    $self->status( 500 );
    $self->content_type('text/html; charset=UTF-8');
    $self->body( $error || 'Internal Server Error' );
    $self;
}

sub not_found {
    my $self = shift;
    my $error = shift;
    $self->status( 500 );
    $self->content_type('text/html; charset=UTF-8');
    $self->body( $error || 'Not Found' );
    $self;
}



1;
