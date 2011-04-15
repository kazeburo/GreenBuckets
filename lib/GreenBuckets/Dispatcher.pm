package GreenBucktes::Dispatcher;

use strict;
use warnings;
use utf8;
use Plack::Builder;
use Class::Accessor::Lite (
    new => 1,
);

sub get_object {
    my ($sef, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};

    my $res = $self->model->get_object($bucket_name, $filename);
    ... #res
}

sub put_object {
    my ($sef, $c) = @_;
    my $bucket = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};

    # read content.
    my $ret = $self->model->put_object($bucket_name, $filename, $fh);

    return 201; #XXX
}

sub delete_object {
    my ($sef, $c) = @_;
    my $bucket = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};
}

sub manip_bucket {
    my ($sef, $c) = @_;
    my $bucket = $c->args->{bucket};
}

sub build_app {
    my $self = shift;

    #router
    my $router = Router::Simple->new;

    # get
    $router->connect(
        '/:bucket/*',
        { action => 'get_object' },
        { method => ['GET','HEAD'] }
    );

    # put
    $router->connect(
        '/:bucket/*',
        { action => 'put_object' },
        { method => ['PUT'] }
    );

    # delete
    $router->connect(
        '/:bucket/*',
        { action => 'delete_object' },
        { method => ['DELETE'] }
    );

    # post manip
    $router->connect(
        '/:bucket/',
        { action => 'manip_bucket' },
        { method => ['POST'] }
    );

    sub {
        my $env = shift;
        my $psgi_res;

        my $s_req = GreenBucktes::Dispatcher::Request->new($env);
        my $s_res = GreenBucktes::Dispatcher::Response->new(200);
        $s_res->content_type('text/html; charset=UTF-8');

        my $c = GreenBucktes::Dispatcher::Connection->new({
            req => $s_req,
            res => $s_res,
            stash => {},
        });

        if ( my $p = $router->match($env) ) {
            my $action = delete $p->{action};
            my $code = $self->can($action);
            Carp::croak('uri match but no action found') unless $code;

            $c->args($p);

            my $res = $code->($self, $c );
            Carp::croak( "undefined response") if ! defined $res;

            my $res_t = ref($res) || '';
            if ( Scalar::Util::blessed $res && $res->isa('Plack::Response') ) {
                $psgi_res = $res->finalize;
            }
            elsif ( $res_t eq 'ARRAY' ) {
                $psgi_res = $res;
            }
            elsif ( !$res_t ) {
                $s_res->body($res);
                $psgi_res = $s_res->finalize;
            }
            else {
                Carp::croak("unknown response type: $res, $res_t");
            }
        }
        else {
            # router not match
            $psgi_res = $c->res->not_found()->finalize;
        }
        
        $psgi_res;
    };
}

package GreenBucktes::Dispatcher::Connection;

use strict;
use warnings;
use Class::Accessor::Lite (
    new => 1,
    rw => [qw/req res stash args/]
);

*request = \&req;
*response = \&res;

package GreenBucktes::Dispatcher::Request;

use strict;
use warnings;
use parent qw/Plack::Request/;
use Hash::MultiValue;
use Encode;

sub body_parameters {
    my ($self) = @_;
    $self->{'shirahata2.body_parameters'} ||= $self->_decode_parameters($self->SUPER::body_parameters());
}

sub query_parameters {
    my ($self) = @_;
    $self->{'shirahata2.query_parameters'} ||= $self->_decode_parameters($self->SUPER::query_parameters());
}

sub _decode_parameters {
    my ($self, $stuff) = @_;

    my @flatten = $stuff->flatten();
    my @decoded;
    while ( my ($k, $v) = splice @flatten, 0, 2 ) {
        push @decoded, Encode::decode_utf8($k), Encode::decode_utf8($v);
    }
    return Hash::MultiValue->new(@decoded);
}
sub parameters {
    my $self = shift;

    $self->env->{'shirahata2.request.merged'} ||= do {
        my $query = $self->query_parameters;
        my $body  = $self->body_parameters;
        Hash::MultiValue->new( $query->flatten, $body->flatten );
    };
}

sub body_parameters_raw {
    shift->SUPER::body_parameters();
}
sub query_parameters_raw {
    shift->SUPER::query_parameters();
}

sub parameters_raw {
    my $self = shift;

    $self->env->{'plack.request.merged'} ||= do {
        my $query = $self->SUPER::query_parameters();
        my $body  = $self->SUPER::body_parameters();
        Hash::MultiValue->new( $query->flatten, $body->flatten );
    };
}

sub param_raw {
    my $self = shift;

    return keys %{ $self->parameters_raw } if @_ == 0;

    my $key = shift;
    return $self->parameters_raw->{$key} unless wantarray;
    return $self->parameters_raw->get_all($key);
}

sub uri_for {
     my($self, $path, $args) = @_;
     my $uri = $self->base;
     $uri->path($path);
     $uri->query_form(@$args) if $args;
     $uri;
}

1;

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

