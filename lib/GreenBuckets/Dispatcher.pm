package GreenBucktes::Dispatcher;

use strict;
use warnings;
use utf8;
use Plack::Builder;
use Scope::Container;
use GreenBucktes::Dispatcher::Request;
use GreenBucktes::Dispatcher::Response;
use GreenBucktes::Dispatcher::Connection;
use GreenBucktes::Model;
use Class::Accessor::Lite (
    new => 1,
);

sub get_object {
    my ($sef, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};
    $self->model->get_object($bucket_name, $filename);
}

sub put_object {
    my ($sef, $c) = @_;
    my $bucket = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};

    $self->model->put_object($bucket_name, $filename, $fh);
}


sub delete_object {
    my ($sef, $c) = @_;
    my $bucket = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};

    $self->model->put_object($bucket_name, $filename);
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

1;

