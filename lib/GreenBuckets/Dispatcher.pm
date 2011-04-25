package GreenBuckets::Dispatcher;

use strict;
use warnings;
use utf8;
use Plack::Builder;
use GreenBuckets::Dispatcher::Request;
use GreenBuckets::Dispatcher::Response;
use GreenBuckets::Dispatcher::Connection;
use GreenBuckets::Model;
use Class::Accessor::Lite (
    new => 1,
);

sub model {
    my $self = shift;
    $self->{_model} ||= GreenBuckets::Model->new(
        config => ... 
    );
    $self->{_model};
}

sub get_object {
    my ($self, $c) = @_;
    my ($bucket_name,$filename) = @{$c->args->{splat}};
    $self->model->get_object($bucket_name, $filename);
}

sub put_object {
    my ($sef, $c) = @_;
    my ($bucket_name, $filename) = @{$c->args->{splat}};

    my $content = $c->req->raw_body;

    $self->model->put_object($bucket_name, $filename, \$content);
}

sub get_bucket {
    my ($sef, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    $self->model->get_bucket($bucket_name);
}

sub delete_object {
    my ($sef, $c) = @_;
    my ($bucket_name, $filename) = @{$c->args->{splat}};

    $self->model->put_object($bucket_name, $filename);
}

sub manip_bucket {
    my ($sef, $c) = @_;
    my ($bucket_name) = @{$c->args->{splat}};
    return $c->res->server_error;
}

sub build_app {
    my $self = shift;

    #router
    my $router = Router::Simple->new;

    # get
    $router->connect(
        '/([a-zA-Z0-9][a-zA-Z0-9_\-]+)/*',
        { action => 'get_object' },
        { method => ['GET','HEAD'] }
    );

    # get dir
    $router->connect(
        '/([a-zA-Z0-9][a-zA-Z0-9_\-]+)/',
        { action => 'get_bucket' },
        { method => ['GET','HEAD'] }
    );

    # put
    $router->connect(
        '/([a-zA-Z0-9][a-zA-Z0-9_\-]+)/*',
        { action => 'put_object' },
        { method => ['PUT'] }
    );

    # delete
    $router->connect(
        '/([a-zA-Z0-9][a-zA-Z0-9_\-]+)/*',
        { action => 'delete_object' },
        { method => ['DELETE'] }
    );

    # post manip
    $router->connect(
        '/([a-zA-Z0-9][a-zA-Z0-9_\-]+)/',
        { action => 'manip_bucket' },
        { method => ['POST'] }
    );

    sub {
        my $env = shift;
        my $psgi_res;

        my $s_req = GreenBuckets::Dispatcher::Request->new($env);
        my $s_res = GreenBuckets::Dispatcher::Response->new(200);
        $s_res->content_type('text/html; charset=UTF-8');

        my $c = GreenBuckets::Dispatcher::Connection->new({
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

