package GreenBuckets::Dispatcher;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use Scalar::Util qw/blessed/;
use Plack::Builder;
use Plack::Builder::Conditionals -prefix => 'c';
use Plack::Loader;
use Router::Simple;
use File::Temp;
use GreenBuckets::Dispatcher::Request;
use GreenBuckets::Dispatcher::Response;
use GreenBuckets::Dispatcher::Connection;
use GreenBuckets::Model;
use Log::Minimal;
use JSON;
use Mouse;

has 'config' => (
    is => 'ro',
    isa => 'GreenBuckets::Config',
    required => 1,
);

has 'model' => (
    is => 'ro',
    isa => 'GreenBuckets::Model',
    lazy_build => 1,
);

sub _build_model {
    my $self = shift;
    GreenBuckets::Model->new( config => $self->config );
}

sub get_object {
    my ($self, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};
    $self->model->get_object($bucket_name, $filename);
}

sub put_object {
    my ($self, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};
    my $content = $c->req->raw_body;

    $self->model->put_object($bucket_name, $filename, \$content);
}

sub get_bucket {
    my ($self, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    $self->model->get_bucket($bucket_name);
}

sub delete_object {
    my ($self, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my ($filename) = @{$c->args->{splat}};

    $self->model->delete_object($bucket_name, $filename);
}

sub manip_bucket {
    my ($self, $c) = @_;
    my $bucket_name = $c->args->{bucket};
    my $content = $c->req->raw_body;
    return $c->res->server_error unless $content;
    my $args = decode_json( Encode::decode_utf8($content) );

    if ( $args->{method} eq 'delete_bucket' ) {
        return $self->model->delete_bucket($bucket_name);
    }
    if ( $args->{method} eq 'delete_object' ) {
        my @path;
        foreach my $ppath ( @{$args->{args}} ) {
            $ppath =~ s!^/!!;
            push @path, $ppath;
        }
        return $self->model->delete_object_multi($bucket_name, \@path);
    }
    else {
        return $c->res->server_error
    }
}

sub build_app {
    my $self = shift;

    #router
    my $router = Router::Simple->new;

    # get
    $router->connect(
        '/{bucket:[a-zA-Z0-9][a-zA-Z0-9_\-]+}/*',
        { action => 'get_object' },
        { method => ['GET','HEAD'] }
    );

    # get dir
    $router->connect(
        '/{bucket:[a-zA-Z0-9][a-zA-Z0-9_\-]+}/',
        { action => 'get_bucket' },
        { method => ['GET','HEAD'] }
    );

    # put
    $router->connect(
        '/{bucket:[a-zA-Z0-9][a-zA-Z0-9_\-]+}/*',
        { action => 'put_object' },
        { method => ['PUT'] }
    );

    # delete
    $router->connect(
        '/{bucket:[a-zA-Z0-9][a-zA-Z0-9_\-]+}/*',
        { action => 'delete_object' },
        { method => ['DELETE'] }
    );

    # post manip
    $router->connect(
        '/{bucket:[a-zA-Z0-9][a-zA-Z0-9_\-]+}/',
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
            croak('uri match but no action found') unless $code;

            $c->args($p);

            my $res = $code->($self, $c );
            croak( "undefined response") if ! defined $res;

            my $res_t = ref($res) || '';
            if ( blessed $res && $res->isa('Plack::Response') ) {
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
                croak("unknown response type: $res, $res_t");
            }
        }
        else {
            # router not match
            $psgi_res = $c->res->not_found()->finalize;
        }
        
        $psgi_res;
    };
}

sub run {
    my $self = shift;

    my $dispatcher_status_access = $self->config->dispatcher_status_access;
    my $front_proxy = $self->config->front_proxy;

    my $app = $self->build_app;
    $app = builder {
        if ( $ENV{PLACK_ENV} eq "development" ) {
            enable "StackTrace";
        }
        enable 'HTTPExceptions';
        enable 'Log::Minimal', autodump => 1;
        if ( @$front_proxy ) {
            enable c_match_if c_addr(@$front_proxy), 'ReverseProxy';
        }
        enable "ServerStatus::Lite",
          path => '/___server-status',
          allow => $dispatcher_status_access,
          scoreboard => File::Temp::tempdir(CLEANUP => 1);
        enable c_match_if c_method('!',qw/GET HEAD/), "Auth::Basic", authenticator => $self->authen_cb;
        $app;
    };

    my $loader = Plack::Loader->load(
        'Starlet',
        port => $self->config->dispatcher_port,
        host => 0,
        max_workers => $self->config->dispatcher_max_worker,
    );

    $loader->run($app);
}

sub authen_cb {
    my $self = shift;
    sub {
        my ($user,$pass) = @_;
        return $user eq $self->config->user && $pass eq $self->config->passwd;
    }
}

1;

