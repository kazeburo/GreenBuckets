package GreenBucktes::Model;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use Scope::Container;
use Scope::Container::DBI;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use GreenBuckets::Schema;
use GreenBuckets::Dispatcher::Response;
use List::Util qw/shuffle/;
use Try::Tiny;
use HTTP::Exception;
use Log::Minimal;
use Mouse;

has 'config' => (
    is => 'ro',
    isa => 'GreenBuckets::Config',
    required => 1,
);

sub slave {
    my $self = shift;
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    my $dbh = Scope::Container::DBI->new($self->config->slave);
    GreenBucktes::Schema->new(dbh=>$dbh, readonly=>1);
}

sub master {
    my $self = shift;
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    my $dbh = Scope::Container::DBI->new($self->config->master);
    GreenBucktes::Schema->new(dbh=>$dbh);
}

sub agent {
    my $self = shift;
    return $self->{_agent} if $self->{_agent};
    if ( $self->config->dav_user ) {
        $self->{_agent} = GreenBucktes::Agent->new(
            user =>  $self->config->dav_user,
            passwd => $self->config->dav_passwd,
        );
    }
    else {
        $self->{_agent} = GreenBucktes::Agent->new();
    }
    $self->{_agent};
}

sub res_ok {
    my $self = shift;
    my $res = GreenBucktes::Dispatcher::Response->new(200);
    $res->body("OK");
}

sub get_object {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $sc = start_scope_container();

    my $slave = $self->slave;
    my $bucket = $slave->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(404) unless $bucket; # 404
    HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    HTTP::Exception->throw(404) if $bucket->{deleted}; # 404;

    my @uri = $slave->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    undef $sc;

    my @r_uri = grep { $_->{can_read} } @uri;
    if ( !@r_uri ) {
        warnf "all storage cannot read %s", \@uri;
        HTTP::Exception->throw(500);
    }

    my $res = $self->agent->get(\@r_uri);
    if ( !$res->is_success ) {
        warnf "all storage cannot get %s, last status_line: %s", \@uri, $res->status_line;
        HTTP::Exception->throw(500);
    }

    my $r_res = GreenBucktes::Dispatcher::Response->new(200);
    for my $header ( qw/server content_type last_modified/ ) {
        $r_res->header($header) = $res->header($header);
    }
    $r_res->body($res->body);
    $r_res;
}

sub get_bucket {
    my $self = shift;
    my ($bucket_name) = @_;

    my $sc = start_scope_container();

    my $slave = $self->slave;
    my $bucket = $slave->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(404) unless $bucket; # 404
    HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    HTTP::Exception->throw(503) if $bucket->{deleted}; # 404;

    return $self->res_ok;
}

sub put_object {
    my $self = shift;
    my ($bucket_name, $filename, $content_ref) = @_;

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(403) if ! $bucket->{enabled};
    HTTP::Exception->throw(503) if $bucket->{deleted}; #XXX 

    if ( $bucket && $self->retrieve_object( bucket_id => $bucket->{id}, filename => $filename ) ) {
        warnf "duplicated upload %s/%s", $bucket, $filename;
        HTTP::Exception->throw(409);
    }

    my @f_nodes = $master->retrieve_fresh_nodes(
        bucket_name => $bucket_name, 
        filename => $filename,
        having => $self->config->{replica}
    );
    undef $sc;

    my $try=3;
    my $gid;
    my $rid;
    for my $f_node ( @f_nodes ) {
        
        my $result = $self->agent->put($f_node->{uri}, $content_ref);

        if ( $result ) {
            infof "%s/%s was uploaded to group_id:%s", $bucket, $filename, $f_node->{gid};
            $gid = $f_node->{gid};
            $rid = $f_node->{rid};
            last;
        }

        infof "Failed upload %s/%s to group_id:%s", $bucket, $filename, $f_node->{gid};
        $self->enqueue('delete_files', $f_node->{uri});

        --$try;
        if ( $try == 0 ) {
            warnf "try time exceed for upload %s/%s", $bucket, $filename;
            last;
        }
    }

    if ( !$gid ) {
        warnf "Upload failed %s/%s", $bucket, $filename;
        return HTTP::Exception->throw(500);
    }

    my $sc2 = start_scope_container();
    $master->insert_object( 
        gid => $gid,
        rid => $rid,
        bucket_name => $bucket_name,
        filename => $filename
    );

    return $self->res_ok;
}

sub delete_object {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(404) unless $bucket; # 404
    HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    HTTP::Exception->throw(404) if $bucket->{deleted}; # 404;

    my @uri = $master->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    $master->delete_object(
        bucket_id => $bucket->{id}, 
        filename => $filename
    );

    undef $sc;

    # remove file
    my @r_uri = map { $_->{uri} }
        grep { $_->{can_read} && $_->{is_fresh} } @uri;
    $self->enqueue('delete_files', \@r_uri);

    return $self->res_ok;
}

sub jobq_delete_files {
    my $self = shift;
    my $r_uri = shift;
    $self->agent->delete($r_uri);
}

sub dequeue {
    my $self = shift;
    my $queue = $self->master->retrieve_queue;
    return unless $queue;
    my $args = Data::MessagePack->unpack($queue->{args});
    my $func = $queue->{func};

    debugf "execute func:%s with args:%s", $func, $args;
    my $subref = $self->can("jobq_". $func);

    if ( !$subref ) {
        croak "func:$func not found";
    }

    try {
        $subref->($self, $queue->{args});
    } catch {
        croak "func:$func failed: ". $_;
    };
    1;
}

sub enqueue {
    my $self = shift;
    my ($func, $args ) = @_;
    $args = Data::MessagePack->pack($args);
    $self->master->create_queue( func => $func, args => $args );
}

__PACKAGE__->meta->make_immutable();
1;

