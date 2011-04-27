package GreenBuckets::Model;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use Scope::Container;
use Scope::Container::DBI;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use GreenBuckets::Schema;
use GreenBuckets::Dispatcher::Response;
use GreenBuckets::Exception;
use Class::Load qw/load_class/;
use List::Util qw/shuffle/;
use Try::Tiny;
use Log::Minimal;
use Mouse;

has 'config' => (
    is => 'ro',
    isa => 'GreenBuckets::Config',
    required => 1,
);

__PACKAGE__->meta->make_immutable();

sub slave {
    my $self = shift;
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    my $dbh = Scope::Container::DBI->connect(@{$self->config->slave});
    GreenBuckets::Schema->new(dbh=>$dbh, readonly=>1);
}

sub master {
    my $self = shift;
    local $Scope::Container::DBI::DBI_CLASS = 'DBIx::Sunny';
    my $dbh = Scope::Container::DBI->connect(@{$self->config->master});
    GreenBuckets::Schema->new(dbh=>$dbh);
}

sub agent {
    my $self = shift;
    return $self->{_agent} if $self->{_agent};
    my $agent_class = $self->config->agent_class;
    load_class($agent_class) or croak $!;
    if ( $self->config->dav_user ) {
        $self->{_agent} = $agent_class->new(
            user =>  $self->config->dav_user,
            passwd => $self->config->dav_passwd,
        );
    }
    else {
        $self->{_agent} = $agent_class->new();
    }
    $self->{_agent};
}

sub res_ok {
    my $self = shift;
    my $res = GreenBuckets::Dispatcher::Response->new(200);
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
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(404) if $bucket->{deleted}; # 404;

    my @uri = $slave->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    undef $sc;
    http_croak(404) if !@uri;
    
    my @r_uri = grep { $_->{can_read} } @uri;
    http_croak(500, "all storage cannot read %s", \@uri) if ! @r_uri;

    my $res = $self->agent->get(\@r_uri);
    http_croak(500, "all storage cannot get %s, last status_line: %s", \@uri, $res->status_line)
        if !$res->is_success; 

    my $r_res = GreenBuckets::Dispatcher::Response->new(200);
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
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(503) if $bucket->{deleted}; # 404;

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
    http_croak(403) if ! $bucket->{enabled};
    http_croak(503) if $bucket->{deleted}; #XXX 

    if ( $bucket && $master->retrieve_object( bucket_id => $bucket->{id}, filename => $filename ) ) {
        http_croak(409, "duplicated upload %s/%s", $bucket_name, $filename);
    }

    my @f_nodes = $master->retrieve_fresh_nodes(
        bucket_id => $bucket->{id}, 
        filename => $filename,
        having => $self->config->{replica}
    );
    undef $sc;
    http_croak(500, "cannot find fresh_nodes") if !@f_nodes;

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

    http_croak(500,"Upload failed %s/%s", $bucket, $filename) if !$gid;

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
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(404) if $bucket->{deleted}; # 404;

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


1;

