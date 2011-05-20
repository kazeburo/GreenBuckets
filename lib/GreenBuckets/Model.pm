package GreenBuckets::Model;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use Scope::Container;
use Scope::Container::DBI;
use GreenBuckets::Schema;
use Plack::MIME;
use GreenBuckets::Dispatcher::Response;
use GreenBuckets::Exception;
use Class::Load qw/load_class/;
use List::Util qw/shuffle/;
use Try::Tiny;
use Log::Minimal;
use Data::MessagePack;
use Mouse;

our $MAX_RETRY_JOB = 3600;

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

    undef $slave;
    http_croak(404) if !@uri;
    
    my @r_uri = map { $_->{uri} } grep { $_->{can_read} && !$_->{remote} } @uri;
    http_croak(500, "all storage cannot read %s", \@uri) if ! @r_uri;

    my $res = $self->agent->get(\@r_uri);
    http_croak(500, "all storage cannot get %s, last status_line: %s", \@uri, $res->status_line)
        if !$res->is_success; 

    my $r_res = GreenBuckets::Dispatcher::Response->new(200);
    for my $header ( qw/server last-modified expires cache-control/ ) {
        $r_res->header($header, $res->header($header));
    }
    $r_res->content_type( Plack::MIME->mime_type($filename) || 'text/plain' );
    $r_res->body($res->body);
    $r_res;
}

sub get_bucket {
    my $self = shift;
    my ($bucket_name) = @_;

    my $bucket = $self->slave->select_bucket(
        name => $bucket_name
    );
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(503) if $bucket->{deleted}; # 404;

    return $self->res_ok;
}

sub enable_bucket {
    my $self = shift;
    my ($bucket_name, $enable) = @_;
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    http_croak(404) unless $bucket; # 404
    http_croak(503) if $bucket->{deleted}; # 404;

    $master->enable_bucket(
        bucket_id => $bucket->{id},
        enabled => $enable
    );

    return $self->res_ok;
}


sub put_object {
    my $self = shift;
    my ($bucket_name, $filename, $content_ref, $overwrite_ok) = @_;

    my $master = $self->master;
    my $bucket = $master->retrieve_or_insert_bucket(
        name => $bucket_name
    );
    http_croak(500, "Cannot retrieve bucket %s", $bucket_name) unless $bucket;
    http_croak(403) if ! $bucket->{enabled};
    http_croak(503) if $bucket->{deleted}; #XXX 

    my $lock = try {
        $master->putlock(
            bucket_id => $bucket->{id},
            filename => $filename        
        );
    };
    http_croak(423) if ! $lock;

    my $locked = GreenBuckets::Model::PutLock->new(
        model => $self,
        lock => $lock
    );
    
    my @exists_nodes = $master->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename
    );

    if ( @exists_nodes && !$overwrite_ok) {
        http_croak(409, "duplicated upload %s/%s", $bucket_name, $filename);
    }

    my $object_id;
    my %fresh_nodes_args = (
        bucket_id => $bucket->{id}, 
        filename => $filename,
        having => $self->config->replica,
    );
    if ( @exists_nodes ) {
        $object_id = $exists_nodes[0]->{object_id}; #objects.id
        $fresh_nodes_args{previous_rid} = $exists_nodes[0]->{rid}; # objects.rid
    }

    my @f_nodes = $master->retrieve_fresh_nodes(\%fresh_nodes_args);
    undef $master;
    http_croak(500, "cannot find fresh_nodes") if !@f_nodes;

    my $gid;
    my $rid;
    my @copied;
    my @replicate_to;
    for my $f_node ( @f_nodes ) {
        my @nodes = @{$f_node->{uri}};
        my @first_nodes = splice @nodes, 0, 2;
        
        my $result = $self->agent->put(\@first_nodes, $content_ref);

        if ( $result ) {
            infof "%s/%s was uploaded to gid:%s first_nodes:%s", 
                $bucket_name, $filename, $f_node->{gid}, \@first_nodes;
            $gid = $f_node->{gid};
            $rid = $f_node->{rid};
            @copied = @first_nodes;
            @replicate_to = @nodes;
            last;
        }

        infof "Failed upload %s/%s to group_id:%s first_nodes:%s",
            $bucket_name, $filename, $f_node->{gid}, \@first_nodes;
        $self->enqueue('delete_files', \@first_nodes );

    }

    http_croak(500,"Upload failed %s/%s", $bucket_name, $filename) if !$gid;

    my $sc = start_scope_container();
    if ( @exists_nodes ) {
        debugf "update objects %s", $exists_nodes[0]; 
        my $result = $self->master->update_object(
            object_id => $object_id,
            rid => $rid,
            gid => $gid,
            prev_rid => $exists_nodes[0]->{rid}
        );
        http_croak(409, "duplicated upload found %s/%s", $bucket_name, $filename) unless $result;
    }
    else {
        $object_id = try {
            $self->master->insert_object(
                gid => $gid,
                rid => $rid,
                bucket_id => $bucket->{id},
                filename => $filename,
            );
        }
        catch {
            http_croak(409, "duplicated upload found %s/%s", $bucket_name, $filename);
        };
        http_croak(500, "Cannot retrieve object_id %s/%s", $bucket_name, $filename) unless $object_id;
    }

    if ( @replicate_to ) {
        $self->enqueue('replicate_object',{
            rid => $rid,
            gid => $gid,
            object_id => $object_id,
            bucket_id => $bucket->{id},
            filename => $filename,
            copied => \@copied,
            replicate_to => \@replicate_to,
        });
    }

    if ( @exists_nodes ) {
        # remove file
        my @r_uri = map { $_->{uri} } @exists_nodes;
        if ( @r_uri ) {
            debugf "enqueue:delete_file exists_nodes args:%s",\@r_uri;
            $self->enqueue('delete_files', \@r_uri);
        }
    }

    return $self->res_ok;
}

sub jobq_replicate_object {
    my $self = shift;
    my $job = shift;
    my $args = $job->args;

    my $res = $self->agent->get($args->{copied});
    die sprintf("cannot get %s,  status_line:",
                $args->{copied}, $res->status_line) if !$res->is_success;
    my $body = $res->content;

    debugf 'replicate %s to %s', $args->{copied}, $args->{replicate_to};
    my $result = $self->agent->put($args->{replicate_to}, \$body);
    if ( $result ) {
        infof "success replicate object gid:%s %s to %s",
            $args->{gid}, $args->{copied}, $args->{replicate_to};
        $job->done(1);
        return;
    }

    infof "failed replicate object gid:%s %s to %s .. retry another fresh_nodes",
            $args->{gid}, $args->{copied}, $args->{replicate_to};

     my @f_nodes = $self->master->retrieve_fresh_nodes(
        bucket_id => $args->{bucket_id}, 
        filename => $args->{filename},
        having => $self->config->replica
    );
    die "cannot find fresh_nodes" if !@f_nodes;

    my $gid;
    my $rid;
    for my $f_node ( @f_nodes ) {
        next if $args->{gid} == $f_node->{gid}; # skip
        my $result = $self->agent->put( $f_node->{uri} );
        if ( $result ) {
            infof "%s was reuploaded to gid:%s %s", 
                $args->{copied}, $f_node->{gid}, $f_node->{uri};
            $gid = $f_node->{gid};
            $rid = $f_node->{rid};
            last;
        }
        infof "Failed replicate %s to gid:%s %s",
            $args->{copied}, $f_node->{gid}, $f_node->{uri};
        $self->enqueue('delete_files', $f_node->{uri} );
    }

    die sprintf("failed replicate %s", $args->{copied}) if !$gid;
    
    infof "success reupload %s to gid:%s", 
        $args->{copied}, $gid;

    my $sc = start_scope_container();
    $result = $self->master->update_object( 
        gid => $gid,
        rid => $rid,
        object_id => $args->{object_id},
        prev_rid => $args->{rid},
    );
    warnf "update failed maybe other worker updated: new_gid:%s, new_rid:%s %s", 
        $gid, $rid, $args unless $result;
    $self->enqueue('delete_files', $args->{copied} );

    $job->done(1);
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

    my @nodes = $master->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    http_croak(404) unless @nodes;

    $master->delete_object(
        object_id => $nodes[0]->{object_id}
    );

    # remove file
    my @r_uri = map { $_->{uri} } @nodes;
    if ( @r_uri ) {
        debugf "enqueue:delete_file args:%s",\@r_uri;
        $self->enqueue('delete_files', \@r_uri);
    }

    return $self->res_ok;
}

sub jobq_delete_files {
    my $self = shift;
    my $job = shift;
    $self->agent->delete($job->args);
    $job->done(1);
}

sub delete_object_multi {
    my $self = shift;
    my $bucket_name = shift;
    my $filenames = shift;
    my @filenames = ref $filenames ? @$filenames : ($filenames);

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(404) if $bucket->{deleted}; # 404;

    while ( my @spliced = splice( @filenames, 0, 300 ) ) {
        
        my $grouped_uri = $master->retrieve_object_nodes_multi(
            bucket_id => $bucket->{id},
            filename => \@spliced
        );
        
        for my $filename ( @spliced ) {
            warnf "not found, bucket_id:%d, filename:%d",
                $bucket->{id}, $filename,
                if ! grep { $_->{filename} eq $filename } map { @{$_} } values %$grouped_uri;
        }

        my @delete_ids = map { $_->[0]->{object_id} } values %$grouped_uri;
        $master->delete_object_multi(
            object_id => \@delete_ids,
        );

        my @r_uri = map { $_->{uri} } map { @{$_} } values %$grouped_uri;
        debugf "enqueue:delete_file args:%s",\@r_uri;
        $self->enqueue('delete_files', \@r_uri);
    }

    return $self->res_ok;
}


sub delete_bucket {
    my $self = shift;
    my ($bucket_name) = @_;

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    http_croak(404) unless $bucket; # 404
    http_croak(403) if ! $bucket->{enabled}; # 403
    http_croak(404) if $bucket->{deleted}; # 404;

    $master->delete_bucket(
        bucket_id => $bucket->{id},
        deleted => 1
    );

    $self->enqueue('delete_bucket', $bucket->{id});

    return $self->res_ok;
}

sub jobq_delete_bucket {
    my $self = shift;
    my $job = shift;
    my $bucket_id = $job->args;
    my $master = $self->master;
    while ( 1 ) {
        my $rows = $master->select_bucket_objects( bucket_id => $bucket_id );
        last if ! @$rows;
        my @delete_uris;
        for my $object ( @$rows ) {
            debugf("delete object %s", $object);
            my @nodes = $master->retrieve_object_nodes(
                bucket_id => $bucket_id,
                filename => $object->{filename},
            );
            $master->delete_object(
                object_id => $nodes[0]->{object_id},
            );
            push @delete_uris, map { $_->{uri} } @nodes;
        }
        if ( @delete_uris ) {
            $self->enqueue('delete_files', \@delete_uris);
        }
    };
    infof("delete bucket completly id:%s", $bucket_id);
    $master->delete_bucket_all( bucket_id => $bucket_id );
    $job->done(1);
}

sub dequeue {
    my $self = shift;
    my $queue = $self->master->retrieve_queue;
    return unless $queue;

    my $args = Data::MessagePack->unpack($queue->{args});
    my $func = $queue->{func};
    my $try = $queue->{try};
    $try++;

    my $job;

    try {
        debugf "execute func:%s try:%d args:%s", $func, $try, $args;
        my $subref = $self->can("jobq_". $func);

        if ( !$subref ) {
            croak "func:$func is not found";
        }
        $job = GreenBuckets::Model::Job->new(
            args => $args,
        );
        $subref->($self, $job);
        debugf "success job func:%s, try:%d, args:%s", $func, $try, $args;
    } catch {
        warnf "func:%s try:%d failed:%s ... retrying", $func, $try, $_;
    } finally {
        if ( !$job || !$job->done ) {
            sleep 1 unless $ENV{JOBQ_STOP}; #XXX
            if ( $try < $MAX_RETRY_JOB ) {
                $self->master->insert_queue(
                    func => $queue->{func}, 
                    args => $queue->{args},
                    try  => $try,
                );
            }
            else {
                critf "retry time exceeded force ended queue, func:%s, try:%d, args:%s", $func, $try, $args;
            }
        }
    };

    1;
}

sub enqueue {
    my $self = shift;
    my ($func, $args ) = @_;
    $args = Data::MessagePack->pack($args);
    $self->master->insert_queue(
        func => $func, 
        args => $args
    );
}

1;

package GreenBuckets::Model::Job;

use strict;
use warnings;
use GreenBuckets;
use Mouse;

has 'args' => (
    is => 'ro',
    isa => 'Any',
    required => 1,
);

has 'done' => (
    is => 'rw',
    isa => 'Flag',
    default => 0,
);

__PACKAGE__->meta->make_immutable();

1;

package GreenBuckets::Model::PutLock;

use strict;
use warnings;
use Mouse;

has 'model' => (
    is => 'ro',
    isa => 'GreenBuckets::Model',
    required => 1,
);

has 'lock' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

sub DEMOLISH {
    my $self = shift;
    eval {
        $self->model->master->release_putlock(
            lock => $self->lock
        );
    };
}

__PACKAGE__->meta->make_immutable();
1;

