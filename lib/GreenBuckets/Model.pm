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
    
    my @r_uri = map { $_->{uri} } grep { $_->{can_read} } @uri;
    http_croak(500, "all storage cannot read %s", \@uri) if ! @r_uri;

    my $res = $self->agent->get(\@r_uri);
    http_croak(500, "all storage cannot get %s, last status_line: %s", \@uri, $res->status_line)
        if !$res->is_success; 

    my $r_res = GreenBuckets::Dispatcher::Response->new(200);
    for my $header ( qw/server content_type last_modified/ ) {
        $r_res->header($header, $res->header($header));
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
    my $bucket = $master->retrieve_or_insert_bucket(
        name => $bucket_name
    );
    http_croak(403) if ! $bucket->{enabled};
    http_croak(503) if $bucket->{deleted}; #XXX 

    if ( $master->retrieve_object( bucket_id => $bucket->{id}, filename => $filename ) ) {
        http_croak(409, "duplicated upload %s/%s", $bucket_name, $filename);
    }

    my @f_nodes = $master->retrieve_fresh_nodes(
        bucket_id => $bucket->{id}, 
        filename => $filename,
        having => $self->config->replica
    );
    undef $master;
    undef $sc;
    http_croak(500, "cannot find fresh_nodes") if !@f_nodes;

    my $gid;
    my $rid;
    for my $f_node ( @f_nodes ) {
        my $first_node = $f_node->{uri}->[0];
        my $result = $self->agent->put($first_node, $content_ref);

        if ( $result ) {
            infof "%s/%s was uploaded to gid:%s first_node:%s", 
                $bucket_name, $filename, $f_node->{gid}, $first_node;
            $gid = $f_node->{gid};
            $rid = $f_node->{rid};
            last;
        }

        infof "Failed upload %s/%s to group_id:%s first_node:%s",
            $bucket_name, $filename, $f_node->{gid}, $first_node;
        $self->enqueue('delete_files', $first_node );

    }

    http_croak(500,"Upload failed %s/%s", $bucket_name, $filename) if !$gid;

    $sc = start_scope_container();
    $self->master->insert_object( 
        gid => $gid,
        rid => $rid,
        bucket_id => $bucket->{id},
        filename => $filename
    );

    $self->enqueue('replicate_object',{
        gid => $gid,
        bucket_id => $bucket->{id},
        filename => $filename
    });

    return $self->res_ok;
}

sub jobq_replicate_object {
    my $self = shift;
    my $job = shift;
    my $args = $job->args;

    my @uri = $self->master->retrieve_object_nodes(
        bucket_id => $args->{bucket_id},
        filename => $args->{filename},
    );
    my @r_uri = map { $_->{uri} } @uri;
    my $first_node = shift @r_uri;

    my $res = $self->agent->get($first_node);
    die "cannot get $first_node , status_line:".$res->status_line if !$res->is_success;
    my $body = $res->content;

    debugf 'replicate %s to %s', $first_node, \@r_uri;
    my $result = $self->agent->put(\@r_uri, \$body);
    if ( $result ) {
        infof "success replicate object gid:%s %s to %s",
            $args->{gid}, $first_node, \@r_uri;
        $job->done(1);
        return;
    }

    infof "failed replicate object gid:%s %s to %s .. retry another fresh_nodes",
            $args->{gid}, $first_node, \@r_uri;
 
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
                $first_node, $f_node->{gid}, $f_node->{uri};
            $gid = $f_node->{gid};
            $rid = $f_node->{rid};
            last;
        }
        infof "Failed replicate %s to gid:%s %s",
            $first_node, $f_node->{gid}, $f_node->{uri};
        $self->enqueue('delete_files', $f_node->{uri} );
    }

    die sprintf("replicate failed %s", $first_node) if !$gid;
    
    infof "success reupload %s to gid:%s", 
        $first_node, $gid;

    my $sc = start_scope_container();
    $self->master->update_object( 
        gid => $gid,
        rid => $rid,
        bucket_id => $args->{bucket_id},
        filename => $args->{filename}
    );
    $self->enqueue('delete_files', $first_node );

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

sub dequeue {
    my $self = shift;
    my $sc = start_scope_container;
    my $queue = $self->master->retrieve_queue;
    undef $sc;
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

