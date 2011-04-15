package GreenBucktes::Model;

use strict;
use warnings;
use utf8;
use Scope::Container;
use Scope::Container::DBI;
use GreenBucktes::Util qw/filename_id internal_path/;
use GreenBucktes::Schema;
use List::Util qw/shuffle/;
use HTTP::Exception;
use Log::Minimal;
use Class::Accessor::Lite (
    new => 1,
);

sub slave {
    my $self = shift;
    my $dbh = Scope::Container::DBI->new(...);
    GreenBucktes::Schema->new(dbh=>$dbh, readonly=1);
}

sub master {
    my $self = shift;
    my $dbh = Scope::Container::DBI->new(...);
    GreenBucktes::Schema->new(dbh=>$dbh);
}

sub get_object {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $sc = start_scope_container();
    my @uri = $self->retrieve_internal_uris($bucket_name, $filename);
    undef $sc;

    my @r_uri = grep { $_->{can_read} } @uri;
    if ( !@r_uri ) {
        warnf "all storage cannot read %s", \@uri;
        HTTP::Exception->throw(500);
    }

    ... #retrieve from storage

    return ...;
}

sub put_object {
    my $self = shift;
    my ($bucket_name, $filename, $fh) = @_;

    my $sc = start_scope_container();
    if ( $self->check_for_put($bucket, $filename) ) {
        warnf "duplicated upload %s/%s", $bucket, $filename;
        HTTP::Exception->throw(409);
    }

    my @f_nodes = $self->retrieve_fresh_nodes($bucket, $filename);
    undef $sc;

    my $try=3;
    my $gid;
    for my $f_node ( @f_node ) {
        my $f_gid = $f_node->{gid};
        
        # upload ...

        if ( success ) {
            infof "%s/%s was uploaded to group_id:%s", $bucket, $filename, $f_gid;
            $gid = $f_gid;
            last;
        }

        infof "Failed upload to group_id:%s", $f_gid;
        --$try;
        if ( $try == 0 ) {
            warnf "try time exceed for upload %s/%s", $bucket, $filename;
            last;
        }
    }

    if ( !$gid ) {
        warnf "Upload faile %s/%s", $bucket, $filename;
        return HTTP::Exception->throw(500);
    }

    $self->create_object( $gid, $bucket, $filename );
    return 1;
}

sub retrieve_internal_uris {
    my $self = shift;
    my ($bucket_name, $filename) = @_;
    
    my $bucket = $self->slave->retrieve_bucket(
        name => $bucket_name
    );
    return HTTP::Exception->throw(404) unless $bucket; # 404

    return HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    return HTTP::Exception->throw(404) if $bucket->{deleted}; # 404;

    my $fid = filename_id($filename);
    my @nodes = $self->slave->retrieve_nodes(
        bucket_id => $bucket->{id},
        fid => $fid
    );

    return HTTP::Exception->throw(404) $bucket unless @nodes; # 404

    my $internal_path = internal_path($bucket_name, $filename);
    my @uris =  sort {
        filename_id($a->{uri}) <=> filename_id($b->{uri})
    } map {
        my $node = $_->{node};
        $node =~ s!/$!!;
        { uri => $node . '/' . $internal_path, %{$_} }
    } @nodes;
    @uri;
}

sub check_for_put {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $bucket = $self->master->retrieve_bucket(
        name => $bucket_name
    );
    return 1 unless $bucket; # no bucket
    
    my $fid = filename_id($filename);
    my @nodes = $self->master->retrieve_nodes(
        bucket_id => $bucket->{id},
        fid => $fid
    );
    return if @nodes; #exists
    1;
}

sub retrieve_fresh_nodes {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my @nodes = $self->master->retrieve_fresh_nodes();

    my %nodes;
    my $internal_path = internal_path($bucket_name, $filename);
    for my $node ( @nodes ) {
        $nodes{$_->{gid}} ||= [];
        my $node_name = $_->{node};
        $node_name =~ s!/$!!;
        push @{$nodes{$_->{gid}}}, $node_name . '/' . $internal_path;
    }
    map { { gid => $_, uri => $nodes{$_} } } shuffle keys %nodes;
}

sub create_object {
    my $self = shift;
    my ($gid, $bucket_name, $filename) = @_;

    my $master = $self->master;
    eval {
        my $txn = $master->txn_scope;

        my $bucket = $master->retrieve_bucket(
            name => $bucket_name
        );
        die "bucket:$bucket_name is disabled" if !$bucket->{enabled};
        die "bucket:$bucket_name is deleted" if $bucket->{deleted};
        my $bucket_id;
        if (!$bucket) {
            $master->create_bucket(
                name => $bucket_name
            );
            $bucket_id = $master->last_insert_id('bucket'); #XXX
        }
        else {
            $bucket_id = $bucket->{id};
        }
        
        $master->create_object(
            bucket_id => $bucket_id,
            fid => filename_id($filename),
            gid => $gid
        );
        $txn->commit;
    };

    if ( $@ ) {
        warnf $@;
        HTTP::Exception->throw(500);
    }

    1;
}

1;

