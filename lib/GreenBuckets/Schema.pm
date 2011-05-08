package GreenBuckets::Schema;

use strict;
use warnings;
use utf8;
use 5.10.0;
use parent qw/DBIx::Sunny::Schema/;
use List::Util qw/shuffle/;
use GreenBuckets;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;

use Log::Minimal;
local $Log::Minimal::AUTODUMP = 1;

__PACKAGE__->select_row(
    'select_bucket',
    name => 'Str',
    q{SELECT * FROM buckets WHERE name = ?}
);

__PACKAGE__->select_all(
    'select_object_nodes',
    fid => 'Natural',
    bucket_id => 'Natural',
    q{SELECT objects.id as object_id, objects.rid, objects.filename, nodes.* FROM objects left join nodes on objects.gid = nodes.gid  WHERE objects.fid = ? AND objects.bucket_id = ?}
);

__PACKAGE__->select_all(
    'select_bucket_objects',
    bucket_id => 'Natural',
    limit => { isa => 'Natural', default => 300 },
    q{SELECT * FROM objects WHERE bucket_id = ? LIMIT ?}
);

__PACKAGE__->select_all(
    'select_fresh_nodes',
    having => 'Natural',
    q{SELECT * FROM nodes WHERE gid IN (SELECT gid FROM nodes WHERE can_read=1 AND is_fresh=1 GROUP BY gid HAVING COUNT(gid) = ?)}
);

__PACKAGE__->query(
    'enable_bucket',
    'enabled' => { isa => 'Flag', default => 0 },
    'bucket_id'  => 'Natural',
    q{UPDATE buckets SET enabled = ? WHERE id = ?}
);

__PACKAGE__->query(
    'delete_bucket',
    'deleted' => { isa => 'Flag', default => 1 },
    'bucket_id'  => 'Natural',
    q{UPDATE buckets SET deleted = ? WHERE id = ?},
);

__PACKAGE__->query(
    'delete_bucket_all',
    'bucket_id'  => 'Natural',
    "DELETE FROM buckets WHERE id = ?"
);

__PACKAGE__->query(
    'update_object',
    'rid'  => 'Natural',
    'gid'  => 'Natural',
    'object_id'  => 'Natural',
    q{UPDATE objects SET rid =?, gid=? WHERE id =?},
);

__PACKAGE__->query(
    'delete_object',
    'object_id' => 'Natural',
    q{DELETE FROM objects WHERE id = ?}
);

__PACKAGE__->select_all(
    'select_queue',
    limit => { isa =>'Natural', default => 10 },
    q{SELECT * FROM jobqueue ORDER BY id LIMIT ?}
);

__PACKAGE__->query(
    'delete_queue',
    id => { isa =>'Natural' },
    q{DELETE FROM jobqueue WHERE  id =?},
);

__PACKAGE__->query(
    'insert_queue',
    func => { isa =>'Str' },
    args => { isa =>'Str' },
    try => { isa => 'Natural', default => 0 },
    q{INSERT INTO jobqueue (func, args, try) VALUES (?,?,?) },
);

sub retrieve_object_nodes {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    );

    my $fid = filename_id($args->{filename});
    my $nodes = $self->select_object_nodes(
        bucket_id => $args->{bucket_id},
        fid => $fid
    );
    my @nodes = grep { $_->{filename} eq $args->{filename} } @$nodes;
    if ( ! @nodes ) {
        return;
    }        

    my $object_path = object_path(
        filename => $args->{filename},
        bucket_id => $args->{bucket_id},
        rid => $nodes[0]->{rid}
    );
    @nodes =  sort {
        filename_id(join "/", $a->{id},$object_path) <=> filename_id(join "/", $b->{id},$object_path)
    } map {
        my $node = $_->{node};
        $node =~ s!/$!!;
        { uri => $node . '/' . $object_path, %{$_} }
    } @nodes;
    @nodes;
}

sub retrieve_object_nodes_multi {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'ArrayRef[Str]',
    );

   my %filenames = map {
       $_ => filename_id($_)
   } @{$args->{filename}};
    my $query = join ",", map { "?" } keys %filenames;
    $query = qq{SELECT objects.id as object_id, objects.fid, objects.bucket_id, objects.rid, objects.filename, nodes.* FROM objects LEFT JOIN nodes ON nodes.gid = objects.gid WHERE objects.fid IN ($query) AND objects.bucket_id = ?};
    my $rows = $self->select_all($query, values %filenames, $args->{bucket_id});

    my %objects;
    for my $row ( @$rows ) {
        next if ! exists $filenames{$row->{filename}};
        $objects{$row->{object_id}} ||= [];
        push @{$objects{$row->{object_id}}}, $row;
    }

    my %result;
    for my $object_id ( keys %objects ) {

        my $nodes = $objects{$object_id};
        my $object_path = object_path(
            filename => $nodes->[0]->{filename},
            bucket_id => $nodes->[0]->{bucket_id},
            rid => $nodes->[0]->{rid},
        );
        my @uris =  sort {
            filename_id(join "/", $a->{id},$object_path) <=> filename_id(join "/", $b->{id},$object_path)
        } map {
            my $node = $_->{node};
            $node =~ s!/$!!;
            { uri => $node . '/' . $object_path, %{$_} }
        } @$nodes;

        $result{$object_id} = \@uris;
    }
    \%result;
}

sub retrieve_fresh_nodes {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
        'having' => 'Int',
        'previous_rid' => { isa => 'Natural', optional => 1 }
    );

    my $nodes = $self->select_fresh_nodes( having => $args->{having} );

    my %group;
    my $rid = gen_rid();
    if ( exists $args->{previous_rid} ) {
        while ( $rid == $args->{previous_rid} ) {
            $rid = gen_rid();
        }
    }

    my $object_path = object_path(
        bucket_id => $args->{bucket_id},
        filename => $args->{filename},
        rid => $rid
    );
    for my $node ( @$nodes ) {
        $group{$node->{gid}} ||= [];
        my $node_name = $node->{node};
        $node_name =~ s!/$!!;
        push @{$group{$node->{gid}}}, {
            uri => $node_name . '/' . $object_path,
            %{$node}
        };
    }

    for my $gid ( keys %group ) {
        my @sort = map { $_->{uri}} sort {
            filename_id(join "/", $a->{id},$object_path) <=> filename_id(join "/", $b->{id},$object_path)
        } @{$group{$gid}};
        $group{$gid} = \@sort;
    }

    map { { rid => $rid, gid => $_, uri => $group{$_} } } shuffle keys %group;
}

sub retrieve_or_insert_bucket {
    my $self = shift;
    my $args = $self->args(
        'name'  => 'Str',
    );

    my $bucket;
    {
        my $txn = $self->txn_scope;

        $bucket = $self->select_bucket(
            name => $args->{name},
        );

        if ( !$bucket ) {
            $self->query(
                q{INSERT INTO buckets (name, enabled, deleted) VALUES (?,?,?)},
                $args->{name},
                1,
                0
            );
            my $bucket_id = $self->last_insert_id();
            $bucket = {
                id => $bucket_id,
                name => $args->{name},
                enabled => 1,
                deleted => 0,
            };
        }

        $txn->commit;
    }
    $bucket;
}

sub insert_object {
    my $self = shift;
    my $args = $self->args(
        'rid'  => 'Natural',
        'gid'  => 'Natural',
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    );

    my $object_id;
    {
        my $txn = $self->txn_scope;
        my @exists = $self->retrieve_object_nodes(
            bucket_id => $args->{bucket_id},
            filename => $args->{filename},
        );
        die sprintf "duplicated entry bucket_id:%s, filename:%s",
            $args->{bucket_id}, $args->{filename} if @exists;
        $self->query(
            q{INSERT INTO objects (fid, bucket_id, rid, gid, filename) VALUES (?,?,?,?,?)},
            filename_id($args->{filename}),
            $args->{bucket_id},
            $args->{rid},
            $args->{gid},
            $args->{filename}
        );
        $object_id = $self->last_insert_id();
        $txn->commit;
    }
    $object_id;
}


sub delete_object_multi {
    my $self = shift;
    my $args = $self->args(
        'object_id' => 'ArrayRef[Natural]'
    );

    my $query = join ",", map { "?" } @{$args->{object_id}};
    $query = qq{DELETE FROM objects WHERE id IN ($query)};
    $self->query($query, @{$args->{object_id}} );
}

sub retrieve_queue {
    my $self = shift;
    my $args = $self->args(
        'limit'  => { isa => 'Natural', default => 10 },
    );
    my $queues = $self->select_queue( limit => $args->{limit} );
    return unless @$queues;
    
    my $queue;
    for my $r_queue ( @$queues ) {
        my $result = $self->delete_queue( id => $r_queue->{id} );
        if ( $result == 1 ) {
            $queue = $r_queue;
            last;
        }
    }
    $queue;
}


1;

