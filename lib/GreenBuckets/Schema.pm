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

__PACKAGE__->select_row(
    'select_bucket',
    name => 'Str',
    q{SELECT * FROM buckets WHERE name = ?}
);

__PACKAGE__->select_all(
    'select_object_nodes',
    fid => 'Natural',
    bucket_id => 'Natural',
    q{SELECT objects.rid, nodes.* FROM nodes, objects WHERE objects.fid = ? AND objects.bucket_id = ? AND nodes.gid = objects.gid;}
);

__PACKAGE__->select_row(
    'select_object',
    fid => 'Natural',
    bucket_id => 'Natural',
    q{SELECT * FROM objects WHERE fid = ? AND bucket_id = ?}
);

__PACKAGE__->select_all(
    'select_fresh_nodes',
    having => 'Natural',
    q{SELECT * FROM nodes WHERE gid IN (SELECT gid FROM nodes WHERE can_read=1 AND is_fresh=1 GROUP BY gid HAVING COUNT(gid) = ?)}
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
    'try' => {
        isa => 'Natural',
        default => 0,
    },
    q{INSERT INTO jobqueue (func, args, try) VALUES (?,?,?) },
);

sub retrieve_object {
    my $self = shift;
    my $args= $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    );

    my $fid = filename_id($args->{filename});
    $self->select_object(
        bucket_id => $args->{bucket_id},
        fid => $fid,
    );
}

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
    if ( ! @$nodes ) {
        debugf "not found bucket_id:%s, filename:%s, fid:%s",
            $args->{bucket_id}, $args->{filename}, $fid;
        return;
    }

    my $rid = $nodes->[0]->{rid};
    my $object_path = object_path(
        bucket_id => $args->{bucket_id}, 
        filename => $args->{filename}, 
        rid => $rid
    );
    my @uris =  sort {
        filename_id($a->{uri}) <=> filename_id($b->{uri})
    } map {
        my $node = $_->{node};
        $node =~ s!/$!!;
        { uri => $node . '/' . $object_path, %{$_} }
    } @$nodes;
    @uris;
}

sub retrieve_fresh_nodes {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
        'having' => 'Int',
    );

    my $nodes = $self->select_fresh_nodes( having => $args->{having} );

    my %group;
    my $rid = gen_rid();
    my $object_path = object_path(
        bucket_id => $args->{bucket_id}, 
        filename  => $args->{filename},
        rid => $rid
    );
    for my $node ( @$nodes ) {
        $group{$node->{gid}} ||= [];
        my $node_name = $node->{node};
        $node_name =~ s!/$!!;
        push @{$group{$node->{gid}}}, $node_name . '/' . $object_path;
    }
    for my $gid ( keys %group ) {
        my @sort = sort {
            filename_id($a) <=> filename_id($b)
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
    
    $self->query(
        q{INSERT INTO objects (fid, bucket_id, rid, gid) VALUES (?,?,?,?)},
        filename_id($args->{filename}),
        $args->{bucket_id},
        $args->{rid},
        $args->{gid},
    );
    1;
}

sub update_object {
    my $self = shift;
    my $args = $self->args(
        'rid'  => 'Natural',
        'gid'  => 'Natural',
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    );
    
    $self->query(
        q{UPDATE objects SET rid =?, gid=? WHERE fid =? AND bucket_id =?},
        $args->{rid},
        $args->{gid},
        filename_id($args->{filename}),
        $args->{bucket_id},
    );

    1;
}


sub delete_object {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    );

    $self->query(
        q{DELETE FROM objects WHERE fid = ? AND bucket_id = ? LIMIT 1},
        filename_id($args->{filename}),
        $args->{bucket_id},
    );
}

sub stop_bucket {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'enabled' => { isa => 'Flag', default => 0 }
    );
    $self->query("UPDATE buckets SET enabled = ? WHERE id = ?", $args->{enabled}, $args->{bucket_id});
}


sub delete_bucket {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
        'deleted' => { isa => 'Flag', default => 1 }
    );
    $self->query("UPDATE buckets SET deleted = ? WHERE id = ?", $args->{deleted}, $args->{bucket_id});
}


sub delete_bucket_all {
    my $self = shift;
    my $args = $self->args(
        'bucket_id'  => 'Natural',
    );
    my $ret;
    do {
        $ret = $self->query("DELETE FROM objects WHERE bucket_id = ? LIMIT 1000", $args->{bucket_id});
    } while ( $ret > 0 );

    $self->query("DELETE FROM buckets WHERE id = ?", $args->{bucket_id});
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

