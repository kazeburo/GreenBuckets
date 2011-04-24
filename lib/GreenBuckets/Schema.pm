package GreenBuckets::Schema;

use strict;
use warnings;
use utf8;
use 5.10.0;
use parent qw/DBIx::Sunny::Schema/;
use HTTP::Exception;
use List::Util qw/shuffle/;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use Mouse::Util::TypeConstraints;

subtype 'Natural'
    => as 'Int'
    => where { $_ > 0 };

subtype 'Flag'
    => as 'Int'
    => where { $_ == 0 || $_ == 1 };


no Mouse::Util::TypeConstraints;

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
    q{SELECT id FROM jobqueue ORDER BY id LIMIT ?}
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
    q{INSERT INTO  jobqueue (func, args) VALUES (?,?) },
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
        'bucket_name' => 'Str',
        'filename' => 'Str',
    );

    my $fid = filename_id($args->{filename});
    my $nodes = $self->select_object_nodes(
        bucket_id => $args->{bucket_id},
        fid => $fid
    );

    return unless @$nodes;

    my $rid = $nodes->[0]->{rid};
    my $object_path = object_path($args->{bucket_name}, $args->{filename}, $rid);
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
        'bucket_name'  => 'Str',
        'filename' => 'Str',
        'having' => 'Int',
    );

    my $nodes = $self->select_fresh_nodes( having => $args->{having} );

    my %group;
    my $rid = gen_rid();
    my $object_path = object_path($args->{bucket_name}, $args->{filename}, $rid);
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

sub insert_object {
    my $self = shift;
    my $args = $self->args(
        'rid'  => 'Natural',
        'gid'  => 'Natural',
        'bucket_name'  => 'Str',
        'filename' => 'Str',
    );

    {
        my $txn = $self->txn_scope;

        my $bucket = $self->select_bucket(
            name => $args->{bucket_name},
        );
        if ( $bucket ) {
            die "bucket:". $args->{bucket_name} ." is disabled" if !$bucket->{enabled};
            die "bucket:". $args->{bucket_name} ." is deleted" if $bucket->{deleted};
        }

        my $bucket_id;
        if (!$bucket) {
            $self->query(
                q{INSERT INTO buckets (name, enabled, deleted) VALUES (?,?,?)},
                $args->{bucket_name},
                1,
                0
            );
            $bucket_id = $self->last_insert_id();
        }
        else {
            $bucket_id = $bucket->{id};
        }
        
        $self->query(
            q{INSERT INTO objects (fid, bucket_id, rid, gid) VALUES (?,?,?,?)},
            filename_id($args->{filename}),
            $bucket_id,
            $args->{rid},
            $args->{gid},
        );
        $txn->commit;
    };

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

sub create_queue {
    my $self = shift;
    my $args = $self->args(
        'func' => 'Str',
        'args'  => { isa => 'Str' },
    );
    $self->insert_queue($args);
}

1;

