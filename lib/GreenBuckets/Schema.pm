package GreenBucktes::Schema;

use strict;
use warnigns;
use utf8;
use parent qw/DBIx::Sunny::Schema/;
use HTTP::Exception;
use Data::Validator;

__PACKAGE__->select_row(
    'select_bucket',
    name => 'Str',
    q{SELECT * FROM buckets WHERE name = ?}
);

__PACKAGE__->select_all(
    'select_object_nodes',
    fid => 'Natural',
    bucket_id => 'Natural',
    q{SELECT nodes.* FROM nodes, objects WHERE objects.fid = ? AND objects.bucket_id = ? AND nodes.gid = objects.gid;}
);

__PACKAGE__->select_row(
    'select_object',
    fid => 'Natural',
    bucket_id => 'Natural',
    q{SELECT * FROM objects WHERE fid = ? bucket_id = ?}
);

__PACKAGE__->select_all(
    'select_fresh_node',
    having => 'Natural',
    q{SELECT * FROM nodes WHERE gid IN (SELECT gid FROM nodes WHERE can_read=1 AND is_fresh=1 GROUP BY gid HAVING COUNT(gid) = ?}
);

sub retrieve_object {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    my $fid = filename_id($filename);
    $self->select_object(
        bucket_id => $args->{bucket_id},
        fid => $filename,
    );
}

sub retrieve_object_nodes {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    my $fid = filename_id($args->{filename});
    my @nodes = $self->select_object_nodes(
        bucket_id => $args->{bucket_id},
        fid => $fid
    );

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

sub retrieve_fresh_nodes {
    state $rule = Data::Validator->new(
        'bucket_name'  => 'Str',
        'filename' => 'Str',
        'having' => 'Int',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    my @nodes = $self->select_fresh_nodes( having => $args->{having} );

    my %group;
    my $internal_path = internal_path($args->{bucket_name}, $args->{filename});
    for my $node ( @nodes ) {
        $group{$node->{gid}} ||= [];
        my $node_name = $node->{node};
        $node_name =~ s!/$!!;
        push @{$group{$node->{gid}}}, $node_name . '/' . $internal_path;
    }
    for my $gid ( keys %group ) {
        my @sort = sort {
            filename_id($a) <=> filename_id($b)
        } @{$group{$gid}};
        $group{$gid} = \@sort;
    }

    map { { gid => $_, uri => $nodes{$_} } } shuffle keys %nodes;
}

sub insert_object {
    state $rule = Data::Validator->new(
        'gid'  => 'Natural',
        'bucket_name'  => 'Str',
        'filename' => 'Str',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    {
        my $txn = $self->txn_scope;

        my $bucket = $self->select_bucket(
            name => $args->{bucket_name},
        );
        die "bucket:$bucket_name is disabled" if !$bucket->{enabled};
        die "bucket:$bucket_name is deleted" if $bucket->{deleted};

        my $bucket_id;
        if (!$bucket) {
            $self->query(
                q{INSERT INTO buckets (name, enabled, deleted) VALUES (?,?,?)},
                $args->{bucket_name},
                1,
                1
            );
            $bucket_id = $self->last_insert_id('bucket');
        }
        else {
            $bucket_id = $bucket->{id};
        }
        
        $self->query(
            q{INSERT INTO objects (fid, bucket_id, gid) VALUES (?,?,?)},
            filename_id($args->{filename}),
            $bucket_id,
            $gid,
        );
        $txn->commit;
    };

    1;
}

sub delete_object {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
        'filename' => 'Str',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    $self->query(
        q{DELETE FROM objects WHERE fid = ? AND bucket_id = ? LIMIT 1},
        filename_id($args->{filename}),
        $bucket_id,
    );
}

sub stop_bucket {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    $self->query("UPDATE buckets SET enabled = 0 WHERE id = ?", $args->{bucket_id});

    1;
}

sub delete_bucket {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    $self->query("UPDATE buckets SET deleted = 1 WHERE id = ?", $args->{bucket_id});

    1;
}

sub delete_bucket_all {
    state $rule = Data::Validator->new(
        'bucket_id'  => 'Natural',
    )->with('Method');
    my($self, $args) = $rule->validate(@_);

    my $ret;
    do {
        $ret = $self->query("DELETE FROM objects WHERE bucket_id = ? LIMIT 1000", $args->{bucket_id});
    } while ($ret);

    $self->query("DELETE FROM buckets WHERE id = ?", $args->{bucket_id});

    1;
}

1;

