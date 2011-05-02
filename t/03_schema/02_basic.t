use strict;
use warnings;
use Test::More;
use t::TestMysql;
use DBIx::Sunny;
use GreenBuckets::Schema;
use GreenBuckets::Util qw/filename_id/;

if ( !$ENV{TEST_MYSQLD} ) {
    plan skip_all => 'TEST_MYSQLD is false';
}

my $mysqld = t::TestMysql->setup or plan skip_all => $t::TestMysql::errstr; 

my $dbh = DBIx::Sunny->connect($mysqld->dsn( dbname => "test" ));
my $schema= GreenBuckets::Schema->new( dbh => $dbh );
ok( $schema );

is_deeply $schema->select_bucket( name => 'foo'), { id => 1, name => 'foo', enabled => 1, deleted => 0 }; 

my $nodes = $schema->select_object_nodes(
    fid => filename_id(1),
    bucket_id => 1
);
is_deeply $nodes, [
    { rid => 250, gid => 1, id => 1, node => 'http://192.168.0.1/', can_read => 1, is_fresh => 1 },
    { rid => 250, gid => 1, id => 2, node => 'http://192.168.0.2/', can_read => 1, is_fresh => 1 },
    { rid => 250, gid => 1, id => 3, node => 'http://192.168.0.3/', can_read => 1, is_fresh => 1 },
];

my $nodes2 = $schema->select_object_nodes(
    fid => filename_id(5),
    bucket_id => 2
);
is_deeply $nodes2, [
    { rid => 254, gid => 3, id => 7, node => 'http://192.168.0.7/', can_read => 1, is_fresh => 0 },
    { rid => 254, gid => 3, id => 8, node => 'http://192.168.0.8/', can_read => 1, is_fresh => 1 },
    { rid => 254, gid => 3, id => 9, node => 'http://192.168.0.9/', can_read => 1, is_fresh => 1 },
];

is_deeply $schema->select_object( fid => filename_id(5), bucket_id => 2 ),
    { fid => filename_id(5), bucket_id =>2, rid => 254, gid => 3 };

is_deeply $schema->select_fresh_nodes(having=>3), [
    { id => 1, gid => 1, node => 'http://192.168.0.1/', can_read => 1, is_fresh => 1 },
    { id => 2, gid => 1, node => 'http://192.168.0.2/', can_read => 1, is_fresh => 1 },
    { id => 3, gid => 1, node => 'http://192.168.0.3/', can_read => 1, is_fresh => 1 },
    { id => 4, gid => 2, node => 'http://192.168.0.4/', can_read => 1, is_fresh => 1 },
    { id => 5, gid => 2, node => 'http://192.168.0.5/', can_read => 1, is_fresh => 1 },
    { id => 6, gid => 2, node => 'http://192.168.0.6/', can_read => 1, is_fresh => 1 },
];

is_deeply $schema->retrieve_object( bucket_id => 1, filename => 3 ),
    { fid => filename_id(3), bucket_id =>1, rid => 252, gid => 2 };
is_deeply $schema->retrieve_object( bucket_id => 2, filename => 5 ),
    { fid => filename_id(5), bucket_id =>2, rid => 254, gid => 3 };

my @nodes = $schema->retrieve_object_nodes( bucket_id => 1, filename => 3 );
is( scalar @nodes, 3);
is( $nodes[0]->{gid}, 2);
ok( $nodes[0]->{uri} );

my $nodes_multi = $schema->retrieve_object_nodes_multi( bucket_id => 1, filename => [1,2] );
ok( $nodes_multi->{filename_id(1)} );
ok( $nodes_multi->{filename_id(2)} );
is( scalar @{$nodes_multi->{filename_id(1)}}, 3); 
is( scalar @{$nodes_multi->{filename_id(2)}}, 3);
is( $nodes_multi->{filename_id(1)}->[0]->{gid}, 1 ); 
ok( $nodes_multi->{filename_id(1)}->[0]->{uri} );

my @f_nodes = $schema->retrieve_fresh_nodes( having => 3, bucket_id => 1, filename => 3 );
is( scalar @f_nodes, 2 );
ok( $nodes[0]->{gid});
ok( $nodes[0]->{rid});
ok( $nodes[0]->{uri});

is_deeply $schema->retrieve_or_insert_bucket( name => 'test'),
    { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

is_deeply $schema->retrieve_or_insert_bucket( name => 'test2'),
    { id => 5, name => 'test2', enabled => 1, deleted => 0 }; 

my $bucket1 = $schema->select_bucket( name => 'test');
is_deeply $bucket1, { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

ok $schema->insert_object(
    rid => 250,
    gid => 1,
    bucket_id => 4,
    filename => 1
);

ok $schema->insert_object(
    rid => 250,
    gid => 1,
    bucket_id => 4,
    filename => 2
);

ok $schema->insert_object(
    rid => 250,
    gid => 1,
    bucket_id => 4,
    filename => 3,
);

is_deeply $schema->retrieve_object( bucket_id => $bucket1->{id}, filename => 1 ),
    { fid => filename_id(1), bucket_id => 4, rid => 250, gid => 1 };
is_deeply $schema->retrieve_object( bucket_id => $bucket1->{id}, filename => 2 ),
    { fid => filename_id(2), bucket_id => 4, rid => 250, gid => 1 };



ok $schema->stop_bucket( bucket_id => 4 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 0, deleted => 0 }; 
ok $schema->stop_bucket( bucket_id => 4, enabled => 1 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

ok $schema->delete_bucket( bucket_id => 4 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 1 }; 
ok $schema->delete_bucket( bucket_id => 4, deleted => 0 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

$schema->stop_bucket( bucket_id => 4 );
eval {
    $schema->insert_object(
        rid => 250,
        gid => 1,
        bucket_name => 'test',
        filename => 3
    );
};
ok $@;
$schema->stop_bucket( bucket_id => 4, enabled => 1 );

$schema->delete_bucket( bucket_id => 4 );
eval {
    $schema->insert_object(
        rid => 250,
        gid => 1,
        bucket_name => 'test',
        filename => 3
    );
};
ok $@;
$schema->delete_bucket( bucket_id => 4, deleted => 0 );

ok $schema->delete_object( bucket_id => 4, filename => 2 );
ok ! $schema->retrieve_object( bucket_id => 4, filename => 2 );

ok $schema->delete_object_multi( bucket_id => 4, filename => [1,3] );
ok ! $schema->retrieve_object( bucket_id => 4, filename => 1 );
ok ! $schema->retrieve_object( bucket_id => 4, filename => 3 );

subtest 'queue' => sub {
    ok ! $schema->retrieve_queue;
    ok $schema->insert_queue( func => 'test', args => 'bar' );
    ok $schema->insert_queue( func => 'foo', args => 'baz' );
    is_deeply $schema->retrieve_queue, { id => 1, func => 'test', args => 'bar', try => 0 };
    is_deeply $schema->retrieve_queue, { id => 2, func => 'foo', args => 'baz', try => 0 };
    ok ! $schema->retrieve_queue;
};

done_testing;

