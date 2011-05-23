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
    { object_id =>1, filename => 1, rid => 250, gid => 1, 
      id => 1, node => 'http://192.168.0.1/', online => 1, fresh => 1, remote => 0 },
    { object_id =>1, filename => 1, rid => 250, gid => 1,
      id => 2, node => 'http://192.168.0.2/', online => 1, fresh => 1, remote => 0 },
    { object_id =>1,  filename => 1, rid => 250, gid => 1,
      id => 3, node => 'http://192.168.0.3/', online => 1, fresh => 1, remote => 0 },
];

$schema->dbh->query('UPDATE nodes set remote = 1 WHERE id= 2');
is_deeply $schema->select_object_nodes(
    fid => filename_id(1),
    bucket_id => 1
), [
    { object_id =>1, filename => 1, rid => 250, gid => 1, 
      id => 1, node => 'http://192.168.0.1/', online => 1, fresh => 1, remote => 0 },
    { object_id =>1, filename => 1, rid => 250, gid => 1,
      id => 2, node => 'http://192.168.0.2/', online => 1, fresh => 1, remote => 1 },
    { object_id =>1,  filename => 1, rid => 250, gid => 1,
      id => 3, node => 'http://192.168.0.3/', online => 1, fresh => 1, remote => 0 },
];

my $nodes2 = $schema->select_object_nodes(
    fid => filename_id(5),
    bucket_id => 2
);
is_deeply $nodes2, [
    { object_id =>5, filename => 5, rid => 254, gid => 3, 
      id => 7, node => 'http://192.168.0.7/', online => 1, fresh => 0, remote =>0 },
    { object_id =>5, filename => 5, rid => 254, gid => 3, 
      id => 8, node => 'http://192.168.0.8/', online => 1, fresh => 1, remote =>0 },
    { object_id =>5, filename => 5, rid => 254, gid => 3, 
      id => 9, node => 'http://192.168.0.9/', online => 1, fresh => 1, remote =>0 },
#conflict objects
    { object_id =>11, filename => '5a', rid => 260, gid => 3, 
      id => 7, node => 'http://192.168.0.7/', online => 1, fresh => 0, remote =>0 },
    { object_id =>11, filename => '5a', rid => 260, gid => 3, 
      id => 8, node => 'http://192.168.0.8/', online => 1, fresh => 1, remote =>0 },
    { object_id =>11, filename => '5a', rid => 260, gid => 3, 
      id => 9, node => 'http://192.168.0.9/', online => 1, fresh => 1,remote=>0 },
];


my @nodes = $schema->retrieve_object_nodes( bucket_id => 1, filename => 3 );
is( scalar @nodes, 3);
is( $nodes[0]->{object_id}, 3);
is( $nodes[0]->{gid}, 2);
ok( $nodes[0]->{uri} );

@nodes = $schema->retrieve_object_nodes( bucket_id => 2, filename => 5 );
is( scalar @nodes, 3);
is( $nodes[0]->{object_id}, 5);
is( $nodes[0]->{gid}, 3);
ok( $nodes[0]->{uri} );


@nodes = $schema->retrieve_object_nodes( bucket_id => 1, filename => 9 );
is( scalar @nodes, 3);
is( $nodes[0]->{object_id}, 9);
is( $nodes[0]->{gid}, 2);
ok( $nodes[0]->{uri} );

@nodes = $schema->retrieve_object_nodes( bucket_id => 1, filename => 1 );
is( scalar @nodes, 3);
is( $nodes[0]->{id}, 3);
is( $nodes[1]->{id}, 1);
is( $nodes[2]->{id}, 2);

my $nodes_multi = $schema->retrieve_object_nodes_multi( bucket_id => 1, filename => [1,9,100] );
ok( $nodes_multi->{1} );
ok( $nodes_multi->{9} );
ok( !$nodes_multi->{100} );
use Log::Minimal;
local $Log::Minimal::AUTODUMP = 1;
is( scalar @{$nodes_multi->{1}}, 3);
is( scalar @{$nodes_multi->{9}}, 3);
is( $nodes_multi->{1}->[0]->{object_id}, 1 ); 
is( $nodes_multi->{1}->[0]->{gid}, 1 ); 
ok( $nodes_multi->{1}->[0]->{uri} );
is( $nodes_multi->{9}->[0]->{object_id}, 9 ); 
is( $nodes_multi->{9}->[0]->{gid}, 2 ); 
ok( $nodes_multi->{9}->[0]->{uri} );


my @f_nodes = $schema->retrieve_fresh_nodes( replica => 3, bucket_id => 1, filename => 3 );
is( scalar @f_nodes, 3 );
ok( $f_nodes[0]->{gid});
ok( $f_nodes[0]->{rid});
ok( $f_nodes[0]->{nodes} );
ok( grep { $_->{gid} == 1 } @f_nodes );
ok( ! grep { $_->{gid} == 3 } @f_nodes );
ok( ! grep { $_->{gid} == 5 } @f_nodes );
for my $f_node ( @f_nodes ) {
    if ( $f_node->{gid} == 1 ) {
        is( scalar @{$f_node->{nodes}}, 3 );
        like $f_node->{nodes}->[0]->{uri}, qr!^http://192\.168\.0\.3/!;
        like $f_node->{nodes}->[1]->{uri}, qr!^http://192\.168\.0\.1/!;
        like $f_node->{nodes}->[2]->{uri}, qr!^http://192\.168\.0\.2/!;
    }
    if ( $f_node->{gid} == 4) {
        is( scalar @{$f_node->{nodes}}, 3 );
        like $f_node->{nodes}->[0]->{uri}, qr!^http://192\.168\.0\.11/!;
        like $f_node->{nodes}->[1]->{uri}, qr!^http://192\.168\.0\.10/!;
        like $f_node->{nodes}->[2]->{uri}, qr!^http://192\.168\.0\.12/!;
    }
}


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

eval {
    $schema->insert_object(
        rid => 250,
        gid => 1,
        bucket_id => 4,
        filename => 3,
    );
};
ok($@);

ok scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 1 );
ok scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 2 );
ok scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 3 );

ok $schema->enable_bucket( bucket_id => 4 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 0, deleted => 0 }; 
ok $schema->enable_bucket( bucket_id => 4, enabled => 1 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

ok $schema->delete_bucket( bucket_id => 4 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 1 }; 
ok $schema->delete_bucket( bucket_id => 4, deleted => 0 );
is_deeply $schema->select_bucket( name => 'test'), { id => 4, name => 'test', enabled => 1, deleted => 0 }; 

$schema->enable_bucket( bucket_id => 4 );
eval {
    $schema->insert_object(
        rid => 250,
        gid => 1,
        bucket_name => 'test',
        filename => 3
    );
};
ok $@;
$schema->enable_bucket( bucket_id => 4, enabled => 1 );

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

@nodes = $schema->retrieve_object_nodes( bucket_id => 4, filename => 2 );
ok $schema->delete_object( object_id => $nodes[0]->{object_id} );
ok ! scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 2 );

$nodes_multi = $schema->retrieve_object_nodes_multi( bucket_id => 4, filename => [1,3] );

ok $schema->delete_object_multi( object_id => [ map { $_->[0]->{object_id} } values %$nodes_multi] );
ok ! scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 1 );
ok ! scalar $schema->retrieve_object_nodes( bucket_id => 4, filename => 3 );

subtest 'queue' => sub {
    ok ! $schema->retrieve_queue;
    ok $schema->insert_queue( func => 'test', args => 'bar' );
    ok $schema->insert_queue( func => 'foo', args => 'baz' );
    is_deeply $schema->retrieve_queue, { id => 1, func => 'test', args => 'bar', try => 0 };
    is_deeply $schema->retrieve_queue, { id => 2, func => 'foo', args => 'baz', try => 0 };
    ok ! $schema->retrieve_queue;
};

done_testing;

