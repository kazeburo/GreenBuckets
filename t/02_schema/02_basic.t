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

my $mysqld = t::TestMysql->setup() or plan skip_all => $t::TestMysql::errstr;

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


done_testing;

