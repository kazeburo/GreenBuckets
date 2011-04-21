package t::TestMysql;

use strict;
use warnings;
use Data::Section::Simple;
use DBIx::Sunny;
use Test::mysqld;
use GreenBuckets;
use GreenBuckets::Util qw/filename_id/;

our $errstr = '';

sub setup {
    $errstr = '';
    my $mysqld = Test::mysqld->new(
        my_cnf => {
            'bind-address' => '127.0.0.1', # no TCP socket
        }
    );
    if ( !$mysqld ) {
        $errstr = $Test::mysqld::errstr;
        return;
    }

    my $dbh = DBIx::Sunny->connect($mysqld->dsn( dbname => "test" ));

    my $reader = Data::Section::Simple->new('GreenBuckets');
    my $all_tables = $reader->get_data_section;
    for (@GreenBuckets::TABLES) {
        $dbh->query($all_tables->{$_});
    }

    $dbh->query(q{INSERT INTO nodes SET gid=1, node='http://192.168.0.1/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=1, node='http://192.168.0.2/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=1, node='http://192.168.0.3/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=2, node='http://192.168.0.4/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=2, node='http://192.168.0.5/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=2, node='http://192.168.0.6/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=3, node='http://192.168.0.7/', can_read=1, is_fresh=0});
    $dbh->query(q{INSERT INTO nodes SET gid=3, node='http://192.168.0.8/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=3, node='http://192.168.0.9/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=4, node='http://192.168.0.10/', can_read=1, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=4, node='http://192.168.0.11/', can_read=0, is_fresh=1});
    $dbh->query(q{INSERT INTO nodes SET gid=4, node='http://192.168.0.12/', can_read=1, is_fresh=1});

    $dbh->query(q{INSERT INTO buckets SET name = ?}, 'foo');
    $dbh->query(q{INSERT INTO buckets SET name = ?}, 'bar');
    $dbh->query(q{INSERT INTO buckets SET name = ?}, 'baz');

    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=250 ,gid=1},filename_id(1));
    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=251 ,gid=1},filename_id(2));
    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=252 ,gid=2},filename_id(3));
    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=253 ,gid=2},filename_id(4));
    $dbh->query(q{INSERT INTO objects SET bucket_id=2, fid=?, rid=254 ,gid=3},filename_id(5));
    $dbh->query(q{INSERT INTO objects SET bucket_id=2, fid=?, rid=255 ,gid=3},filename_id(6));
    $dbh->query(q{INSERT INTO objects SET bucket_id=3, fid=?, rid=256 ,gid=2},filename_id(7));
    $dbh->query(q{INSERT INTO objects SET bucket_id=3, fid=?, rid=257 ,gid=1},filename_id(8));
    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=258 ,gid=2},filename_id(9));
    $dbh->query(q{INSERT INTO objects SET bucket_id=1, fid=?, rid=259 ,gid=1},filename_id(10));

    $mysqld;
}

1;


