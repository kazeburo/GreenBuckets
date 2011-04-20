use strict;
use warnings;
use Test::More;
use Data::Section::Simple;
use DBIx::Sunny;
use GreenBuckets;
use GreenBuckets::Schema;

if ( !$ENV{TEST_MYSQLD} ) {
    plan skip_all => 'TEST_MYSQLD is false';
}

use Test::mysqld;

my $mysqld = Test::mysqld->new(
    my_cnf => {
        'bind-address' => '127.0.0.1', # no TCP socket
    }
) or plan skip_all => $Test::mysqld::errstr;

my $dbh = DBIx::Sunny->connect($mysqld->dsn( dbname => "test" ));
my $schema= GreenBuckets::Schema->new( dbh => $dbh );
ok( $schema );

my $reader = Data::Section::Simple->new('GreenBuckets');
my $all_tables = $reader->get_data_section;
for (@GreenBuckets::TABLES) {
    ok( $schema->query($all_tables->{$_}) );
}


done_testing;

