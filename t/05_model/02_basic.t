use strict;
use Test::More;
use t::TestConfig;
use GreenBuckets::Model;

if ( !$ENV{TEST_MYSQLD} ) {
    plan skip_all => 'TEST_MYSQLD is false';
}

my $config = t::TestConfig->setup;
my $model = GreenBuckets::Model->new( config => $config );
ok($model);

done_testing;

