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

{
    ok( $model->get_bucket('baz2') );
    ok( $model->rename_bucket('baz2','baz') );
    ok( $model->get_bucket('baz') );

    eval { $model->rename_bucket('baz','foo') };
    my $e = HTTP::Exception->caught;
    is( $e->code, 409);
}

{
    ok( $model->delete_bucket('baz') );
    eval { $model->get_bucket('baz') };
    my $e = HTTP::Exception->caught;
    is( $e->code, 503);
    $model->dequeue;
    eval { $model->get_bucket('baz') };
    $e = HTTP::Exception->caught;
    is( $e->code, 404);
}

done_testing;

