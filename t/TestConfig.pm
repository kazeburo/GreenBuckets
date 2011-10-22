package t::TestConfig;

use strict;
use warnings;
use GreenBuckets::Config;
use Test::TCP;

sub setup {
    my $config = {
        dispatcher_port => empty_port(),
        jobqueue_worker_port => empty_port(),
    };
    if ( $ENV{TEST_MYSQLD} ) {
        require t::TestMysql;
        my $mysqld = t::TestMysql->setup;
        my @dsn = $mysqld->dsn( dbname => 'test' );
        $config->{master} = \@dsn;
        $config->{slave} = [\@dsn, \@dsn];
    }
    else {
        $config->{master} = ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],;
        $config->{slave} = [
            ['dbi:mysql:greenbuckets;host=127.0.0.2','user','passwd'],
            ['dbi:mysql:greenbuckets;host=127.0.0.2','user','passwd']
        ];
    }
    GreenBuckets::Config->new($config);
}

1;

