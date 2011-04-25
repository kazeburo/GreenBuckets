package GreenBuckets::Dispatcher::Connection;

use strict;
use warnings;
use Class::Accessor::Lite (
    new => 1,
    rw => [qw/req res stash args/]
);

*request = \&req;
*response = \&res;

1;

