use strict;
use warnings;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use Test::More;


like filename_id('foo'), qr/^\d+$/;
like filename_id("\x{2600}"), qr/^\d+$/;

like gen_rid(), qr/^\d+$/;

my $object_path =  object_path(
    bucket_id => 1,
    filename => 'bar.jpg',
    rid => 3
);
like $object_path, qr!^\w/\w/\w/\w{56}\.jpg$!;

$object_path =  object_path(
    bucket_id => 1,
    filename => 'bar',
    rid => 3
);
like $object_path, qr!^\w/\w/\w/\w{56}$!;

$object_path =  object_path(
    bucket_id => 1,
    filename => "\x{2600}.jpg",
    rid => 3
);
like $object_path, qr!^\w/\w/\w/\w{56}.jpg$!;

$object_path =  object_path(
    bucket_id => 1,
    filename => "\x{2600}.\x{2600}",
    rid => 3
);
like $object_path, qr!^\w/\w/\w/\w{56}$!;

done_testing();

