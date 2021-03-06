use strict;
use warnings;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use Test::More;


like filename_id('foo'), qr/^\d+$/;
like filename_id("\x{2600}"), qr/^\d+$/;

like gen_rid(), qr/^\d+$/;

my $object_path =  object_path(
    bucket_id => 1,
    filename => "\x{2600}",
    rid => 3,
    flat => 0
);
like $object_path, qr!^\d{2}/\d{2}/\w{56}$!;

$object_path =  object_path(
    bucket_id => 1,
    filename => "\x{2600}",
    rid => 3,
    flat => 1
);
like $object_path, qr!^\w{56}$!;

done_testing();

