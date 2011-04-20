use strict;
use warnings;
use GreenBuckets::Util qw/filename_id gen_rid object_path/;
use Test::More;


like filename_id('foo'), qr/^\d+$/;
like filename_id("\x{2600}"), qr/^\d+$/;

like gen_rid(), qr/^\d+$/;

my $object_path =  object_path('foo','bar.jpg','baz');
like $object_path, qr!^\w/\w/\w/\w{56}\.jpg$!;

$object_path =  object_path('foo','bar','baz');
like $object_path, qr!^\w/\w/\w/\w{56}$!;

$object_path =  object_path("\x{2600}","\x{2600}.jpg","\x{2600}");
like $object_path, qr!^\w/\w/\w/\w{56}.jpg$!;

$object_path =  object_path("\x{2600}","\x{2600}.\x{2600}","\x{2600}");
like $object_path, qr!^\w/\w/\w/\w{56}$!;


done_testing();

