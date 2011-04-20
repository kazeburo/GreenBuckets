package GreenBucktes::Util;

use strict;
use warnings;
use utf8;
use parent qw/Exporter/;
use Encode;
use Digest::MurmurHash qw/murmur_hash/;
use Digest::SHA qw/sha224_hex/;

our @EXPORT_OK = qw/filename_id gen_rid object_path/;

sub filename_id {
    my $filename = shift;
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    murmur_hash($filename);   
}

sub gen_rid {
    int(rand(0xff));
}

sub object_path {
    my ($bucket,$filename, $rid) = @_;
    $bucket = Encode::encode_utf8($bucket) if Encode::is_utf8($bucket);
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    $rid = Encode::encode_utf8($rid) if Encode::is_utf8($rid);

    my $suffix;
    if ( $filename =~ m!\.(.+)$! ) {
        $suffix = $1;
    }
    my $hash = sha224_hex($bucket . '/' . $rid . '/' . $filename);

    my $path = sprintf("%s/%s/%s/%s", 
                       substr($hash, 0, 1),
                       substr($hash, 1, 1),
                       substr($hash, 2, 1),
                       $hash);
    $path .= '.'.$suffix if $suffix;
    $path;
}

1;

