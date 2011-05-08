package GreenBuckets::Util;

use strict;
use warnings;
use 5.10.0;
use parent qw/Exporter/;
use Encode;
use Digest::MurmurHash qw/murmur_hash/;
use Digest::SHA qw/sha224_hex/;
use GreenBuckets;
use Data::Validator;

our @EXPORT_OK = qw/filename_id gen_rid object_path/;

sub filename_id($) {
    my $filename = shift;
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    murmur_hash($filename);
}

my $PID=$$;
sub gen_rid() {
    if ( $PID != $$ ) {
        $PID=$$;
        srand();
    }
    int(rand(65535)) + 1;
}

sub object_path {
    state $rule = Data::Validator->new(
        filename  => 'Str',
        bucket_id => 'Natural',
        rid       => 'Natural'
    );
    my $args = $rule->validate(@_);
    my $filename = $args->{filename};
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    my $fid = filename_id($filename);
    my $hash = sha224_hex($args->{bucket_id}.'/'.$args->{rid}.'/'.$filename);
    my $path = sprintf("%02d/%02d/%s",
                       int( $fid % 10000 / 100),
                       $fid % 100,
                       $hash);
    $path;
}

1;

