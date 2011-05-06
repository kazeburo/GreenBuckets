package GreenBuckets::Util;

use strict;
use warnings;
use 5.10.0;
use parent qw/Exporter/;
use Encode;
use Digest::FNV qw/fnv32a fnv64a/;
use Digest::SHA qw/sha224_hex/;
use GreenBuckets;
use Data::Validator;

our @EXPORT_OK = qw/filename_id sort_hash gen_rid object_path/;

sub filename_id($) {
    my $filename = shift;
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    fnv64a($filename)->{longlong};
}

sub sort_hash($) {
    my $filename = shift;
    $filename = Encode::encode_utf8($filename) if Encode::is_utf8($filename);
    fnv32a($filename);
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
        bucket_id => 'Natural',
        fid => 'Natural',
        rid      => 'Natural'
    );
    my $args = $rule->validate(@_);

    my $hash = sha224_hex($args->{bucket_id} . '/' . $args->{rid} . '/' . $args->{fid});
    my $path = sprintf("%02d/%02d/%s",
                       int( $args->{rid} % 10000 / 100),
                       $args->{rid} % 100,
                       $hash);
    $path;
}

1;

