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
    int(rand(0xfe)) + 1;
}

sub object_path {
    state $rule = Data::Validator->new(
        bucket_id => 'Natural',
        filename => 'Str',
        rid      => 'Natural'
    );
    my $args = $rule->validate(@_);
    my $filename = Encode::is_utf8($args->{filename}) 
        ? Encode::encode_utf8($args->{filename}) : $args->{filename};

    my $suffix;
    if ( $filename =~ m!\.([a-zA-Z0-9]+)$! ) {
        $suffix = $1;
    }
    my $hash = sha224_hex($args->{bucket_id} . '/' . $args->{rid} . '/' . $filename);

    my $path = sprintf("%s/%s/%s/%s", 
                       substr($hash, 0, 1),
                       substr($hash, 1, 1),
                       substr($hash, 2, 1),
                       $hash);
    $path .= '.'.$suffix if $suffix;
    $path;
}

1;

