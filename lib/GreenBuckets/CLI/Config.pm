package GreenBuckets::CLI::Config;

use strict;
use warnings;
use parent qw/App::CLI::Command/;
use GreenBuckets;
use Data::Section::Simple;

sub run {
    my ($self, $args) = @_;

    my $reader = Data::Section::Simple->new('GreenBuckets');
    my $all = $reader->get_data_section;

    print $all->{config};
}

1;

__END__

=encoding utf8

=head1 NAME

GreenBuckets::CLI::Config - output GreenBuckets sample configuration

=head1 SYNOPSIS

  $ greenbuckets config > config.pl

=cut
