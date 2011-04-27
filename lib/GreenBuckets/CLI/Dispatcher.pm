package GreenBuckets::CLI::Dispatcher;

use strict;
use warnings;
use parent qw/App::CLI::Command/;
use GreenBuckets;
use GreenBuckets::Config;
use GreenBuckets::Dispatcher;
use Pod::Usage;

use constant options => (
    'config|c=s' => 'config',
);

sub run {
    my ($self, $arg) = @_;

    unless ( $self->{config} ) {
        $self->usage();
        exit;
    }

    my $config = GreenBuckets::Config->load( $self->{config} );
    GreenBuckets::Dispatcher->new(
        config => $config,
    )->run;
}

1;
__END__

=encoding utf8

=head1 NAME

GreenBuckets::CLI::Dispatcher - run dispatcher httpd server

=head1 SYNOPSIS

  $ greenbuckets dispatcher -c config.pl

=cut

