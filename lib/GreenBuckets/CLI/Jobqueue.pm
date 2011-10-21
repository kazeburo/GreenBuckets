package GreenBuckets::CLI::Jobqueue;

use strict;
use warnings;
use parent qw/App::CLI::Command/;
use GreenBuckets;
use GreenBuckets::Config;
use GreenBuckets::JobQueue;
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
    GreenBuckets::JobQueue->new(
        config => $config,
    )->run;
}

1;
__END__

=encoding utf8

=head1 NAME

GreenBuckets::CLI::Jobqueue - run jobqueue daemon

=head1 SYNOPSIS

  $ greenbuckets jobqueue -c config.pl

=cut

