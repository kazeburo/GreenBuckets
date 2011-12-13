package GreenBuckets::CLI::Jobqueue;

use strict;
use warnings;
use parent qw/App::CLI::Command/;
use GreenBuckets;
use GreenBuckets::Config;
use GreenBuckets::JobQueue;
use Pod::Usage;
use File::Temp qw//;

use constant options => (
    'config|c=s' => 'config',
    'scoreboard|s=s' => 'scoreboard',
);

sub run {
    my ($self, $arg) = @_;

    unless ( $self->{config} ) {
        $self->usage();
        exit;
    }

    my @dir_opt = (CLEANUP => 1);
    if ( $self->{scoreboard} ) {
        push @dir_opt, 'DIR' => $self->{scoreboard};
    }
    my $scoreboard_dir = File::Temp::tempdir(@dir_opt)

    my $config = GreenBuckets::Config->load( $self->{config} );
    GreenBuckets::JobQueue->new(
        config => $config,
        scoreboard_dir => $scoreboard_dir,
    )->run;
}

1;
__END__

=encoding utf8

=head1 NAME

GreenBuckets::CLI::Jobqueue - run jobqueue daemon

=head1 SYNOPSIS

  $ greenbuckets jobqueue -c config.pl -s /var/run/greenbuckets

=cut

