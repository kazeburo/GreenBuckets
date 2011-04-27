package GreenBuckets::JobQueue;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use File::Temp qw();
use Log::Minimal;
use Scope::Container;
use IO::Socket::INET;
use Parallel::Prefork;
use Parallel::Scoreboard;
use GreenBuckets::Model;
use Mouse;

our $MAX_JOB = 100;
our $SLEEP = 1;

has 'config' => (
    is => 'ro',
    isa => 'GreenBuckets::Config',
    required => 1,
);

has 'model' => (
    is => 'ro',
    isa => 'GreenBuckets::Model',
    lazy_build => 1,
);

has 'scoreboard' => (
    is => 'ro',
    isa => 'Parallel::Scoreboard',
    lazy_build => 1,
);

sub _build_model {
    my $self = shift;
    GreenBuckets::Model->new( config => $self->config );
}

sub _build_scoreboard {
    my $self = shift;
    Parallel::Scoreboard->new(
        base_dir => File::Temp::tempdir(CLEANUP => 1)
    );
}

sub run {
    my $self = shift;

    my $scoreboard = $self->scoreboard;
    my $status_server_pid = $self->status_server;

    my $pm = Parallel::Prefork->new({
        max_workers  => $self->config->jobqueue_max_worker,
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
            USR1 => undef,
        }
    });

    while ( $pm->signal_received ne 'TERM' ) {
        $pm->start and next;
        $0 = "$0 (jobqueue worker)";       
        $scoreboard->update('.');

        my $stop;
        my $i = 0;
        local $SIG{TERM} = sub { $stop++ };

        while ( !$stop ) {
            $scoreboard->update('A');
            my $sc = start_scope_container;
            my $model = $self->model;
            my $result = $model->dequeue;
            undef $sc;
            $scoreboard->update('.');
            $i++ if $result;
            last if $i > $MAX_JOB;
            sleep $SLEEP;
        }
        
        debugf "[%s] finished", $$;
    }

    kill 'TERM', $status_server_pid;
    waitpid( $status_server_pid, 0 );
}


sub status_server {
    my $self = shift;
    my $scoreboard = $self->scoreboard;
    my $start_time = time();

    my $sock = IO::Socket::INET->new(
        Listen => 5,
        LocalPort => $self->config->jobqueue_worker_port,
        Proto  => 'tcp',
        Reuse  => 1,
    );
    die $! unless $sock;

    my $pid = fork;
    die "fork failed: $!" unless defined $pid;

    return $pid if $pid;

    # status worker
    $0 = "$0 (jobqueue status worker)";
    $SIG{TERM} = sub { exit(0) };
    while ( 1 ) {
        my $client = $sock->accept();

        my $uptime = time - $start_time;
        my $stats = $scoreboard->read_all();
        my $raw_stats;
        my $busy = 0;
        my $idle = 0;
        for my $pid ( sort { $a <=> $b } keys %$stats) {
            if ( $stats->{$pid} =~ m!^A! ) {
                $busy++;
            }
            else {
                $idle++;
            }
        }
        $raw_stats = <<EOF;
Uptime: $uptime
BusyWorkers: $busy
IdleWorkers: $idle
EOF
        print $client $raw_stats;
    }
    exit(0);
}

__PACKAGE__->meta->make_immutable();
1;


