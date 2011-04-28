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
use Log::Minimal;
use Term::ANSIColor qw//;
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

our $DEFAULT_COLOR = {
    info  => { text => 'green', },
    debug => {
        text       => 'red',
        background => 'white',
    },
    'warn' => {
        text       => 'black',
        background => 'yellow',
    },
    'critical' => {
        text       => 'black',
        background => 'red'
    }
};

sub build_logger {
    my ($self) = @_;
    return sub {
        my ( $time, $type, $message, $trace) = @_;
        my $raw_message = $message;
        if ( $ENV{PLACK_ENV} && $ENV{PLACK_ENV} eq 'development' ) {
             $message = Term::ANSIColor::color($DEFAULT_COLOR->{lc($type)}->{text}) 
                 . $message . Term::ANSIColor::color("reset")
                 if $DEFAULT_COLOR->{lc($type)}->{text};
             $message = Term::ANSIColor::color("on_".$DEFAULT_COLOR->{lc($type)}->{background}) 
                 . $message . Term::ANSIColor::color("reset")
                 if $DEFAULT_COLOR->{lc($type)}->{background};
        }
        print STDERR sprintf("%s [%s] [%s] %s at %s\n", $time, $$, $type, $message, $trace);
    };
}


sub run {
    my $self = shift;

    local $ENV{$Log::Minimal::ENV_DEBUG} = ($ENV{PLACK_ENV} && $ENV{PLACK_ENV} eq 'development') ? 1 : 0;
    local $Log::Minimal::AUTODUMP = 1;
    local $Log::Minimal::PRINT = $self->build_logger();

    my $scoreboard = $self->scoreboard;
    my $status_server_pid = $self->status_server;

    my $pm = Parallel::Prefork->new({
        max_workers  => $self->config->jobqueue_max_worker,
        trap_signals => {
            'TERM' => 'TERM',
            'HUP'  => 'TERM',
            'INT'  => 'TERM',
            'USR1' => undef,
        }
    });

    while ( $pm->signal_received !~ m!^(?:TERM|INT)$! ) {
        $pm->start(sub{
            debugf "process start";
            $0 = "$0 (jobqueue worker)";       
            $scoreboard->update('.');

            local $ENV{JOBQ_STOP};
            my $i = 0;
            local $SIG{TERM} = sub { $ENV{JOBQ_STOP} = 1 };
            local $SIG{INT} = sub { $ENV{JOBQ_STOP} = 1 };

            while ( !$ENV{JOBQ_STOP} ) {
                $scoreboard->update('A');
                my $result = $self->model->dequeue;
                $scoreboard->update('.');
                $i++ if $result;
                last if $i > $MAX_JOB;
                sleep $SLEEP unless $ENV{JOBQ_STOP};
            }
        
            debugf "process finished";
        });
    }
    $pm->wait_all_children;

    debugf "kill status_server pid:%s", $status_server_pid;
    kill 'TERM', $status_server_pid;
    waitpid( $status_server_pid, 0 );
    debugf "all finished";
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

    debugf "start status_server port:%s", $self->config->jobqueue_worker_port;
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


