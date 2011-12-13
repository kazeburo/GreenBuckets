package GreenBuckets::JobQueue;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use File::Temp qw();
use Encode;
use Log::Minimal;
use Scope::Container;
use IO::Socket::INET;
use Parallel::Prefork;
use Parallel::Scoreboard;
use GreenBuckets::Model;
use Term::ANSIColor qw//;
use Time::HiRes qw//;
use Mouse;

our $MAX_JOB = 5;

has 'config' => (
    is => 'ro',
    isa => 'GreenBuckets::Config',
    required => 1,
);

has 'scoreboard_dir' => (
    is => 'ro',
    isa => 'Str',
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
        base_dir => $self->scoreboard_dir
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
        $message = Encode::encode_utf8($message) if Encode::is_utf8($message);
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

    my $pm = Parallel::Prefork->new({
        max_workers  => $self->config->jobqueue_max_worker + $self->config->recovery_max_worker + 1,
        spawn_interval  => 1,
        trap_signals => {
            'TERM' => 'TERM',
            'HUP'  => 'TERM',
            'INT'  => 'TERM',
            'USR1' => undef,
        },
    });

    while ( $pm->signal_received !~ m!^(?:TERM|INT)$! ) {
        $pm->start(sub{
            srand();

            local $ENV{JOBQ_STOP};
            local $SIG{TERM} = sub { $ENV{JOBQ_STOP} = 1 };
            local $SIG{INT} = sub { $ENV{JOBQ_STOP} = 1 };

            my $stats = $scoreboard->read_all;
            my %running;
            for my $pid ( keys %{$stats} ) {
                my $val = $stats->{$pid};
                chomp($val);
                my @val = split /\s/, $val; 
                $running{$val[1]}++;
            }

            if ( !$running{worker} || $running{worker} < $self->config->jobqueue_max_worker ) {
                debugf "start jobqueue worker";
                $0 = "$0 (jobqueue worker)";
                $scoreboard->update('. worker');
                my $i = 0;
                while ( !$ENV{JOBQ_STOP} ) {
                    $scoreboard->update('A worker');
                    my $result = $self->model->dequeue;
                    $scoreboard->update('. worker');
                    $i++ if defined $result;
                    last if $i > $MAX_JOB;
                    my $sleep = (!defined($result) || $result == 0) ? 0.8+rand(0.2) : rand(0.05)+0.05;
                    Time::HiRes::sleep($sleep) if !$ENV{JOBQ_STOP};
                }
            }
            elsif ( !$running{recovery} || $running{recovery} < $self->config->recovery_max_worker ) {
                debugf "start recovery worker";
                $0 = "$0 (recovery jobqueue worker)";
                $scoreboard->update('. recovery');
                my $i = 0;
                while ( !$ENV{JOBQ_STOP} ) {
                    $scoreboard->update('A recovery');
                    my $result = $self->model->dequeue_recovery;
                    $scoreboard->update('. recovery');
                    $i++ if defined $result;
                    last if $i > $MAX_JOB;
                    my $sleep = (!defined($result) || $result == 0) ? 0.8+rand(0.2) : rand(0.05)+0.05;
                    Time::HiRes::sleep($sleep) if !$ENV{JOBQ_STOP};
                }
            }
            elsif ( !$running{status} ) {
                $scoreboard->update('A status');
                $self->status_server();
            }
            debugf "process finished";
            exit(0);
        });
    }
    $pm->wait_all_children;
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
    if ( !$sock ) {
        sleep 3;
        die $!;
    }

    debugf "start status_server port:%s", $self->config->jobqueue_worker_port;
    # status worker
    $0 = "$0 (jobqueue status worker)";
    local $SIG{TERM} = sub { exit(0) };
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

        my $db_stats;
        eval { 
            $db_stats = $self->model->stats;
        };
        $db_stats ||= {};

        $raw_stats = <<EOF;
Uptime: $uptime
BusyWorkers: $busy
IdleWorkers: $idle
ObjectsMaxID: $db_stats->{objects_maxid}
BucketsMaxID: $db_stats->{buckets_maxid}
Queue: $db_stats->{jobqueue}
RecoveryQueue: $db_stats->{recovery_queue}
EOF
        print $client $raw_stats;
    }
    exit(0);
}

__PACKAGE__->meta->make_immutable();
1;


