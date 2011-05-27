package GreenBuckets::Config;

use GreenBuckets;
use Mouse;

sub load {
    my $class = shift;
    my $fname = shift;
    my $config = eval {
        do $fname or die "Cannot load configuration file: $fname";
    };
    die $@ if $@;
    __PACKAGE__->new($config);
}

has 'dispatcher_port' => (
    is => 'ro',
    isa => 'Natural',
    default => 5500,
);

has 'jobqueue_worker_port' => (
    is => 'ro',
    isa => 'Natural',
    default => 5501,
);

has 'user' => (
    is => 'ro',
    isa => 'Str',
    default => '',
);

has 'passwd' => (
    is => 'ro',
    isa => 'Str',
    default => '',
);

has 'dav_user' => (
    is => 'ro',
    isa => 'Str',
    default => '',
);

has 'dav_passwd' => (
    is => 'ro',
    isa => 'Str',
    default => '',
);

has 'flat_dav' => (
    is => 'ro',
    isa => 'Flag',
    default => 0,
);

has 'front_proxy' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    default => sub { [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!] },
);

has 'dispatcher_status_access' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    default => sub { [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!] },
);

has 'master' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has 'slave' => (
    is => 'ro',
    isa => 'ArrayRef[Defined]',
    required => 1,
);

has 'replica' => (
    is => 'ro',
    isa => 'Replica',
    default => 3,
);

has 'dispatcher_max_worker' => (
    is => 'ro',
    isa => 'Natural',
    default => 20,
);

has 'jobqueue_max_worker' => (
    is => 'ro',
    isa => 'Natural',
    default => 5,
);

has 'recovery_max_worker' => (
    is => 'ro',
    isa => 'Natural',
    default => 2,
);

has 'agent_class' => (
    is => 'ro',
    isa => 'Str',
    default => 'GreenBuckets::Agent',
);

has 'timeout_for_get' => (
    is => 'ro',
    isa => 'Natural',
    default => 10,
);

has 'timeout_for_put' => (
    is => 'ro',
    isa => 'Natural',
    default => 30,
);

__PACKAGE__->meta->make_immutable();
1;

