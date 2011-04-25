package GreenBuckets::Config;

use Mouse;
use Mouse::Util::TypeConstraints;

subtype 'Natural'
    => as 'Int'
    => where { $_ > 0 };

no Mouse::Util::TypeConstraints;

sub load {
    my $class = shift;
    my $fname = shift;
    my $config = do $fname or die "Cannot load configuration file: $fname";
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

has 'allow_from' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    default => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!]
);

has 'front_proxy' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    default => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!]
);

has 'master' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has 'slave' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has 'replica' => (
    is => 'ro',
    isa => 'Natural',
    default => 3,
);

has 'dispatcher_worker' => (
    is => 'ro',
    isa => 'Natural',
    default => 20,
);

has 'jobqueue_worker' => (
    is => 'ro',
    isa => 'Natural',
    default => 5,
);

__PACKAGE__->meta->make_immutable();
1;

