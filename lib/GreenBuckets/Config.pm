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

has 'port' => (
    is => 'r',
    isa => 'Natural',
    default => 5000,
);

has 'user' => (
    is => 'r',
    isa => 'Str',
    default => 'admin',
);

has 'passwd' => (
    is => 'r',
    isa => 'Str',
    default => 'admin',
);

has 'allow_from' => (
    is => 'r',
    isa => 'ArrayRef[Str]',
    default => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!]
);

has 'front_proxy' => (
    is => 'r',
    isa => 'ArrayRef[Str]',
    default => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!]
);

has 'master' => (
    is => 'r',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has 'slave' => (
    is => 'r',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has 'replica' => (
    is => 'r',
    isa => 'Natural',
    default => 3,
);

has 'dispatcher_worker' => (
    is => 'r',
    isa => 'Natural',
    default => 20,
);

has 'jobqueue_worker' => (
    is => 'r',
    isa => 'Natural',
    default => 5,
);

__PACKAGE__->meta->make_immutable();
