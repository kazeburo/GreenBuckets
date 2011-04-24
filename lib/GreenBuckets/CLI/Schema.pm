package GreenBuckets::CLI::Schema;

use strict;
use warnings;
use parent qw/App::CLI::Command/;
use GreenBuckets;
use Data::Section::Simple;

sub run {
    my ($self, $opt, $args) = @_;


    my $reader = Data::Section::Simple->new('GreenBuckets');
    my $all = $reader->get_data_section;

    my $schema;
    $schema .= join ";\n\n",
        map { my $t = $all->{$_}; $t =~ s/\s$//g; $t } 
            @GreenBuckets::TABLES;
    $schema .= ";\n";
    print $schema;
}

1;

__END__

=encoding utf8

=head1 NAME

GreenBuckets::CLI::Schema - output GreenBuckets datatabse schema

=head1 SYNOPSIS

  $ greenbuckets schema | mysql greenbuckets

=cut


