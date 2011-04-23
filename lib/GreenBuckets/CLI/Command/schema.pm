package GreenBuckets::CLI::Command::schema;

use strict;
use warnings;
use GreenBuckets::CLI -command;
use GreenBuckets;
use Data::Section::Simple;

sub abstract { "Output greenbuckets database schema" }

sub run {
    my ($self, $opt, $args) = @_;

    my $database = $opt->{database} || 'greenbuckets';

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

