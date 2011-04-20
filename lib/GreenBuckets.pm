package GreenBuckets;

use strict;
use warnings;
use 5.10.0;

our $VERSION = 0.01;
our @TABLES = qw/nodes buckets objects/;

1;
__DATA__
@@ nodes
CREATE TABLE nodes (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    gid SMALLINT UNSIGNED NOT NULL,
    node VARCHAR(128) UNIQUE,
    can_read TINYINT UNSIGNED DEFAULT 0,
    is_fresh TINYINT UNSIGNED DEFAULT 0,
    INDEX gid (gid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ buckets
CREATE TABLE buckets (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(32) UNIQUE,
    enabled TINYINT UNSIGNED DEFAULT 1,
    deleted TINYINT UNSIGNED DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ objects
CREATE TABLE objects (
    fid INT UNSIGNED NOT NULL,
    bucket_id INT UNSIGNED NOT NULL,
    rid SMALLINT UNSIGNED NOT NULL,
    gid SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY(fid, bucket_id),
    INDEX (bucket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

__END__

=encoding utf8

=head1 NAME

GreenBuckets - simple object storage

=head1 SYNOPSIS

  ...

=head1 DESCRIPTION

GreenBuckets is simple object storage system, ordinaly used for mass image storage

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo {at} gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright (C) Masahiro Nagano

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut


