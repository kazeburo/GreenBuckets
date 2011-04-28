package GreenBuckets;

use strict;
use warnings;
use 5.10.0;
use Mouse;
use Mouse::Util::TypeConstraints;

subtype 'Natural'
    => as 'Int'
    => where { $_ > 0 };

subtype 'Replica'
    => as 'Int'
    => where { $_ > 1 };

subtype 'Flag'
    => as 'Int'
    => where { $_ == 0 || $_ == 1 };

no Mouse::Util::TypeConstraints;

our $VERSION = 0.01;
our @TABLES = qw/nodes buckets objects jobqueue/;

__PACKAGE__->meta->make_immutable();
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

@@ jobqueue
CREATE TABLE jobqueue (
    id BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    func VARCHAR(64),
    args BLOB,
    try SMALLINT UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8


@@ config
return +{
    # listen port of dispatcher
    port => 5000,

    # dispatcher's basic authorization id/pass
    user => 'admin', 
    passwd => 'admin',

    # dispatcher's permited ip access from
    allow_from => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # fontend proxy ip
    front_proxy => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # master/slave dbn
    master => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
    slave => [
        ['dbi:mysql:greenbuckets;host=127.0.0.2','user','passwd'],
        ['dbi:mysql:greenbuckets;host=127.0.0.3','user','passwd']
    ],

    # backend dav storages's basic authorization id/passs
    dav_user => 'storage',
    dav_passwd => 'storage'

    # replica number
    replica => 3,

    # number of dispatcher worker
    dispatcher_worker => 20,
    # numbe of JobQueue worker
    jobqueue_worker => 5,

};


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


