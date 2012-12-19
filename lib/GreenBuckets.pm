package GreenBuckets;

use strict;
use warnings;
use 5.10.0;
use Mouse;
use Mouse::Util::TypeConstraints;

subtype 'Natural'
    => as 'Int'
    => where { $_ > 0 };

subtype 'Uint'
    => as 'Int'
    => where { $_ >= 0 };

subtype 'Replica'
    => as 'Int'
    => where { $_ > 1 };

subtype 'Flag'
    => as 'Int'
    => where { $_ <= 1 };

no Mouse::Util::TypeConstraints;

our $VERSION = '0.13';
our @TABLES = qw/nodes buckets objects jobqueue recovery putlock/;

__PACKAGE__->meta->make_immutable();
1;
__DATA__
@@ nodes
CREATE TABLE nodes (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    gid SMALLINT UNSIGNED NOT NULL,
    node VARCHAR(128) UNIQUE,
    online TINYINT UNSIGNED DEFAULT 0,
    fresh TINYINT UNSIGNED DEFAULT 0,
    remote TINYINT UNSIGNED DEFAULT 0,
    INDEX gid (gid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ buckets
CREATE TABLE buckets (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) UNIQUE,
    enabled TINYINT UNSIGNED DEFAULT 1,
    deleted TINYINT UNSIGNED DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ objects
CREATE TABLE objects (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    fid INT UNSIGNED NOT NULL,
    bucket_id INT UNSIGNED NOT NULL,
    rid SMALLINT UNSIGNED NOT NULL,
    gid SMALLINT UNSIGNED NOT NULL,
    filename VARCHAR(1024),
    INDEX (fid, bucket_id),
    INDEX (bucket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ jobqueue
CREATE TABLE jobqueue (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    func VARCHAR(64),
    args BLOB,
    try SMALLINT UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ recovery
CREATE TABLE recovery (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    args BLOB,
    try SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL,
    INDEX (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ putlock
CREATE TABLE putlock (
    fdigest VARBINARY(56) NOT NULL PRIMARY KEY,
    ctime TIMESTAMP NOT NULL,
    KEY (ctime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

@@ config
return +{
    # listening port of dispatcher
    dispatcher_port => 5000,

    # listening port of worker's status server 
    jobqueue_worker_port => 5101,

    # dispatcher's basic authorization id/pass
    # only used modification methods like PUT,POST,DELETE
    user => 'admin', 
    passwd => 'admin',

    # backend dav storages's basic authorization id/passs
    dav_user => 'storage',
    dav_passwd => 'storage',

    # fontend forwared proxy ip
    front_proxy => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # dispatcher's status page acl
    dispatcher_status_access => [qw!192.168.0.0/16 10.0.0.0/8 127.0.0.1!],

    # master/slave dsn
    master => ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
    slave => [
        ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd'],
        ['dbi:mysql:greenbuckets;host=127.0.0.1','user','passwd']
    ],

    #ttl of dsn hostname resolver
    dsn_resolver_cache_ttl => 5,

    # replica number
    replica => 3,

    # number of dispatcher worker
    dispatcher_max_worker => 20,
    # numbe of JobQueue worker
    jobqueue_max_worker => 5,
    recovery_max_worker => 2,

    # timeout seconds request to storage node
    timeout_for_get => 10,
    timeout_for_put => 30,

    # don't use directory on dav storage
    # eg: flat_dav=0  http://storage/[00-99]/[00-99]/\w{56}
    #     flat_dav=1  http://storage/\w{56}
    flat_dav => 0,

    # treat PATH_INFO as unescape uri, default 0
    # if 0, PATH_INFO will unescape and encode to utf8
    escaped_uri => 0,

    # additonal mimetypes
    # hashref '.suffix' => 'type'
    add_mime_type => {
        '.epub' => 'application/epub+zip',
    },
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


