package GreenBucktes::Model;

use strict;
use warnings;
use utf8;
use Scope::Container;
use Scope::Container::DBI;
use GreenBucktes::Util qw/filename_id internal_path/;
use GreenBucktes::Schema;
use GreenBucktes::Dispatcher::Response;
use List::Util qw/shuffle/;
use HTTP::Exception;
use Log::Minimal;
use Class::Accessor::Lite (
    new => 1,
);

sub slave {
    my $self = shift;
    my $dbh = Scope::Container::DBI->new(...);
    GreenBucktes::Schema->new(dbh=>$dbh, readonly=1);
}

sub master {
    my $self = shift;
    my $dbh = Scope::Container::DBI->new(...);
    GreenBucktes::Schema->new(dbh=>$dbh);
}

sub get_object {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $sc = start_scope_container();

    my $slave = $self->slave;
    my $bucket = $slave->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(404) unless $bucket; # 404
    HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    HTTP::Exception->throw(404) if $bucket->{deleted}; # 404;

    my @uri = $slave->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    undef $sc;

    my @r_uri = grep { $_->{can_read} } @uri;
    if ( !@r_uri ) {
        warnf "all storage cannot read %s", \@uri;
        HTTP::Exception->throw(500);
    }

    # XXX res
    my $res = GreenBucktes::Dispatcher::Response->new(200);
    $res->body(Dumper(@r_uri));
    $res;
}

sub get_bucket {
    my $self = shift;
    my ($bucket_name) = @_;

   my $sc = start_scope_container();

   my $slave = $self->slave;
   my $bucket = $slave->select_bucket(
       name => $bucket_name
   );
   HTTP::Exception->throw(404) unless $bucket; # 404
   HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
   HTTP::Exception->throw(503) if $bucket->{deleted}; # 404;

   return 1;
}

sub put_object {
    my $self = shift;
    my ($bucket_name, $filename, $fh) = @_;

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(403) if ! $bucket->{enabled};
    HTTP::Exception->throw(503) if $bucket->{deleted}; #XXX 

    if ( $bucket && $self->retrieve_object( bucket_id => $bucket->{id}, filename => $filename ) ) {
        warnf "duplicated upload %s/%s", $bucket, $filename;
        HTTP::Exception->throw(409);
    }

    my @f_nodes = $master->retrieve_fresh_nodes(
        bucket_name => $bucket_name, 
        filename => $filename
    );
    undef $sc;

    my $try=3;
    my $gid;
    for my $f_node ( @f_node ) {

        my $f_gid = $f_node->{gid};
        my $f_uri = $f_node->{uri}->[0];

        # upload first node

        if ( success ) {
            infof "%s/%s was uploaded to group_id:%s %s", $bucket, $filename, $f_gid, $f_uri;
            $gid = $f_gid;
            last;
        }

        infof "Failed upload to group_id:%s %s", $f_gid, $f_uri;
        --$try;
        if ( $try == 0 ) {
            warnf "try time exceed for upload %s/%s", $bucket, $filename;
            last;
        }
    }

    if ( !$gid ) {
        warnf "Upload failed %s/%s", $bucket, $filename;
        return HTTP::Exception->throw(500);
    }

    my $sc2 = start_scope_container();
    $master->insert_object( 
        gid => $gid, 
        bucket_name => $bucket_name,
        filename => $filename
    );

    return GreenBucktes::Dispatcher::Response->new(206);
}

sub delete_object {
    my $self = shift;
    my ($bucket_name, $filename) = @_;

    my $sc = start_scope_container();
    my $master = $self->master;
    my $bucket = $master->select_bucket(
        name => $bucket_name
    );
    HTTP::Exception->throw(404) unless $bucket; # 404
    HTTP::Exception->throw(403) if ! $bucket->{enabled}; # 403
    HTTP::Exception->throw(404) if $bucket->{deleted}; # 404;

    my @uri = $master->retrieve_object_nodes(
        bucket_id => $bucket->{id},
        filename => $filename,
    );

    # remove file

    $master->delete_object(
        bucket_id => $bucket_id->{id}, 
        filename => $filename
    );

    return GreenBucktes::Dispatcher::Response->new(200);
}

1;

