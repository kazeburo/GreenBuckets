use strict;
use Test::More;
use t::TestConfig;
use GreenBuckets::Dispatcher;
use Plack::Builder;
use HTTP::Request;
use HTTP::Response;
use HTTP::Message::PSGI;
use JSON;

sub req_url {
    my $app = shift;
    my $method = shift;
    my $url = shift;
    my $req = HTTP::Request->new(GET => 'http://127.0.0.1' . $url);
    res_from_psgi $app->(req_to_psgi($req));
}

sub json_post {
    my $app = shift;
    my $url = shift;
    my $args = shift;
    my $content = encode_json($args);
    my $req = HTTP::Request->new(POST => 'http://127.0.0.1' . $url,[ 'Content-Type' => 'application/json'], $content);
    res_from_psgi $app->(req_to_psgi($req));
}

if ( !$ENV{TEST_MYSQLD} ) {
    plan skip_all => 'TEST_MYSQLD is false';
}

my $config = t::TestConfig->setup;
my $dispatcher = GreenBuckets::Dispatcher->new( config => $config );
ok($dispatcher);

my $app = $dispatcher->build_app();
ok($app);
$app = builder {
    enable 'HTTPExceptions';
    $app;
};

my $res;

{
    $res = req_url($app, 'GET', '/baz2/');
    is($res->code, 200);
    $res = json_post($app, '/baz2/', {method=>'rename', rename_to=>'baz'});
    is($res->code, 200);
    $res = req_url($app, 'GET', '/baz/');
    is($res->code, 200);

    $res = json_post($app, '/baz/', {method=>'rename', rename_to=>'foo'});
    is($res->code, 409);
}

{
    $res = json_post($app, '/baz/', {method=>'delete_bucket'});
    $res = req_url($app, 'GET', '/baz/');
    is($res->code, 503);
}

done_testing();

