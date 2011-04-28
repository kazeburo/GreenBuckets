use inc::Module::Install;
name 'GreenBuckets';
all_from 'lib/GreenBuckets.pm';

requires 'Data::Section::Simple';
requires 'Plack';
requires 'DBIx::Sunny';
requires 'DBD::mysql';
requires 'Furl';
requires 'Net::DNS::Lite';
requires 'Log::Minimal';
requires 'MIME::Base64';
requires 'Class::Accessor::Lite', 0.05;
requires 'Scope::Container';
requires 'Scope::Container::DBI';
requires 'List::Util';
requires 'HTTP::Exception';
requires 'Data::Validator';
requires 'parent';
requires 'Digest::MurmurHash';
requires 'Digest::SHA';
requires 'Hash::MultiValue';
requires 'App::CLI';
requires 'Mouse';
requires 'Pod::Usage';
requires 'Router::Simple';
requires 'File::Temp';
requires 'Term::ANSIColor';
requires 'Class::load';
requires 'Data::MessagePack';
requires 'Parallel::Prefork';
requires 'Parallel::Scoreboard';
requires 'Try::Tiny';

tests 't/*.t t/*/*.t';
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::TCP';
test_requires 'Test::mysqld' if $Module::Install::AUTHOR;

auto_set_repository;
#auto_include;
WriteAll;
