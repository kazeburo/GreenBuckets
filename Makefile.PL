use inc::Module::Install;
name 'GreenBuckets';
all_from 'lib/GreenBuckets.pm';

requires 'Data::Section::Simple';
requires 'Plack';
requires 'DBIx::Sunny';
requires 'DBD::mysql';
requires 'Coro';
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

tests 't/*.t t/*/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
#auto_include;
WriteAll;
