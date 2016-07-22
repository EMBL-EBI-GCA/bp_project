requires 'autodie';
requires 'DBI';
requires 'DBD::mysql';
requires 'Data::Dump';

on 'build' => sub {
  requires 'Module::Build::Pluggable';
  requires 'Module::Build::Pluggable::CPANfile';
};

on 'test' => sub {
  requires 'Test::More';
};
