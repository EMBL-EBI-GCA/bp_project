#!/usr/bin/env perl
use strict;
use warnings;

use Module::Build::Pluggable ( 'CPANfile' );

my $builder = Module::Build::Pluggable->new(
    module_name => 'BlueprintFileMovePath',
    license     => 'apache',
);
$builder->create_build_script();
