#!/usr/bin/env perl
use strict;
use warnings;
use DBI;
use Getopt::Long;

my $database;
my $hostname;
my $port;
my $user;
my $password;
my $infile;
my $out_file = 'array_sample_group_infile';


GetOptions( 'database=s' => \$database,
            'hostname=s' => \$hostname,
            'port=s'     => \$port,
            'user=s'     => \$user,
            'password=s' => \$password,
            'infile=s'   => \$infile,
            'out_file=s' => \$out_file,
          );



my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";
my $dbh = DBI->connect($dsn, $user, $password);
my $sth = $dbh->prepare("SELECT a.array_sample_id,f.file_id FROM array_sample a, file f where f.name = ? and a.study_source_id = ? and a.sample_name = ?");

open my $IN,'<',$infile;
open my $OUT, '>', $out_file;

while( <$IN> ){
  chomp;
  next if /^#/;
  my ($study, $sample, $file) = split "\t";
  $sth->execute($file, $study, $sample);
  while(my $row=$sth->fetchrow_arrayref()){
    print $OUT join ( "\t", @$row),$/;
  }
}
close($IN);
close($OUT);
$sth->finish;
$dbh->disconnect();
