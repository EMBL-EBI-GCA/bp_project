#!/usr/bin/env perl 
use strict;
use warnings;
use XML::Twig;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use Getopt::Long;

my ( $in_file, $era_user, $era_pass );

GetOptions( 'infile=s'   => \$in_file,
            'era_user=s' => \$era_user,
            'era_pass=s' => \$era_pass,
          );

die `perldoc -t $0` if !$in_file || !$era_user || !$era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn); 
$era->dbc->db_handle->{LongReadLen} = 66000;

my $run_id_list = get_list(  $in_file ); 

my $run_id;

my $twig = XML::Twig->new( 
                twig_roots     =>  { 'RUN' => 
                                        sub{ my ($twig, $e) = @_; 
                                             my $acc_id = $e->att('accession');

                                             die $acc_id unless exists $$run_id_list{$acc_id}{RUN_EGA_ID};
                                             my $ega_id = $$run_id_list{$acc_id}{RUN_EGA_ID};
                                             $e->set_att( accession => $ega_id );
                                             $e->print();
                                           },
                                   },
                twig_handlers  =>  {  'RUN/IDENTIFIERS/PRIMARY_ID' =>  
                                         sub{  my ($twig, $e) = @_;
                                               my $acc_id = $e->text;
                                               $run_id = $acc_id ? $acc_id : '';
                  
                                               die $acc_id unless exists $$run_id_list{$acc_id}{RUN_EGA_ID};
                                               my $ega_id = $$run_id_list{$acc_id}{RUN_EGA_ID};
                                               $e->set_text( $ega_id ); 
                                            },
                                      'RUN/EXPERIMENT_REF' =>
                                         sub{  my ($twig, $e) = @_;
                                               my $exp_id     = $e->att('accession');
                                               my $exp_alias  = $e->att('refname'); 
 
                                               die $run_id unless exists $$run_id_list{$run_id}{NEW_EXPERIMENT_EGA_ID};
                                               die $run_id unless exists $$run_id_list{$run_id}{NEW_EXPERIMENT_ALIAS};

                                               my $new_exp_id    = $$run_id_list{$run_id}{NEW_EXPERIMENT_EGA_ID};
                                               my $new_exp_alias = $$run_id_list{$run_id}{NEW_EXPERIMENT_ALIAS};
                                            
                                               warn "changing experiment alias for $run_id, from $exp_alias, to $new_exp_alias",$/;
     
                                               $e->set_att( accession => $new_exp_id, refname => $new_exp_alias );
                                            },
                                      'RUN/EXPERIMENT_REF/IDENTIFIERS/PRIMARY_ID' =>
                                         sub{  my ($twig, $e) = @_;
                                               my $exp_id     = $e->text;
    
                                               die $run_id unless exists $$run_id_list{$run_id}{NEW_EXPERIMENT_EGA_ID};

                                               my $new_exp_id    = $$run_id_list{$run_id}{NEW_EXPERIMENT_EGA_ID};
                                               $e->set_text( $new_exp_id );
                                            },
                                      'RUN/EXPERIMENT_REF/IDENTIFIERS/SUBMITTER_ID' =>
                                         sub{  my ($twig, $e) = @_;
                                               my $exp_alias = $e->text;

                                               die $run_id unless exists $$run_id_list{$run_id}{NEW_EXPERIMENT_ALIAS};
                                               my $new_exp_alias = $$run_id_list{$run_id}{NEW_EXPERIMENT_ALIAS};
                                               $e->set_text( $new_exp_alias );
                                            },
                                      },
                  pretty_print             => 'indented',
                  keep_encoding            => 1,
                  twig_print_outside_roots => 0,
                );


my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(run_xml) xml from run where run_id = ?");

print '<RUN_SET>'.$/;    

foreach my $run_id ( keys %{ $run_id_list } ) {
  $xml_sth->execute( $run_id );
  my $xr = $xml_sth->fetchrow_arrayref();
  my ($xml) = @$xr; 
  $twig->parse($xml);
}
  
print '</RUN_SET>'.$/;    


sub get_ega_id {
  my ( $ena_id, $table, $type ) = @_;

  my $column_name = $table .'_'. $type;
  my $xml_sth = $era->dbc->prepare("select ega_id from $table where $column_name = ? ");
  $xml_sth->execute( $ena_id );
  my $xr = $xml_sth->fetchrow_arrayref();
  my $ega_id = $$xr[0];
  return $ega_id; 
}

sub get_list {
  my ( $file ) = @_;
  my %id_hash; 

  open my $fh ,'<', $file;

  while ( <$fh> ){ 
    chomp;
    next if /^#/;
 
    my ($run_id, $new_exp_alias) = split '\t';

    die $file,$/ if !$run_id || !$new_exp_alias;

    my $ega_exp_id = get_ega_id( $new_exp_alias, 'experiment', 'alias' );
    my $ega_run_id = get_ega_id( $run_id, 'run', 'id' );

    $id_hash{ $run_id }{ RUN_EGA_ID }            = $ega_run_id;
    $id_hash{ $run_id }{ NEW_EXPERIMENT_ALIAS }  = $new_exp_alias;
    $id_hash{ $run_id }{ NEW_EXPERIMENT_EGA_ID } = $ega_exp_id;
  }
  close( $fh );
  return \%id_hash;
}


=head1 Description

Sript for changing experiment details in the EGA run XML file

Options:

 -infile, tab-delimited file listing the ERA run id and the  new experiment alias name

 e.g RUN_ID <TAB> NEW_EXPERIMENT_ALIASE 

Parameters ERAPRO connection:

 -era_user, the name of the ERAPRO user
 -era_pass, the password for ERAPRO access

=head1 Examples

 Run it like this for the Blueprint project:

 perl run_xml_swap.pl -infile list_of_runs_for_sample_swap -era_user $ERA_USER_NAME -era_pass $ERA_PASSWORD 

=cut
                 
