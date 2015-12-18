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

die `perldoc -t $0`  unless $in_file && $era_user && $era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $exp_id_list = get_list(  $in_file );

my $exp_id;

my $twig = XML::Twig->new(
             twig_roots      => { 'EXPERIMENT' => 
                                     sub{ my ($twig, $e) = @_; 
                                          my $acc_id = $e->att('accession');
                                          die $acc_id unless exists $$exp_id_list{$acc_id}{EXPERIMENT_EGA_ID};
                                          my $ega_id = $$exp_id_list{$acc_id}{EXPERIMENT_EGA_ID};
                                          $e->set_att( accession => $ega_id );
                                          $e->print(); 
                                       }},
             twig_handlers   => { 'EXPERIMENT/IDENTIFIERS/PRIMARY_ID' => 
                                     sub{  my ($twig, $e) = @_; 
                                           my $acc_id = $e->text;
                                           $exp_id = $acc_id ? $acc_id : '';
                                           die $acc_id unless exists $$exp_id_list{$acc_id}{EXPERIMENT_EGA_ID};
                                           my $ega_id = $$exp_id_list{$acc_id}{EXPERIMENT_EGA_ID};
                                           $e->set_text( $ega_id );
                                        },
                                  'EXPERIMENT/STUDY_REF' => 
                                     sub{  my ($twig, $e) = @_; 
                                           my $acc_id = $e->att('accession');
                                           my $ega_id = get_ega_id( $acc_id, 'study', 'id' );
                                           $e->set_att( accession => $ega_id );
                                        },
                                  'EXPERIMENT/STUDY_REF/IDENTIFIERS/PRIMARY_ID' =>
                                    sub{   my ($twig, $e) = @_; 
                                           my $acc_id = $e->text;
                                           my $ega_id = get_ega_id( $acc_id, 'study', 'id' );
                                           $e->set_text( $ega_id );
                                        },
                                   'EXPERIMENT/DESIGN/SAMPLE_DESCRIPTOR' => 
                                      sub{ my ($twig, $e) = @_;
                                           my $sample_id    = $e->att('accession');
                                           my $sample_alias = $e->att('refname'); 
                                           die $exp_id unless exists $$exp_id_list{$exp_id}{NEW_SAMPLE_EGA_ID};
                                           die $exp_id unless exists $$exp_id_list{$exp_id}{NEW_SAMPLE_ALIAS};

                                           my $new_sample_ega_id = $$exp_id_list{$exp_id}{NEW_SAMPLE_EGA_ID}; 
                                           my $new_sample_alias  = $$exp_id_list{$exp_id}{NEW_SAMPLE_ALIAS};
                                        
                                           warn "changing sample alias for $exp_id, from: $sample_alias, to: $new_sample_alias",$/;               
                                           $e->set_att( accession => $new_sample_ega_id, refname => $new_sample_alias );
                                        },
                                   'EXPERIMENT/DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID' => 
                                      sub{  my ($twig, $e) = @_;
                                            my $sample_id = $e->text;

                                            die $exp_id unless exists $$exp_id_list{$exp_id}{NEW_SAMPLE_EGA_ID};

                                            my $new_sample_ega_id = $$exp_id_list{$exp_id}{NEW_SAMPLE_EGA_ID}; 
                                            $e->set_text( $new_sample_ega_id );           
                                         },
                                   'EXPERIMENT/DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/SUBMITTER_ID' => 
                                      sub{  my ($twig, $e) = @_;
                                            my $sample_alias = $e->text;

                                            die $exp_id unless exists $$exp_id_list{$exp_id}{NEW_SAMPLE_ALIAS};
                                            my $new_sample_alias = $$exp_id_list{$exp_id}{NEW_SAMPLE_ALIAS}; 
                                                      
                                            $e->set_text( $new_sample_alias );
                                         },
                                   'EXPERIMENT/EXPERIMENT_ATTRIBUTES/EXPERIMENT_ATTRIBUTE[string(TAG)="EXPERIMENT_TYPE"]' => 
                                      sub{ my ($twig, $e) = @_;
                                           die $exp_id unless exists $$exp_id_list{$exp_id}{NEW_EXP_TYPE};
                                           my $new_exp_type = $$exp_id_list{$exp_id}{NEW_EXP_TYPE};

                                           foreach my $child ($e->children){
                                             if ($child->name eq 'VALUE'){
                                                my $value = $child->text;

                                                warn "changing exp type for $exp_id, from: $value to: $new_exp_type",$/
                                                  unless $value eq $new_exp_type;
                                                $child->set_text( $new_exp_type ); 
                                              }
                                            }
                                         },
                                   'EXPERIMENT/EXPERIMENT_ATTRIBUTES/EXPERIMENT_ATTRIBUTE[string(TAG)="CHIP_ANTIBODY"]' => 
                                      sub{ my ($twig, $e) = @_;
                                           
                                           die $exp_id,$/ unless exists $$exp_id_list{$exp_id}{NEW_CHIP_ANTOBODY};
                                           my $new_chip_antibody = $$exp_id_list{$exp_id}{NEW_CHIP_ANTOBODY};
            
                                           foreach my $child ($e->children){
                                             if ($child->name eq 'VALUE'){
                                                my $value = $child->text;
 
                                                warn "changing CHIP Antibody for $exp_id, from: $value to: $new_chip_antibody",$/
                                                  unless $value eq $new_chip_antibody;

                                                $child->set_text( $new_chip_antibody );
                                             }
                                           }
                                         },
                                   'EXPERIMENT/EXPERIMENT_ATTRIBUTES/EXPERIMENT_ATTRIBUTE[string(TAG)="CHIP_ANTIBODY_LOT"]' =>
                                      sub{ my ($twig, $e) = @_;
                                           die $exp_id,$/ unless exists $$exp_id_list{$exp_id}{NEW_ANTIBODY_LOT};
                                           my $new_antibody_lot = $$exp_id_list{$exp_id}{NEW_ANTIBODY_LOT};
        
                                           foreach my $child ($e->children){
                                             if ($child->name eq 'VALUE'){
                                                my $value = $child->text;

                                                warn "changing ChIP antibody lot for $exp_id, from: $value to: $new_antibody_lot",$/
                                                   unless $value eq $new_antibody_lot;

                                                $child->set_text( $new_antibody_lot );
                                             }
                                           }
                                         },  
                                       }, 
             pretty_print             => 'indented',
             keep_encoding            => 1,
             twig_print_outside_roots => 0,        # print the rest
           );

my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(experiment_xml) xml from experiment where experiment_id = ? ");

print '<EXPERIMENT_SET>'.$/;
foreach my $exp_id ( keys %{ $exp_id_list } ) {    

  $xml_sth->execute( $exp_id );
  my $xr = $xml_sth->fetchrow_arrayref();
  my ($xml) = @$xr;
  $twig->parse($xml);
}                    
print '</EXPERIMENT_SET>'.$/;    


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
  my @header;

  open my $fh ,'<', $file;
  while ( <$fh> ){
    chomp;
    next if /^#/;


    if( @header ){
      my @vals = split "\t";
      die "value missing in the list file: ", join("\t",@vals),$/ unless scalar @vals==5;

      my %val_hash;
      @val_hash{ @header } = @vals;
      
      my $exp_id            = $val_hash{EXPERIMENT_ID};
      my $new_sample_alias  = $val_hash{NEW_SAMPLE_ALIAS};
      my $new_exp_type      = $val_hash{NEW_EXPERIMENT_TYPE};
      my $new_chip_antibody = $val_hash{NEW_CHIP_ANTIBODY};
      my $new_antibody_lot  = $val_hash{NEW_CHIP_ANTIBODY_LOT};

      foreach my $key ( @header ){
        die $exp_id,$/ unless $val_hash{$key};
      }

      my $ega_sample_id = get_ega_id(  $new_sample_alias, 'sample', 'alias');
      my $ega_exp_id    = get_ega_id( $exp_id, 'experiment', 'id');
    
      $new_exp_type = 'Histone ' .$new_exp_type unless $new_exp_type =~ /^Histone/i;

      $id_hash{ $exp_id }{NEW_SAMPLE_ALIAS}  = $new_sample_alias;
      $id_hash{ $exp_id }{NEW_EXP_TYPE}      = $new_exp_type;
      $id_hash{ $exp_id }{NEW_CHIP_ANTOBODY} = $new_chip_antibody;
      $id_hash{ $exp_id }{NEW_ANTIBODY_LOT}  = $new_antibody_lot;
      $id_hash{ $exp_id }{NEW_SAMPLE_EGA_ID} = $ega_sample_id;
      $id_hash{ $exp_id }{EXPERIMENT_EGA_ID} = $ega_exp_id;
    }
    else {
      @header = split "\t";
    }
  }
  close( $fh );
  return \%id_hash;
}        

=head1 

This script generates xml file for EGA experiment entries with modified samples and experiment type details (for sample swap events).

Options:

 -infile, tab-delimited file listing the ERA experiment id, new sample alias, new experiment type (histone marks name) & ChIP antibody details, must have headers

 e.g EXPERIMENT_ID <TAB> NEW_SAMPLE_ALIAS <TAB> NEW_EXPERIMENT_TYPE <TAB> NEW_CHIP_ANTIBODY <TAB> NEW_CHIP_ANTIBODY_LOT

Parameters ERAPRO connection:

 -era_user, the name of the ERAPRO user
 -era_pass, the password for ERAPRO access

=head1 Examples

 Run it like this for the Blueprint project:

 perl chip_seq_sample_swap_xml.pl -infile list_of_experiments_for_sample_swap -era_user $ERA_USER_NAME -era_pass $ERA_PASSWORD 

=cut
