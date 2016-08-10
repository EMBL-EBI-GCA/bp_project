use strict;
use warnings;
use Data::Dumper;
use File::Basename;
use Getopt::Long;

my $root_dir;
my $ftp_base ;
my $file_base;
my $index_file;
my $parent_name        = 'bp';
my $parent_tag         = 'BLUEPRINT';
my $track_priority     = 4;
my $sample_shortname   = undef; ### require for trackhub short label
my $shortname_keyword  = 'SHORT_NAME';
my $analysis_info_file = undef;
my $epirr_index        = undef;

my $track_on_sample_barcode = 'C0010K';
my $track_on_cell_type      = 'CD14-positive, CD16-negative classical monocyte';

&GetOptions(
  'search=s'          => \$root_dir,
  'ftp_base=s'        => \$ftp_base,
  'file_base=s'       => \$file_base,
  'index_file=s'      => \$index_file,
  'short_name_file=s' => \$sample_shortname,
  'analysis_info=s'   => \$analysis_info_file,
);

my $usage =<<USAGE;

perl $0 -index_file <index_file> -ftp_base <ftp_base> -analysis_info <tab delimited file>  -short_name_file <short_name_file>

USAGE

die $usage unless ( $index_file && $ftp_base && $analysis_info_file );

### list of views ###

my @views = qw/ region      
                signal
             /;     

### list of experiments ###
                   
my @experiment_types = qw/ Bisulfite-Seq 
                           ChIP-Seq 
                           DNase-Hypersensitivity 
                           RNA-Seq
                         /; 
                         
my %experiment_types_hash = map { $_ => 1} @experiment_types;

### set tags for trackhub display ###

my @dimension_tags = qw/  experiment
                          sample_description
                          sample_source
                          sample_barcode
                          analysis_group
                          analysis_type                         
                      /;
                      
my @priority_tags = qw/  view
                         analysis_type
                         sample_source
                         sample_barcode
                         sample_description
                         analysis_group  
                     /;                     

my @subgroup_tags =  ( @dimension_tags, ,"sample_description_3", "view" );

### read epirr index ###
my $epirr_hash = {};
$epirr_hash = read_epirr_index( $epirr_index )
                if $epirr_index;

### read index file ###
                         
my @index_data = sort {
       $a->{SAMPLE_NAME} cmp $b->{SAMPLE_NAME}
    || $a->{EXPERIMENT_TYPE} cmp $b->{EXPERIMENT_TYPE}
    } grep { $_->{FILE} =~ /\.b[bw]$/ } @{ read_file($index_file) };

$ftp_base  .= '/' unless ( $ftp_base  =~ /\/$/ );
$file_base .= '/' unless ( $file_base =~ /\/$/ );

### read analysis info ###

my $analysis_info_array = read_file( $analysis_info_file );

### add info to hash ###

map { add_data_to_file_entry( $_, $ftp_base, $file_base, $analysis_info_array, $epirr_hash ) } @index_data;
 
my $track_label_hash = track_analysis_label( $sample_shortname, $shortname_keyword );

my @all_ls_data;
my @all_region_data;
my @all_signal_data;

foreach(@index_data){
  push (@all_ls_data, $_)  if(exists($experiment_types_hash{$_->{LIBRARY_STRATEGY}}));
}


### writing main composite block ####

my $tag_setup        = tags( \@dimension_tags, \@priority_tags, \@subgroup_tags );
my $composite_setup  = composite_config($parent_tag);
my $parent_track     = $composite_setup->{track_name};

write_composite_block( \@all_ls_data, $composite_setup, $tag_setup, $track_priority )
    if ( @all_ls_data );



### writing sub tracks ###

foreach my $view ( @views ) {
  my $sub_composite_setup  = composite_config( $view );    
  my $track_name           = $sub_composite_setup->{track_name};
  my $short_label          =  $sub_composite_setup->{short_label};
  
  write_sub_tracks ( $track_name, $view, $short_label,  $parent_track );

### writing tracks ###  

  foreach my $exp ( @experiment_types ) {
    my $exp_composite_setup  = composite_config( $exp );
    my $rtrack_name          = $exp_composite_setup->{track_name};
        
    @all_region_data = grep { $_->{view} eq "region" && $_->{LIBRARY_STRATEGY} eq $exp } @all_ls_data;
    @all_signal_data = grep { $_->{view} eq "signal" && $_->{LIBRARY_STRATEGY} eq $exp } @all_ls_data;
     
    if ( $view eq "region" ) {        
      my $visible_track_limit = $exp_composite_setup->{num_visible_regions};
            
      write_region_tracks( \@all_region_data, $rtrack_name,  $track_name, 
           $visible_track_limit, $tag_setup, $track_label_hash,
           $track_on_sample_barcode, $track_on_cell_type );
    }
    elsif ( $view eq "signal" ) {    
      my $visible_track_limit = $exp_composite_setup->{num_visible_signals}; 
      
      write_signal_tracks( \@all_signal_data, $rtrack_name, $track_name, 
            $visible_track_limit, $tag_setup, $track_label_hash,
            $track_on_sample_barcode, $track_on_cell_type );        
    }   
  }                          
}


#######################

sub read_epirr_index {
  my ( $epirr_index ) = @_;
  open my $fh, '<', $epirr_index;
  my @header;
  my %epirr_hash;

  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;
    die unless scalar @vals == 3;
 
    if (@header) {
      $epirr_hash{ $vals[0] } = $vals[2];      
    }
    else {
      @header = map { uc($_) } @vals;
    }
  }
  close( $fh );
  return \%epirr_hash;
}


sub read_file {
  my ( $file ) = @_;

  open my $fh, '<', $file || die("Could not open $file: $!");

  my @header;
  my @data;
  while ( <$fh> ) {
    chomp;
    next if m/^#/;

    my @vals = split "\t", $_;

    if (@header) {
      my %row;
      @row{@header} = @vals;
      push @data, \%row;
    }
    else {
      @header = map { uc($_) } @vals;
    }
  }
  close( $fh );

  return \@data;
}


sub add_data_to_file_entry {
  my ( $fe, $ftp_base, $file_base, $analysis_info_array, $epirr_hash ) = @_;
  
  $fe = get_analysis_details( $fe, $analysis_info_array );
  
  my $sample_name = $fe->{SAMPLE_NAME};
  $sample_name =~ s{\s+}{_}g;
  $sample_name =~ s{/}{_}g;
 
  
  $fe->{SAMPLE_NAME}           = $sample_name; 
  $fe->{sample_id}             = $fe->{SAMPLE_NAME};  
  $fe->{lab}                   = $fe->{CENTER_NAME};
  $fe->{cell_type}             = $fe->{CELL_TYPE};
  $fe->{donor_id}              = $fe->{DONOR_ID};
  $fe->{tissue}                = $fe->{TISSUE_TYPE};
  $fe->{url}                   = $ftp_base  . $fe->{FILE};
  $fe->{file_path}             = $file_base . $fe->{FILE};  
  $fe->{sample_description_1}  = $fe->{SAMPLE_DESC_1};       ### fix all CAPS on/off
  $fe->{sample_description_2}  = $fe->{SAMPLE_DESC_2};
  $fe->{sample_description_3}  = $fe->{SAMPLE_DESC_3}; 
  $fe->{analysis_group}        = $fe->{ANALYSIS_GROUP};
  $fe->{analysis_type}         = $fe->{ANALYSIS_TYPE};
  $fe->{experiment}            = $fe->{EXPERIMENT};
  $fe->{short_analysis_type}   = $fe->{SHORT_ANALYSIS_TYPE};

  my $exp_id = $fe->{EXPERIMENT_ID};
  my $epirr_id;
  $epirr_id = $$epirr_hash{ $exp_id } if exists $$epirr_hash{ $exp_id };
  $fe->{EPIRR_ID} = $epirr_id if $epirr_id;

  if ( $fe->{FILE} =~ /\.bw$/ ) {
    $fe->{view} = 'signal';
  }
  elsif ( $fe->{FILE} =~ /\.bb$/ ) {
    $fe->{view} = 'region';
  }
  else {
    die "Cannot deteriment view type for " . Dumper($fe);
  }
  
  $fe->{sample_description} = $fe->{SAMPLE_DESC_3};
  $fe->{sample_source}      = $fe->{SAMPLE_DESC_1};
  $fe->{sample_barcode}     = $fe->{SAMPLE_DESC_2};
     
  if ( $fe->{BIOMATERIAL_TYPE} =~ m/Cell Line/i ) {   ### cell line should have "SEX", not "DONER_SEX"
    $fe->{SEX} = $fe->{DONOR_SEX};        
  }  
}

sub get_analysis_details {  
  my ( $fe, $analysis_info_array ) = @_;
  
  if ( $analysis_info_array  ) {

    my $library_strategy = $fe->{LIBRARY_STRATEGY};
    $library_strategy =~ s{\s+}{_}g;
    $library_strategy = uc( $library_strategy );
  
    my $file_type = $fe->{FILE_TYPE};
    my $file = $fe->{FILE};
    
    foreach my $info( @{$analysis_info_array} ) {
      my $info_file_type = $info->{FILE_TYPE};
      my $info_file = $info->{FILE};
    
      if ( $info->{LIBRARY_STRATEGY} eq $library_strategy &&  $info_file_type=~ /$file_type/ ) {  
      
        if ( $info_file && $info_file ne '.' ) { ### hack for RNA-seq, comment if plus and minus strand data have their own file_type    
             
          if (  $info_file =~ /^!/ ) {
            $info_file =~ s{!}{}g;
            $fe = add_info( $fe, $info ) if  $file !~ /$info_file/;
          }
          else {          
            $fe = add_info( $fe, $info ) if  $file =~ /$info_file/;         
          }  
        }
        else {        
          $fe = add_info( $fe, $info );
        }
      }
    }
  }
  return $fe;
}

sub add_info {  
  my ( $fe, $info ) = @_;
   
   foreach my $key ( keys %{ $info } ) {
     next if $key =~ /(LIBRARY_STRATEGY|FILE_TYPE|FILE)/;
     next if $info->{$key} eq '.';
          
     $fe->{$key} = $info->{$key}; ### adding analysis info to file  
   }
   $fe->{EXPERIMENT} = $fe->{EXPERIMENT_TYPE} if !$fe->{EXPERIMENT};
 return $fe; 
}

sub tags {  
  my ( $dimension_tags, $priority_tags, $subgroup_tags ) = @_;
                                     
  my %tags = ( sub_group_tags  => $subgroup_tags,
               dim_tags        => $dimension_tags,
               priority_tags   => $priority_tags,
             );             
  return \%tags;
}

sub composite_config {
  my ($ls) = @_;
  $ls =~ s{\s+}{_}g;
  $ls = uc($ls);
  
  my %cc = (                              ### project specific code
    'DNASE-HYPERSENSITIVITY' => {
      track_name          => 'bpDnase',
      short_label         => 'Blueprint DNase-seq',
      long_label          => 'Blueprint DNase-seq Peaks and Signal of Open Chromatin',
      num_visible_regions => 1,
      num_visible_signals => 0,
    },
    'BISULFITE-SEQ' => {
      track_name          => 'bpDNAMeth',
      short_label         => 'Blueprint DNA Methylation',
      long_label          => 'Blueprint DNA Methylation',
      num_visible_regions => 2,
      num_visible_signals => 0,
    },
    'CHIP-SEQ' => {
      track_name          => 'bpHistoneMods',
      short_label         => 'Blueprint Histone modifications',
      long_label          => 'Blueprint Histone modifications',
      num_visible_regions => 5,
      num_visible_signals => 0,
    },
    'RNA-SEQ' => {
      track_name          => 'bpRNA',
      short_label         => 'Blueprint RNA-Seq',
      long_label          => 'Blueprint RNA-Seq',
      num_visible_regions => 0,
      num_visible_signals => 2,
    },
    BLUEPRINT => {
      track_name          => 'bp',
      short_label         => 'Blueprint',
      long_label          => 'Blueprint',
      num_visible_regions => 5,
      num_visible_signals => 5,
    },
    REGION => {
      track_name          => 'region',
      short_label         => 'Blueprint Region',
      long_label          => 'Blueprint Region',
      num_visible_regions => 5,
      num_visible_signals => 0,
    },
    SIGNAL => {
      track_name          => 'signal',
      short_label         => 'Blueprint Signal',
      long_label          => 'Blueprint Signal',
      num_visible_regions => 0,
      num_visible_signals => 5,
    },
  );
  return $cc{$ls} or die "No config found for $ls";
}

sub write_composite_block {
  my ( $file_entries, $cc, $tag_setup, $priority ) = @_;

  my $sub_group_block = sub_group_block( $file_entries, $tag_setup  );

  my ( $dimensions_line, $filter_composite_line ) =
    dimensions_and_filter_lines( $tag_setup );
  
  my $sort_order = join ' ',
    map { $_ . '=+' } @{ $tag_setup->{priority_tags} };

  print <<END_BLOCK;             
track $cc->{track_name}
compositeTrack on
shortLabel $cc->{short_label}
longLabel $cc->{long_label}
END_BLOCK

  print $sub_group_block;

  print <<END_BLOCK;
dimensions $dimensions_line
filterComposite $filter_composite_line
dragAndDrop subTracks
sortOrder $sort_order  
priority $priority
type bed 3
visibility full

END_BLOCK
}

sub sub_group_block {
  my ( $file_entries, $tag_setup ) = @_;

  my @tags = @{ $tag_setup->{sub_group_tags} };
  my %tag_values;

  for my $fe (@$file_entries) {
    for my $t (@tags) {
      my $v = $fe->{$t};
      $tag_values{$t}{$v} = 1 if $v;
    }
  }
  my $i = 1;
  my $sub_group_block;

  for my $t (@tags) {
    $sub_group_block .= 'subGroup';
    $sub_group_block .= ( $i++ );
    $sub_group_block .= ' ';
    $sub_group_block .= $t;
    $sub_group_block .= ' ';
    $sub_group_block .= ucfirst($t);

    my @vals = keys %{ $tag_values{$t} };

    for my $v (sort @vals) {
  
      my ( $name, $label ) = sub_group_naming($v);
      $sub_group_block .= " $name=$label";
    }

    $sub_group_block .= $/;
  }

  return $sub_group_block;
}

sub sub_group_naming {
  my ( $sub_group_value ) = @_;

  # spaces are not permitted, use underscores instead
  
  $sub_group_value =~ s{\s+}{_}g;

  my $name  = $sub_group_value;
  my $label = $sub_group_value;  
  
  $name = escape_css_meta_chars($name);

  return ( $name, $label );
}

sub escape_css_meta_chars {
  my ($input) = @_;
  
  # UCSC use sub group name and track name in jQuery/CSS selectors without escaping any troublesome metachars, 
  # so you must remove these from the name if you want the select grid to work
  
    my $css_meta_chars = '!"#$%&\'()*+,./:;<=>?@[\]^`{|}~';

    for my $cmc ( split( '', $css_meta_chars ) ) {
      $input =~ s/\Q$cmc\E/_/g;
    }
    
    return $input;
}
sub dimensions_and_filter_lines {
  my ($tag_setup) = @_;

  my $dimensions_line;
  my $filter_composite_line;
  my @dim_names = ( 'X', 'Y', 'A' .. 'W' );

  for my $dim ( @{ $tag_setup->{dim_tags} } ) {
    my $dn = shift @dim_names;
    $dimensions_line .= "dim$dn=$dim ";
    $filter_composite_line .= "dim$dn " unless ( $dn eq 'X' || $dn eq 'Y' );
  }
  return ( $dimensions_line, $filter_composite_line );
}

sub write_sub_tracks {
  my ( $track_name, $view_type, $short_label,  $parent_track ) = @_;
  
  my %type_hash = ( REGION => { view => 'Region',
                                type => 'bigBed',
                                visibility => 'dense',
                              },
                    SIGNAL => { view => 'Signal',
                                type => 'bigWig',
                                autoscale => 'off',
                                maxHeightPixels => '64:32:16',
                                visibility => 'pack',
                              }, 
                 );
                 
  $view_type = uc( $view_type );
  
  my $print_lines;
  
  foreach (keys %{ $type_hash { $view_type } } ){
    $print_lines .= "  ". $_ . " " . $type_hash{ $view_type }{ $_ } . "\n" ;
  }
            
  print <<END_BLOCK;        ### improve formatting
  track $track_name
  parent $parent_track
  shortLabel $short_label
END_BLOCK
  
  print $print_lines ,"\n";
}

sub write_region_tracks {
  my (
       $tracks, $track_name,  $parent_track, 
       $show_limit, $tag_setup, $track_label_hash,
       $track_on_sample_barcode, $track_on_cell_type
     ) = @_;

  my $visibility = 'dense';
  my $track_on;
  my %exps;
  my $visible_tracks_count = 0;

  for (@$tracks) {
    #if ( !defined $exps{ $_->{experiment} } && $visible_tracks_count < $show_limit )
    #{
    #  $track_on = 1;
    #  $exps{ $_->{experiment} } = 1;
    #  $visible_tracks_count++;
    #}
    my $donor_id = $_->{DONOR_ID};
    my $cell_type = $_->{CELL_TYPE};
    $cell_type =~ s/\s/_/g;
    $track_on_cell_type =~ s/\s/_/g;

    if ( $donor_id eq $track_on_sample_barcode && $cell_type eq $track_on_cell_type ) {
      $track_on = 1;
    }
    else {
      $track_on = 0;
    }
    one_peak_track( $_, $parent_track, $track_name, $visibility, $track_on, $tag_setup, $track_label_hash );

  }
}

sub one_peak_track {
  my ( $data, $parent, $parent_track_name, $visbility, $on, $tag_setup, $track_label_hash ) = @_;

  my $url            = $data->{url};
  my $sample_id      = $data->{sample_id};
  my $date           = $data->{date};
  my $experiment     = $data->{experiment};
  my $analysis_type  = $data->{analysis_type};
  my $lab            = $data->{lab};
  my $seq_type       = $data->{seq_type};
  my $path           = $data->{path};
  my $analysis_group = $data->{analysis_group};
  my $experiment_id  = $data->{EXPERIMENT_ID};
 

  my $colour;
  my $track_type;
  
  $tag_setup = metadata_tag( $tag_setup, $data->{BIOMATERIAL_TYPE}  ); ## add metadata tag
  
  if ( $data->{LIBRARY_STRATEGY} =~ /Bisulfite-Seq/ && $data->{analysis_type} ) {
    $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{analysis_type} );
    $track_type = 'region';
  }
  elsif ( $data->{LIBRARY_STRATEGY} =~ /RNA-Seq/ ) {
     $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{analysis_type} );
     $track_type = 'peak';
  }
  else {
    $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{experiment} );
    $track_type = 'peak';
  }

  my ( $subgroup_info, $meta_data_info ) =
    subgroup_and_meta_info( $data, $tag_setup );

  my ( $short_label, $long_label ) = track_label( $data, $track_type, $track_label_hash );

  
  my $track_name;
  
  if ( $analysis_type ) {
   $track_name = "${parent_track_name}${experiment_id}${sample_id}${experiment}${analysis_type}${analysis_group}";
  }

  
  if ( $data->{FILE_TYPE} =~ /WIGGLER/) {
    $track_name .= "wiggler"
  }
  
  my $track = escape_css_meta_chars( $track_name );
  
  my $field_count;
  if ( $url =~ m/broad/ ) {
    $field_count = "6 ."
      ; # could be 12,but it looks better this waay (solid block vs. blocks+whiskers)
  }
  else {
    $field_count = "6 .";
  }
  $on = ( $on ? 'on' : 'off' );
  print <<END_BLOCK;                ### improve formatting
    track $track
    bigDataUrl $url
    parent $parent $on
    type bigBed $field_count
    shortLabel $short_label
    longLabel $long_label
    color $colour
    subGroups $subgroup_info
    metadata $meta_data_info
    visibility $visbility

END_BLOCK
}

sub write_signal_tracks {
  my (
       $tracks,       $track_name, 
       $parent_track, $show_limit, $tag_setup, 
       $track_label_hash, $track_on_sample_barcode, 
       $track_on_cell_type
     ) = @_;

  my $visibility = 'pack';
  my $track_on;
  my %exps;
  my $visible_tracks_count = 0;

  for (@$tracks) {
    die "no experiment! " . Dumper($_) unless $_->{experiment};
    #if ( !defined $exps{ $_->{experiment} } && $visible_tracks_count < $show_limit )
    #{
    #  $track_on = 1;
    #  $exps{ $_->{experiment} } = 1;
    #  $visible_tracks_count++;
    #}
    my $donor_id = $_->{DONOR_ID};
    my $cell_type = $_->{CELL_TYPE};
    $cell_type =~ s/\s/_/g;
    $track_on_cell_type =~ s/\s/_/g;

    if ( $donor_id eq $track_on_sample_barcode && $cell_type eq $track_on_cell_type ) {
      $track_on = 1;
    }
    else {
      $track_on = 0;
    }
    one_signal_track( $_, $parent_track, $track_name, $visibility, $track_on, $tag_setup, $track_label_hash );

  }
}

sub one_signal_track {
  my ( $data, $parent, $parent_track_name, $visibility, $on, $tag_setup, $track_label_hash ) = @_;

  my $url            = $data->{url};
  my $path           = $data->{path};
  my $sample_id      = $data->{sample_id};
  my $date           = $data->{date};
  my $experiment     = $data->{experiment};
  my $analysis_type  = $data->{analysis_type};
  my $lab            = $data->{lab};
  my $seq_type       = $data->{seq_type};
  my $analysis_group = $data->{analysis_group}; 
  my $experiment_id  = $data->{EXPERIMENT_ID};
  
  $tag_setup = metadata_tag( $tag_setup , $data->{BIOMATERIAL_TYPE} ); ## add metadata tag

  my ( $subgroup_info, $meta_data_info ) =
    subgroup_and_meta_info( $data, $tag_setup );

  my $colour ;
  if ( $data->{LIBRARY_STRATEGY} =~ /Bisulfite-Seq/ && $data->{analysis_type} ) {
    $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{analysis_type} );
  }
  elsif ( $data->{LIBRARY_STRATEGY} =~ /RNA-Seq/ ) {
    $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{analysis_type} );
  }
  else {
    $colour = colour( $data->{LIBRARY_STRATEGY}, $data->{experiment} );
    
  }
  
  die "no colour for " . Dumper($data) unless ($colour);

  my ( $short_label, $long_label ) = track_label( $data, 'signal', $track_label_hash );


  my $track_name;
  
  if ( $analysis_type ) {
   $track_name = "${parent_track_name}${experiment_id}${sample_id}${experiment}${analysis_type}${analysis_group}";
  }

  
  if ( $data->{FILE_TYPE} =~ /WIGGLER/) {
    $track_name .= "wiggler"
  }
  
  my $track = escape_css_meta_chars( $track_name );
  
  my ( $min, $max ) = signal_min_max( $data );
  $on = ( $on ? 'on' : 'off' );

  print <<END_BLOCK;
    track $track
    bigDataUrl $url
    parent $parent $on
    type bigWig $min $max
    shortLabel $short_label
    longLabel $long_label
    color $colour
    subGroups $subgroup_info
    metadata $meta_data_info
    visibility $visibility

END_BLOCK
}

sub metadata_tag {
  my ( $tag_setup, $bm_type  ) = @_;
  
  my $metadata_hash = trachub_metadata();
  
  $bm_type = uc ( $bm_type );
  $bm_type =~ s{\s+}{_}g;
  
      
  die "Unknown Biomaterial type: $bm_type \n" unless ( exists ( $$metadata_hash { $bm_type } ) );
  
  my $bm_type_meta = ${ $metadata_hash } { $bm_type };
   
  ${$tag_setup} {  $bm_type } { 'meta_data_tags' } = $bm_type_meta ;
  
  return $tag_setup;
}

sub trachub_metadata {

### IHEC trackhub metadata requirement  ###

  my @common_tags = qw/ MOLECULE DISEASE
                        BIOMATERIAL_TYPE
                        REFERENCE_REGISTRY_ID  
                        SAMPLE_ONTOLOGY_URI
                        DISEASE_ONTOLOGY_URI
                        SAMPLE_ID
                        EXPERIMENT_TYPE
                        LIBRARY_STRAREGY
                        EXPERIMENT_ID
                        ALIGNMENT_SOFTWARE 
                        ALIGNMENT_SOFTWARE_VERSION 
                        ANALYSIS_SOFTWARE 
                        ANALYSIS_SOFTWARE_VERSION
                        EPIRR_ID 
                   /;

  my @cell_fields = qw/  DONOR_ID
                         DONOR_AGE
                         DONOR_HEALTH_STATUS
                         DONOR_SEX
                         DONOR_ETHNICITY
                   /;

  my @primary_tissue_fields = ( @common_tags, @cell_fields,  qw/ TISSUE_TYPE
                                                                 TISSUE_DEPOT
                                                               / ) ;

  my @cell_line_fields = ( @common_tags, qw/ LINE
                                             LINEAGE
                                             DIFFERENTIATION_STAGE
                                             MEDIUM
                                             SEX
                                          / );

  my @primary_cell_fields = ( @common_tags, @cell_fields, qw/ CELL_TYPE / );

  my @primary_cell_culture_fields = ( @common_tags, @cell_fields, qw/  CELL_TYPE 
                                                                       CULTURE_CONDITIONS
                                                                    / );

  my %metadata_hash = ( PRIMARY_CELL          => \@primary_cell_fields ,
                        PRIMARY_TISSUE        => \@primary_tissue_fields ,
                        CELL_LINE             => \@cell_line_fields ,
                        PRIMARY_CELL_CULTURE  => \@primary_cell_culture_fields,
                      );
                          
  return \%metadata_hash;
}

sub subgroup_and_meta_info {
  my ( $file_entry, $tag_setup ) = @_;
  
  my $bm_type = $file_entry->{BIOMATERIAL_TYPE};
  $bm_type = uc ( $bm_type );
  $bm_type =~ s{\s+}{_}g;
  
  my @subgroup_info;
  my $meta_data_info;

  for my $sg ( @{ $tag_setup->{sub_group_tags} } ) {
    my $value = $file_entry->{$sg};  

    my ( $name, $label ) = sub_group_naming($value);
    push @subgroup_info, "$sg=$name";               ### analysis type name  
  }
  my $subgroup_line = join ' ', @subgroup_info;
      
 foreach ( @{ $tag_setup->{$bm_type}{meta_data_tags} } ) {
   if ( exists ( $file_entry->{$_} ) ) {
     my $v = $file_entry->{$_};
     unless ( $v eq "-" || $v =~ /^NA$/i || $v =~ /^None$/i ) {
       $v =~ s/ /_/g;

       $meta_data_info .= $_ . "=" . $v . " ";
     }   
   }
 }
   
 return ( $subgroup_line, $meta_data_info );
}

sub colour {
  my ( $ls, $experiment ) = @_;
  $ls = uc( $ls );
  
  my %colour = (
    'DNASE-HYPERSENSITIVITY'  => { 'DNase' => '8,104,172' },
    'BISULFITE-SEQ'           => {
      'CPG_methylation_cov'    => '251,143,32',
      'CPG_methylation_calls' => '251,143,32',
      'hypo_methylation'      => '252,180,100',
      'hyper_methylation'     => '250,108,0',
    },
    'CHIP-SEQ' => {
      'H3K27ac'   => '255,94,106',
      'H3K27me3'  => '128,128,128',
      'H3K36me3'  => '0,177,92',
      'H3K4me1'   => '255,188,58',
      'H3K4me3'   => '255,0,11',
      'H3K9me3'   => '128,128,128',
      'Input'     => '0,0,0',
      'H2A.Zac'   => '255,0,11',
      'H3K9/14ac' => '255,0,11',

    },
    'RNA-SEQ' => {
      'Signal'              => '0,177,92',
      'RNAPlus'             => '0,177,92',
      'RNAPlusMulti'        => '0,177,92',
      'RNAMinus'            => '0,177,92',
      'RNAMinusMulti'       => '0,177,92',
      'RNAUnstranded'       => '0,177,92',
      'RNAUnstrandedMulti'  => '0,177,92',
      
    },
  );
  return $colour{$ls}{$experiment} or die("No colour for $ls $experiment ");
}

sub track_label {
  my ( $data, $type, $track_label_hash ) = @_;

  my ( @short, @long );

  # donor
  
  push @short, $data->{sample_description_2};
  push @long,  $data->{sample_description_2};

  #mark
  my $analysis_type   = $data->{analysis_type};
  my $experiment      = $data->{experiment};
  
 
  
  
  push @long,  $data->{experiment};
  push @long,  $analysis_type;
  
  
  #ChIP seq hack for short labels
  $experiment =~ s/^H3K/K/;
  #BS-Seq hack
  $experiment =~ s/^BS-Seq/BS/;
  #RNA-Seq hack
  $experiment =~ s/^RNA-Seq/RNA/;

  push @short, $experiment; 
  push @short, $data->{short_analysis_type};
  
  my $cell_type = $data->{sample_description};
  
  my $shortname_key = $track_label_hash->{short_name}{shortname_key};    
  my $short_name_array = $track_label_hash->{short_name}{short_name_array};
  
  push @long, $cell_type;
  
  foreach my $term_hash ( @{ $short_name_array } ) {  
    foreach my $term ( keys %{ $term_hash } ) {    
      if ( $term_hash->{ $term } && $term ne $shortname_key ) {      
        my $q_cell_type = quotemeta( $cell_type );
                
        if ( $term_hash->{ $term } =~ m/$q_cell_type/i) {        
          $cell_type = $term_hash->{ $shortname_key };        
        }  
      }
    }
  }
  
  push @short, $cell_type;

  #lab
  my $lab = $data->{lab};
  push @long, "$type from $lab";

  my $short_label = join( '.', @short );
  my $long_label  = join( ' ', @long );

  if ( length($short_label) > 17 ) {
    my $first = substr( $short_label, 0, 17 );
    my $second = substr( $short_label, 17 );
    print STDERR "Short label is too long: $first<limit remainder>$second$/";
  }
  return ( $short_label, $long_label );
}

sub track_analysis_label {
 my ( $sample_description_shortname, $shortname_key ) = @_;
 my $short_name_array = read_file( $sample_description_shortname );
 
 my %track_label_hash = (   short_name    => { sample_description_shortname  =>  $sample_description_shortname,
                                               shortname_key                 =>  $shortname_key,
                                               short_name_array              =>  $short_name_array,
                                             },                
                        );
                          
  return \%track_label_hash;                          
}

sub signal_min_max {
  my ($file_entry) = @_;
  my ( $min, $max );

  my $experiment     = $file_entry->{experiment};
  my $analysis_type  = $file_entry->{analysis_type};
  my $seq_type       = $file_entry->{LIBRARY_STRATEGY};
  
  $seq_type =~ s{\s+}{_}g;
  $seq_type = uc( $seq_type );

  if ($experiment =~ /BS-Seq/i) {
    if ( $analysis_type =~ /CPG/ && $analysis_type =~ /call/i ) {
      ( $min, $max ) = ( 0, 1 );
    }
    elsif ( $analysis_type =~ /CPG/ && $analysis_type =~ /cov/i ) {
    ( $min, $max ) = ( 0, 0.1 );
    }
  }  
  elsif ( $experiment =~ /H3K4me3/i
       || $experiment =~ /H3K27ac/i
       || $experiment =~ /H2a.Zac/i
       || $experiment =~ /H3K9.14ac/i )
  {
    ( $min, $max ) = ( 0, 100 );
  }
  elsif ( $experiment =~ /H3K/ 
       || $experiment =~ /Input/ ) {
    ( $min, $max ) = ( 0, 50 );
  }
  elsif ( $seq_type eq 'RNA-SEQ' ) {
    ( $min, $max ) = ( 0, 100 );
  }
  elsif ( $seq_type eq 'DNASE-HYPERSENSITIVITY' ) {
    ( $min, $max ) = ( 0, 100 );
  }
  else {
    die
"Could not infer correct signal range for $experiment & $seq_type, using file values for $file_entry->{FILE}$/";
    my $path    = $file_entry->{file_path};
    my $wigInfo = `bigWigInfo $path`;
    for my $l ( split $/, $wigInfo ) {
      chomp $l;
      my ( $k, $v ) = split ": ", $l;
      $min = $v if ( $k eq 'min' );
      $max = $v if ( $k eq 'max' );
    }
  }
  return ( $min, $max );
}
