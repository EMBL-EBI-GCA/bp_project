=pod

=head1 NAME

    ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf

=head1 SYNOPSIS

    init_pipeline.pl ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf -inputfile file_list -work_dir dir_name -java_path java_path -ega_jar /path/webin-data-streamer-Upload-Client.jar 


=cut


package ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly


sub default_options {
    my ($self) = @_;
    return {
        %{ $self->SUPER::default_options() },               # inherit other stuff from the base class

        'pipeline_name' => 'ega_upload',                   # name used by the beekeeper to prefix job names on the farm

        # runnable-specific parameters' defaults:
        'java_path'   => 'java',
        'ega_jar'     => undef,
        'file'        => undef,
        'work_dir'    => undef,
        'upload_dest' => undef,
        'lsf_queue'   => 'production',
    };
}


sub pipeline_create_commands {
    my ($self) = @_;
    return [
        @{$self->SUPER::pipeline_create_commands},  # inheriting database and hive tables' creation

        'mkdir -p '.$self->o('work_dir'),
    ];
}

sub resource_classes {
  my ($self) = @_;
    return {
          %{$self->SUPER::resource_classes}, 
          '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
          '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
    };
}

sub hive_meta_table {
  my ($self) = @_;
  return {
    %{$self->SUPER::hive_meta_table},
    'hive_use_param_stack' => 1,
   };
}

sub pipeline_analyses {
    my ($self) = @_;
    return [
        {   -logic_name => 'find_files',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
            -parameters => {
                'inputcmd'     => 'cat #file#',
                'column_names' => [ 'filename' ],
            },
            -flow_into => {
                '2->A'  =>  [ 'encrypt_file' ],     
                'A->1'  =>  [ 'list_file' ], 
            },
        },

        {   -logic_name => 'encrypt_file',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters    => {
                'java_path' => $self->o('java_path'),
                'ega_jar'   => $self->o('ega_jar'),
                'cmd'       => '#java_path# -jar #ega_jar# -file #filename#',
            },
            -analysis_capacity => 10,
            -rc_name => '500Mb',
            -flow_into => {
            1 => [ 'find_encrypted_files' ],
            },
        },
        
        {    -logic_name => 'find_encrypted_files',
             -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
             -parameters => {  
                     'inputcmd'      => 'find #filename#.*',
                     'column_names' => [ 'gpg_file' ],
             },
               -flow_into => {
                   2 => [ ':////accu?gpg_file=[]' ],
               }
        },

        {   -logic_name => 'list_file',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
            -parameters    => {
                'inputlist'       => '#gpg_file#',
                'column_names' => ['gpg_filename'],
                'fan_branch_code' => 1,
            },
            -flow_into => {
                1  =>  { 'upload_file' => { 'gpg_filename' => '#gpg_filename#', 'gpg_suffix' => '#expr( (#gpg_filename# =~ /\S+\/(\S+)$/)[0] )expr#' }},  
                },   
        },
         {    -logic_name => 'upload_file',
              -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
              -parameters    => {
                  'work_dir' => $self->o('work_dir'),
                  'upload_dest' => $self->o('upload_dest'),
                  'cmd'       => 'mkdir -p #work_dir#/#gpg_suffix#;rm -f #work_dir#/#gpg_suffix#/aspera-scp-transfer.log;ascp -d -k2  -Tr -Q -l 100M -L #work_dir#/#gpg_suffix# #gpg_filename# #upload_dest#/#gpg_suffix#; cat #work_dir#/#gpg_suffix#/aspera-scp-transfer.log|perl -e \'while(<>){if(/LOG - Source file transfers passed\s+:\s+(\d)/){ die unless $ 1 > 0}}\'',
              },
              -rc_name => '500Mb',
              -flow_into => {
                 1  =>  { 'clean_file' => { 'gpg_filename' => '#gpg_filename#', 'gpg_suffix' => '#gpg_suffix#'} }, 
             },
         },
         {   -logic_name => 'clean_file',
             -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
             -parameters    => {
                'work_dir' => $self->o('work_dir'),
                'cmd'       => 'rm -f #gpg_filename#; rm -rf #work_dir#/#gpg_suffix#',
            },
        },
    ];
}

1;

