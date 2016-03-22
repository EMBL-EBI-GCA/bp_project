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
        'java_path'  => 'java',
        'ega_jar'     => 'webin-data-streamer-Upload-Client.jar',
        'file'            => undef,
    };
}


sub pipeline_create_commands {
    my ($self) = @_;
    return [
        @{$self->SUPER::pipeline_create_commands},  # inheriting database and hive tables' creation

       # 'mkdir -p '.$self->o('work_dir'),
    ];
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
                'cmd'       => '#java_path# -jar #ega_jar# -file #filename#',
            },
            -flow_into => {
            1 => [ 'find_encrypted_files' ],
            },
        },
        
        {    -logic_name => 'find_encrypted_files',
             -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
             -parameters => {  
                     'inputcmd'      => "find #filename#.*",
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
                1  =>  [ 'upload_file' ],  
                },   
        },
         {   -logic_name => 'upload_file',
             -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
             -parameters    => {
                'cmd'       => 'ls #gpg_filename#',
            },
            -flow_into => {
                1  =>  [ 'clean_file' ],  
                },   
         },
         {   -logic_name => 'clean_file',
             -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
             -parameters    => {
                'cmd'       => 'rm -f #gpg_filename#',
            },
        },
    ];
}

1;

