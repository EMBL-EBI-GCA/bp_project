package ReseqTrack::Hive::PipeConfig::ChipPeakCall_conf;

use strict;
use warnings;

use base ('ReseqTrack::Hive::PipeConfig::ReseqTrackGeneric_conf');

sub default_options {
    my ($self) = @_;

    return {
        %{ $self->SUPER::default_options() },

        'pipeline_name' => 'peak',
        seeding_module => 'ReseqTrack::Hive::PipeSeed::ChipPeakCallSeed',
        seeding_options => {
            output_columns           => ['name', 'collection_id'],
            require_columns          => $self->o('require_collection_columns'),
            exclude_columns          => $self->o('exclude_collection_columns'),
            require_attributes       => $self->o('require_collection_attributes'),
            exclude_attributes       => $self->o('exclude_collection_attributes'),
            require_experiment_type  => $self->o('require_experiment_type'),
            non_match_input          => $self->o('non_match_input'),
            bam_collection_type      => $self->o('bam_collection_type'),
            input_prefix             => $self->o('pipeline_input_file_prefix'),
            exp_type_attribute_name  => $self->o('exp_type_attribute_name'),
            metadata_file            => $self->o('metadata_file'),
            path_names_array         => $self->o('path_names_array'),
        },

        metadata_file                 => $self->o('metadata_file'),
        path_names_array              => [ 'sample_desc_1', 'sample_desc_2', 'sample_desc_3', 'library_strategy', 'center_name' ],
        require_collection_columns    => {'type' => $self->o('bam_collection_type')}, 
        exclude_collection_columns    => {},
        require_collection_attributes => {},
        exclude_collection_attributes => {},
        require_experiment_type       => 'ChIP-Seq Input',
        non_match_input               => undef,
        bam_collection_type           => 'CHIP_DEDUP_BAM',
        pipeline_input_file_prefix    => 'input_file',
        exp_type_attribute_name       => 'EXPERIMENT_TYPE',

        macs2_exe       => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/macs2',
        samtools        => '/nfs/1000g-work/G1K/work/bin/samtools/samtools',
        bedtools        => '/nfs/1000g-work/G1K/work/bin/bedtools2-2.20.1/bin/bedtools',
        bedToBigBedPath => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/bedToBigBed',
 
        chr_file        => $self->o('chr_file'),

        dir_label_params_list => ["sample_alias", "experiment_source_id"],  

        bed_type            => 'CHIP_MACS2_BED',
        bb_type             => 'CHIP_MACS2_BB',
        broad_bed_type      => 'CHIP_MACS2_BROAD_BED',
        broad_bb_type       => 'CHIP_MACS2_BROAD_BB',

        'collection_name'   => '#experiment_source_id#',
        'build_collection'  => 1,

        chip_bed_in_columns => { seq    => 1,
                                 start  => 2,
                                 end    => 3,
                                 name   => undef,
                                 score  => 5,
                                 strand => undef,
                               },

        'root_output_dir'    => $self->o('root_output_dir'),
        'final_output_dir'   => $self->o('final_output_dir'),
        'final_aln_dir'      => $self->o('final_aln_dir'),
        final_output_layout     => '#sample_desc_1#/#sample_desc_2#/#sample_desc_3#/#library_strategy#/#center_name#',


        name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
        name_file_method => 'basic',
        name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38',
            add_datestamp => 1,
            suffix        => '.bed.gz',
        },
        name_broad_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38.broad',
            add_datestamp => 1,
            suffix        => '.bed.gz',
        },
        support_bed_name_file_params => {
            new_dir       => '#final_aln_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38.summits',
            add_datestamp => 1,
            suffix        => '.bed.gz',
        },
        support_broad_bed_name_file_params => {
            new_dir       => '#final_aln_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38.broad',
            add_datestamp => 1,
            suffix        => '.gappedPeak.gz',
        },
        xls_name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38',
            add_datestamp => 1,
            suffix        => '.xls.gz',
        },
        xls_name_broad_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38.broad',
            add_datestamp => 1,
            suffix        => '.xls.gz',
        },
    };
}

sub pipeline_create_commands {
    my ($self) = @_;

    return [
        @{$self->SUPER::pipeline_create_commands}, 
    ];
}

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters},


    };
}

sub resource_classes {
    my ($self) = @_;
    return {
            %{$self->SUPER::resource_classes},
            '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
            '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
            '1Gb'   => { 'LSF' => '-C0 -M1000 -q '.$self->o('lsf_queue').' -R"select[mem>1000] rusage[mem=1000]"' },
            '2Gb'   => { 'LSF' => '-C0 -M2000 -q '.$self->o('lsf_queue').' -R"select[mem>2000] rusage[mem=2000]"' },
            '4Gb'   => { 'LSF' => '-C0 -M4000 -q '.$self->o('lsf_queue').' -R"select[mem>4000] rusage[mem=4000]"' },
            '5Gb'   => { 'LSF' => '-C0 -M5000 -q '.$self->o('lsf_queue').' -R"select[mem>5000] rusage[mem=5000]"' },
            '8Gb'   => { 'LSF' => '-C0 -M8000 -q '.$self->o('lsf_queue').' -R"select[mem>8000] rusage[mem=8000]"' },
            '12Gb'  => { 'LSF' => '-C0 -M12000 -q '.$self->o('lsf_queue').' -R"select[mem>12000] rusage[mem=12000]"' },
    };
}

sub pipeline_analyses {
    my ($self) = @_;

    my @analyses;
    push(@analyses, {
            -logic_name    => 'get_seeds',
            -module        => 'ReseqTrack::Hive::Process::SeedFactory',
            -meadow_type => 'LOCAL',
            -parameters    => {
                seeding_module  => $self->o('seeding_module'),
                seeding_options => $self->o('seeding_options'),
            },
            -flow_into => {
                2 => [ 'block_seed_complete' ],
            },
      });
   push(@analyses, {
          -logic_name => 'block_seed_complete',
          -module        => 'ReseqTrack::Hive::Process::BaseProcess',
          -meadow_type=> 'LOCAL',
          -parameters => {
            reseqtrack_options => {
              flows_non_factory => [1,2],
            },
          },
            -flow_into => {
                '2->A' => { 'find_source_bams' => {'callgroup' => '#name#', 'bam_collection_id' => '#collection_id#'}},
                'A->1' => [ 'mark_seed_complete' ],
            },
   });
   push(@analyses, {
            -logic_name    => 'find_source_bams',
            -module        => 'ReseqTrack::Hive::Process::ImportCollection',
            -meadow_type => 'LOCAL',
            -parameters    => {
                collection_id=> '#bam_collection_id#',
                output_param => 'bam',
                reseqtrack_options => {
                  flows_file_count_param => 'bam',
                  flows_file_count => { 1 => '1+', },
                },
            },
            -flow_into => {
                1 => [ 'macs2_peak_call' ],
            },
      });
   push(@analyses, {
            -logic_name    => 'macs2_peak_call',
            -module        => 'ReseqTrack::Hive::Process::RunMacs2',
            -parameters    => {
                program_file => $self->o('macs2_exe'),
                samtools_path => $self->o('samtools'),
                control_files => '#input_file#',
                fragment_size => '#estFraglen#',
                broad         => '#broad#',
            },
            -rc_name => '2Gb',
            -analysis_capacity  =>  50,
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'macs2_stats' => { 'bed'         => '#bed#',
                                          'support_bed' => '#support_bed#',
                                          'bed_xls'     => '#bed_xls#',  
                                        },
                     },
            },
      });
    push(@analyses, {
            -logic_name    => 'macs2_stats',
            -module        => 'ReseqTrack::Hive::Process::Macs2Attribute',
            -parameters    => {
               samtools => $self->o('samtools'),
               bedtools => $self->o('bedtools'),
            },
           -rc_name => '2Gb',
           -hive_capacity  =>  200,
           -flow_into => {
                1 => {  'store_bed_file' => { 'attribute_metrics' => '#attribute_metrics#'}},
                },
   }); 
   push(@analyses, {
            -logic_name    => 'store_bed_file',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              bed_type               => $self->o('bed_type'),
              broad_bed_type         => $self->o('broad_bed_type'),
              bed_name_file_params   => $self->o('name_file_params'),
              broad_name_file_params => $self->o('name_broad_file_params'),
              type                   => '#expr( #broad#==1 ? #broad_bed_type# : #bed_type# )expr#',
              file                   => '#bed#',
              name_file_module       => $self->o('name_file_module'),
              name_file_method       => $self->o('name_file_method'),
              name_file_params       => '#expr( #broad#==1 ? #broad_name_file_params# : #bed_name_file_params# )expr#',
              final_output_dir       => $self->o('final_output_dir'),
              final_output_layout    => $self->o('final_output_layout'),
              collection_name        => $self->o('collection_name'),
              collect                => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => {  'bed_attributes'=> {'bed_type' => '#type#', 'bed' => '#file#' }},
            },
   });
   push(@analyses, {
            -logic_name => 'bed_attributes',
            -module        => 'ReseqTrack::Hive::Process::UpdateAttribute',
            -parameters => {
                attribute_metrics =>  ['#attribute_metrics#'],
                collection_type   => '#bed_type#',
                collection_name   => $self->o('collection_name'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => [ 'store_support_bed' ],
           },
   });
   push(@analyses, {
            -logic_name    => 'store_support_bed',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              support_bed_name_file_params       => $self->o('support_bed_name_file_params'),
              support_broad_bed_name_file_params => $self->o('support_broad_bed_name_file_params'),
              type                => '#bed_type#',
              file                => '#support_bed#',
              name_file_module    => $self->o('name_file_module'),
              name_file_method    => $self->o('name_file_method'),
              name_file_params    => '#expr( #broad#==1 ? #support_broad_bed_name_file_params# : #support_bed_name_file_params# )expr#',
              final_aln_dir       => $self->o('final_aln_dir'),
              final_output_layout => $self->o('final_output_layout'),
              collection_name     => $self->o('collection_name'),
              collect             => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_bed_xls' => { 'bed_xls' => '#bed_xls#' }},
           },
   });
   push(@analyses, {
            -logic_name    => 'store_bed_xls',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              xls_name_file_params       => $self->o('xls_name_file_params'),
              xls_name_broad_file_params => $self->o('xls_name_broad_file_params'),
              type                       => '#bed_type#',
              file                       => '#bed_xls#',
              name_file_module           => $self->o('name_file_module'),
              name_file_method           => $self->o('name_file_method'),
              name_file_params           => '#expr( #broad#==1 ? #xls_name_broad_file_params# : #xls_name_file_params# )expr#',
              final_output_dir           => $self->o('final_output_dir'),
              final_output_layout        => $self->o('final_output_layout'),
              collection_name            => $self->o('collection_name'),
              collect                    => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'bed_to_bigbed' => { 'bed' => '#bed#' }},
           },
   });
   push(@analyses, {
            -logic_name => 'bed_to_bigbed',
            -module        => 'ReseqTrack::Hive::Process::ConvertBedToBigBed',
            -parameters => {
                bed  => ['#bed#'],
                chr_file        => $self->o('chr_file'),
                bedToBigBedPath => $self->o('bedToBigBedPath'),
                in_columns      => $self->o('chip_bed_in_columns'),
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => {  'store_bigbed' => { 'bigbed' => '#bigbed#' } },
                },
   });  
   push(@analyses, {
            -logic_name    => 'store_bigbed',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              bb_type          => $self->o('bb_type'),
              broad_bb_type    => $self->o('broad_bb_type'),
              type             => '#expr( #broad#==1 ? #broad_bb_type# : #bb_type# )expr#',
              file             => '#bigbed#',
              collection_name  => $self->o('collection_name'),
              collect          => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
   });
   push(@analyses, {
            -logic_name    => 'mark_seed_complete',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_complete  => 1,
            },
            -meadow_type => 'LOCAL',
      });
      
      return \@analyses;
}

1;
