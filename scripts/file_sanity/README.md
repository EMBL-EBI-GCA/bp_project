# File sanity
This is a repository of code to check the integrity of the Blueprint files that are part of the release

We do not expect this code to be generally useful but if you have any questions about the code you find here or our processes managing the Blueprint data please email [blueprint-info@ebi.ac.uk](mailto:blueprint-info@ebi.ac.uk)

## analysis_BED_files.pl
Script to check sanity of .bed, .bb and MAC'S xls files. Currently, these are the file types that are by default:
- CHIP_MACS2_BROAD_BED
- CHIP_MACS2_BED
- CHIP_MACS2_BB
- CHIP_MACS2_BROAD_BB
- DNASE_HOTSPOT_BB
- DNASE_HOTSPOT_BED
- DNASE_HOTSPOT_PEAK_BED

Edit @valid_files array to control the file types to be analyzed

### Output:
This script will produce the following information:
```
/path/to/file/fileA.bed.gz	CHIP_MACS2_BROAD_BB	OK
/path/to/file/fileA.xls.gz	CHIP_MACS2_BROAD_BB	OK
/path/to/file/fileA.bb	CHIP_MACS2_BROAD_BB	OK
```

Where the 3rd column will be OK if number of peaks per chr in the comparison among the different files (fileX.bed.gz;fileX.xls.gz;fileX.bb) for a specific sample-experiment combination is the same.
3rd column will say EMPTY if no peaks at all were identified for a specifig sample-experiment combination

This script will also generate a file named report_chros.bed.txt, containing a report of the frequency of files having peaks in each chromosome for each particular file type.
For example, one example of a report_chros.bed.txt showing a possible issue that should be checked would be:
```
#type	chr	number
CHIP_MACS2_BROAD_BB	chr14	8
CHIP_MACS2_BROAD_BB	chrY	8
CHIP_MACS2_BROAD_BB	chr1	1
CHIP_MACS2_BROAD_BB	chr21	8
CHIP_MACS2_BROAD_BB	chr3	8
CHIP_MACS2_BROAD_BB	chr18	8
CHIP_MACS2_BROAD_BB	chr15	8
CHIP_MACS2_BROAD_BB	chr2	8
CHIP_MACS2_BROAD_BB	chr10	8
CHIP_MACS2_BROAD_BB	chr7	8
CHIP_MACS2_BROAD_BB	chr20	8
CHIP_MACS2_BROAD_BB	chr6	8
CHIP_MACS2_BROAD_BB	chr11	8
CHIP_MACS2_BROAD_BB	chrX	8
CHIP_MACS2_BROAD_BB	chr4	8
CHIP_MACS2_BROAD_BB	chr9	8
CHIP_MACS2_BROAD_BB	chr5	8
CHIP_MACS2_BROAD_BB	chr22	8
CHIP_MACS2_BROAD_BB	chr8	8
CHIP_MACS2_BROAD_BB	chr17	8
CHIP_MACS2_BROAD_BB	chr16	8
CHIP_MACS2_BROAD_BB	chr12	8
CHIP_MACS2_BROAD_BB	chr13	8
CHIP_MACS2_BROAD_BB	chr19	8
...
```
Where we can see that for each canonical chromosome, there are 8 files of type CHIP_MACS2_BROAD_BB that have peaks. This is not the case for chr1, where only one file has peaks in this chromosome

## analysis_BW_files.pl
Script to check sanity of .bw files. Currently, these are the file types that are verified:
- CHIP_WIGGLER
- DNASE_WIGGLER

Edit @valid_files array to control the file types to be analyzed

Edit  @check_chros array to control the chromosomes to be analyzed

### Output:
This script will produce the following information:
```
/path/to/file/fileA.bw  171     OK      CHIP_WIGGLER
/path/to/file/fileB.bw  174     OK      CHIP_WIGGLER
/path/to/file/fileC.bw  171     OK      CHIP_WIGGLER
/path/to/file/fileD.bw  171     OK      CHIP_WIGGLER
...
```
Where the 2nd column is the total number of chros having peaks in a particular file
The 3rd column will be OK if all the chromosomes that are in @check_chros_array are present in the file. If not,the chromosomes within the @check_chros array that are not present will be printed

This script will also generate a file named report_chros.bw.txt, containing a report on the number of files missing peaks in the chromosomes specified in the @check_chros array
