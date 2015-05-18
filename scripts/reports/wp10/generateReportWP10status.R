#final categories that are considered are:
#A=Primary analysis available
#B=BAMs available
#R=reads received
library(reshape)
library(xlsx)
library(dplyr)

args<-commandArgs(TRUE)

matfile<-c(args[1]) #path to wp10 .tsv file
outdir<-c(args[2]) #output dir

#generate output filename
outfile=paste(outdir,"/","bp_wp10_status.xlsx",sep="")

## Function definitions

#++++++++++++++++++++++++
# Function simplifies the complex labels, i.e.: 
# B,B,R->B
# A,A,B->A
# A(P)->A
# and so on...
#++++++++++++++++++++++++
reduce<-function(str) {
  if (!is.na(str)) {
    l=strsplit(str,",")
    v<-l[[1]]
    v<-unique(v)
    v<-paste(v,collapse="")
    #remove label for Publicly released
    v<-sub("(\\w)\\(P\\)","\\1",v)
    f<-c()
    if (v=="AB" || v=="BA") {
      f<-"A"
    } else if (v=="BR" || v=="RB") {
      f<-"B"
    } else {
      f<-v
    }
  } else {
    str
  }
}

#++++++++++++++++++++++++
# Helper function to add titles
#++++++++++++++++++++++++
# - sheet : sheet object to contain the title
# - rowIndex : numeric value indicating the row to 
#contain the title
# - title : the text to use as title
# - titleStyle : style object to use for title
xlsx.addTitle<-function(sheet, rowIndex, title, titleStyle){
  rows <-createRow(sheet,rowIndex=rowIndex)
  sheetTitle <-createCell(rows, colIndex=1)
  setCellValue(sheetTitle[[1,1]], title)
  setCellStyle(sheetTitle[[1,1]], titleStyle)
}

# create a new workbook for outputs
# possible values for type are : "xls" and "xlsx"
wb<-createWorkbook(type="xlsx")

# Title and sub title styles
TITLE_STYLE <- CellStyle(wb)+ Font(wb,  heightInPoints=16, 
                                   color="blue", isBold=TRUE, underline=1)
SUB_TITLE_STYLE <- CellStyle(wb) + 
  Font(wb,  heightInPoints=14, 
       isItalic=TRUE, isBold=FALSE)

# Styles for the data table row/column names
TABLE_ROWNAMES_STYLE <- CellStyle(wb) + Font(wb, isBold=TRUE)
TABLE_COLNAMES_STYLE <- CellStyle(wb) + Font(wb, isBold=TRUE) +
  Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
  Border(color="black", position=c("TOP", "BOTTOM"), 
         pen=c("BORDER_THIN", "BORDER_THICK")) 

#read tsv file
DF<-read.table(matfile,header=TRUE,sep = "\t",na.strings=c(""),stringsAsFactors = FALSE)

#drop columns that are not necessary according to Nicole's email
DF<-select(DF,-Bisulfite.Seq,-H3K36me3,-H3K9me3,-H2A.Zac,-H3K9.14ac)

#simplify Cell.type
DF[,5]<-gsub("CD14-positive, CD16-negative classical monocyte","monocyte",DF[,5])
DF[,5]<-gsub("CD4-positive, alpha-beta T cell","T_cell",DF[,5])

#Clean_up column names
names(DF)[9:length(colnames(DF))]<-gsub("\\.\\.","",names(DF[,9:length(colnames(DF))]))
names(DF)[9:length(colnames(DF))]<-gsub("\\.$","",names(DF[,9:length(colnames(DF))]))
names(DF)[9:length(colnames(DF))]<-gsub("\\.","_",names(DF[,9:length(colnames(DF))]))

#replace null Cell.type by 'whole_blood'
DF$Cell.type[is.na(DF$Cell.type)] <- "whole_blood"

#inititalize data.frame for summary sheet
summary.df<-data.frame(matrix(NA, nrow=4,ncol=length(colnames(DF)[9:length(colnames(DF))])+1))
names(summary.df)<-c("Cell.Type",colnames(DF)[9:length(colnames(DF))])

#iterate over each of the experiments and calculate frequencies
for (n in 9:length(colnames(DF))) 
{
  #reduce the complexity of labels of 'n' treatment
  elmsv<-unlist(elms<-sapply(DF[,n],reduce))
  #replace old column with new column with simplified labels and as a factor
  elmsv.asfactor<-factor(elmsv,levels<-c("A","B","R"))
  DF[,n]<-elmsv.asfactor
  #melt data
  mdata <- melt(DF, id=c("Cell.type"),measure.vars=c(n))
  #calculate frequencies with 'table' function
  freqs <- cast(mdata, Cell.type~variable, table)
  #add totals per Cell.type
  freqs$total.CellType<-as.numeric(table(DF$Cell.type))
  #add assay_type column
  freqs$Assay.type<-rep(colnames(DF)[n],nrow(freqs))
  #reorder columns
  freqs<-freqs[c(1,6,5,4,3,2)]
  #generate summary sheet
  summary.df$Cell.Type<-c(freqs$Cell.type)
  summary.df[colnames(DF)[n]]<- apply(freqs[4:6],1,sum)
  #change columnnames
  colnames(freqs)[1] <- "Cell Type"
  colnames(freqs)[2] <- "Assay Type"
  colnames(freqs)[3] <- "Total"
  colnames(freqs)[4] <- "Reads"
  colnames(freqs)[5] <- "Alignments"
  colnames(freqs)[6] <- "Analysis"
 
  # Create a new sheet in the workbook
  sheet <- createSheet(wb, sheetName = colnames(DF)[n])
  
  # Add title
  xlsx.addTitle(sheet, rowIndex=1, title="Numeric summary on the status of WP10",
                titleStyle = TITLE_STYLE)
  # Add sub titles
  xlsx.addTitle(sheet, rowIndex=2, 
                title="Total=column refers to the total number of donors for which a certain Cell Type is available",
                titleStyle = SUB_TITLE_STYLE)
  xlsx.addTitle(sheet, rowIndex=3, 
                title="Reads=Number of donors for which a certain Cell Type has FASTQ files available",
                titleStyle = SUB_TITLE_STYLE)
  xlsx.addTitle(sheet, rowIndex=4, 
                title="Alignments=Number of donors for which a certain Cell Type has BAM files available",
                titleStyle = SUB_TITLE_STYLE)
  xlsx.addTitle(sheet, rowIndex=12, 
                title="Please e-mail blueprint-dcc@ebi.ac.uk if you identify any issues with this report.",
                titleStyle = SUB_TITLE_STYLE)
  # Add a data.frame
  addDataFrame(freqs, sheet, startRow=5, startColumn=1, 
               colnamesStyle = TABLE_COLNAMES_STYLE,
               rownamesStyle = TABLE_ROWNAMES_STYLE,row.names=FALSE)
  # Change column width
  setColumnWidth(sheet, colIndex=c(1:ncol(freqs)), colWidth=11)
}

# Add summary.df in a new sheet
sheet <- createSheet(wb, sheetName = "Summary")
addDataFrame(summary.df, sheet, startRow=1, startColumn=1, 
             colnamesStyle = TABLE_COLNAMES_STYLE,
             rownamesStyle = TABLE_ROWNAMES_STYLE,row.names=FALSE)
# Change column width
setColumnWidth(sheet, colIndex=c(1:ncol(summary.df)), colWidth=20)

# Add raw data in a new sheet
sheet1 <- createSheet(wb, sheetName = "Raw")
addDataFrame(DF, sheet1, startRow=1, startColumn=1, 
             colnamesStyle = TABLE_COLNAMES_STYLE,
             rownamesStyle = TABLE_ROWNAMES_STYLE,row.names=FALSE)
# Change column width
setColumnWidth(sheet1, colIndex=c(1:ncol(DF)), colWidth=20)

# save
saveWorkbook(wb,outfile)
