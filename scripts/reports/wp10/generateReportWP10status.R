#final categories that are considered are:
#A=Primary analysis available
#B=BAMs available
#R=reads received
library(reshape)
library(xlsx)

args<-commandArgs(TRUE)

matfile<-c(args[1]) #path to wp10 .tsv file


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

#simplify Cell.typeQ
DF[,5]<-gsub("CD14-positive, CD16-negative classical monocyte","monocyte",DF[,5])
DF[,5]<-gsub("CD4-positive, alpha-beta T cell","T_cell",DF[,5])

#replace null Cell.type by 'whole_blood'
DF$Cell.type[is.na(DF$Cell.type)] <- "whole_blood"

#iterate over each of the experiments and calculate frequencies
for (n in 9:22) 
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
  
  # Add a data.framce
  addDataFrame(freqs, sheet, startRow=5, startColumn=1, 
               colnamesStyle = TABLE_COLNAMES_STYLE,
               rownamesStyle = TABLE_ROWNAMES_STYLE,row.names=FALSE)
  # Change column width
  setColumnWidth(sheet, colIndex=c(1:ncol(freqs)), colWidth=11)
}

saveWorkbook(wb,"bp_wp10.xlsx")
