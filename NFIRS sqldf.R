library(foreign)
library(sqldf)
library(sp)
library(gsubfn)
library(proto)
library(RSQLite)
library(DBI)

filepath<-""
Basicincident <- read.dbf(filepath, as.is=FALSE)
filepath<-""
Fireincident<-read.dbf(filepath, as.is=FALSE)
filepath<-""
InciAdd<-read.dbf(filepath, as.is=FALSE)
filepath<-""
zipCode<-read.csv(filepath, header=TRUE)
filepath<-""
FFcasualty<-read.dbf(filepath, as.is=FALSE)
filepath<-""
Civcasualty<-read.dbf(filepath, as.is=FALSE)


#make the data string#
str(Basicincident)
str(Fireincident)
str(InciAdd)
str(zipCode)
str(FFcasualty)
str(Civcasualty)

#count the number of rows in each database#
nrow(Basicincident)
nrow(Fireincident)
nrow(InciAdd)
nrow(zipCode)
nrow(FFcasualty)
nrow(Civcasualty)

#get summary#
summary(Basicincident)
summary(Fireincident)
summary(InciAdd)
summary(zipCode)
summary(FFcasualty)
summary(Civcasualty)
#Call out packages#
library(sqldf)

#BASIC INCIDENT INNER JOIN FIRE INCIDENT INNER JOIN INCIDENT ADDRESS#
Data1<-sqldf("SELECT distinct Basicincident.FDID, Fireincident.SUP_FAC_1,Fireincident.SUP_FAC_2,Fireincident.SUP_FAC_3, Fireincident.EQUIP_INV,Basicincident.INC_TYPE,Basicincident.STATE, Basicincident.EXP_NO,Basicincident.ARRIVAL, Fireincident.CAUSE_IGN, Basicincident.INC_NO,Basicincident.INC_DATE,InciAdd.CITY, InciAdd.ZIP5, Basicincident.PROP_USE, Fireincident.DETECTOR, Fireincident.DET_TYPE, Basicincident.FF_DEATH,Basicincident.OTH_DEATH, Basicincident.FF_INJ,Basicincident.OTH_INJ, Fireincident.AREA_ORIG, Fireincident.HEAT_SOURC, Fireincident.FIRST_IGN
             FROM (Basicincident INNER JOIN Fireincident ON (Basicincident.EXP_NO = Fireincident.EXP_NO) AND (Basicincident.INC_NO = Fireincident.INC_NO) AND (Basicincident.INC_DATE = Fireincident.INC_DATE) AND (Basicincident.FDID = Fireincident.FDID) AND (Basicincident.STATE = Fireincident.STATE)) INNER JOIN InciAdd ON (Fireincident.EXP_NO = InciAdd.EXP_NO) AND (Fireincident.INC_NO = InciAdd.INC_NO) AND (Fireincident.INC_DATE = InciAdd.INC_DATE) AND (Fireincident.FDID = InciAdd.FDID) AND (Fireincident.STATE = InciAdd.STATE) 
             ")
nrow(Data1)

#BASIC INCIDENT INNER JOIN FIRE INCIDENT INNER JOIN INCIDENT ADDRESS#
Data1<-sqldf("SELECT distinct Basicincident.FDID, Fireincident.SUP_FAC_1,Fireincident.SUP_FAC_2,Fireincident.SUP_FAC_3, Fireincident.EQUIP_INV,Basicincident.INC_TYPE,Basicincident.STATE, Basicincident.EXP_NO,Basicincident.ARRIVAL, Fireincident.CAUSE_IGN, Basicincident.INC_NO,Basicincident.INC_DATE,InciAdd.CITY, InciAdd.ZIP5, Basicincident.PROP_USE, Fireincident.DETECTOR, Fireincident.DET_TYPE, Basicincident.FF_DEATH,Basicincident.OTH_DEATH, Basicincident.FF_INJ,Basicincident.OTH_INJ, Fireincident.AREA_ORIG, Fireincident.HEAT_SOURC, Fireincident.FIRST_IGN, Fireincident.FACT_IGN_1, Fireincident.FACT_IGN_2
             FROM (Basicincident INNER JOIN Fireincident ON (Basicincident.EXP_NO = Fireincident.EXP_NO) AND (Basicincident.INC_NO = Fireincident.INC_NO) AND (Basicincident.INC_DATE = Fireincident.INC_DATE) AND (Basicincident.FDID = Fireincident.FDID) AND (Basicincident.STATE = Fireincident.STATE)) INNER JOIN InciAdd ON (Fireincident.EXP_NO = InciAdd.EXP_NO) AND (Fireincident.INC_NO = InciAdd.INC_NO) AND (Fireincident.INC_DATE = InciAdd.INC_DATE) AND (Fireincident.FDID = InciAdd.FDID) AND (Fireincident.STATE = InciAdd.STATE) 
             ")
nrow(Data1)


#FIRE INCIDENT NARROWED DOWN WITH FIRE CODES#
Data2<-Data3<-sqldf("SELECT distinct FDID, STATE, EXP_NO,INC_TYPE, ARRIVAL, INC_NO, INC_DATE, CITY, ZIP5,EQUIP_INV, PROP_USE, CAUSE_IGN, OTH_DEATH, OTH_INJ, AREA_ORIG, HEAT_SOURC, FIRST_IGN, FACT_IGN_1, FACT_IGN_2
                    FROM Data1 Where ((EQUIP_INV)=000 
                    
                    
                    )")
nrow(Data2)


#FIRE INCIDENT NARROWED DOWN WITH FIRE CODES#

Data3<-sqldf("SELECT distinct FDID, STATE, EXP_NO,INC_TYPE, ARRIVAL, INC_NO, INC_DATE, CITY, ZIP5,EQUIP_INV, PROP_USE, CAUSE_IGN, OTH_DEATH, OTH_INJ, AREA_ORIG, HEAT_SOURC, FIRST_IGN
             FROM Data1 Where ((EQUIP_INV)=00 
             and (AREA_ORIG)=00 
             
             )")

nrow(Data3)


#FFCASUALTY NARROWED DOWN BY FACTOR CODES#
Data4<-sqldf("SELECT distinct STATE,FDID,EXP_NO, INC_NO, INC_DATE, PROP_USE, CITY, ZIP5, FF_DEATH, FF_INJ, SYMPTOM, CAUSE, FACTOR, CAREER, SEVERITY, LOCATION, INJ_DATE
             FROM Data2 WHERE ((( FACTOR >00) and (FACTOR <100)) and FACTOR !=01) 
             ")

#FFCASUALTY EXTRACTED BY FACTOR AND NARROWED MORE DOWN BY CAUSE#
Data5<-sqldf("SELECT distinct STATE, FDID, EXP_NO,INC_NO, INC_DATE, PROP_USE, CITY, ZIP5, FF_DEATH, FF_INJ, SYMPTOM, CAUSE, FACTOR, CAREER, SEVERITY, LOCATION, INJ_DATE
             FROM Data4 WHERE CAUSE !=0 and CAUSE !=0 and CAUSE !=0 and CAUSE !='U' and CAUSE !='NA'
             ")
