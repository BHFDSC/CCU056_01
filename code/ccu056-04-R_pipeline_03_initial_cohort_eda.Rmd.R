---
title: "CCU056 Pipeline"
subtitle: "Initial Cohort Insights"
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---

```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}


#libraries
library(odbc)
library(DBI)
library(tidyverse)
library(glue)
library(kableExtra)

#connect
con <- DBI::dbConnect(odbc::odbc(), dsn = "databricks", HTTPPath = "sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy", 
    PWD = ""
      #rstudioapi::askForPassword("Please enter Databricks PAT")
    )

#parameters
dsa = "dsa_391419_j3w9t_collab"
proj = "ccu056"
select_all = glue("SELECT * FROM ",dsa,".")
select_all_proj = glue("SELECT * FROM ",dsa,".",proj,"_")


```


```{r, data, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


acs <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_acs")))
tavi <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_tavi")))

# AS, SAVR and TAVI codelists
codelists_inclusions <- dbGetQuery(con,sprintf(glue(select_all_proj,"out_codelists_inclusions")))


skinny <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_all_cases_selected")))
skinny_sex <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_all_cases_selected_sex")))
lsoa <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_all_cases_lsoa_selected")))

# redone for ethnicity to find extra cases where detailed eth codes available
skinny_eth <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_all_cases_selected_ethnicity")))


ethnicty_codes <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_ethnicty_codes")))


```


```{r, data_1, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

main_cohort_final_first = read_rds("tmp/main_cohort_final_first.rds")

skinny_1 = skinny %>% 
  left_join(lsoa,by="PERSON_ID") %>%
  mutate(`_date_LSOA` = as.Date(`_date_LSOA`))

main_cohort_final_first %>% 
  left_join(skinny_1) %>%
  select(PERSON_ID,DOB, OPERATION_DATE,LSOA) %>%
  mutate(LSOA_Outside_England = ifelse(!str_starts(LSOA,"E|Unknown"),1,0)) %>%
  select(-LSOA) %>%
  write_csv("tmp/main_cohort_final_first_new2_supp.csv")


main_cohort_final_first %>% 
  left_join(skinny_1) %>%
  select(PERSON_ID,DOB, OPERATION_DATE,LSOA) %>%
  mutate(LSOA_Outside_England = ifelse(!str_starts(LSOA,"E|Unknown"),1,0)) %>%
  select(-LSOA) %>%
  write_rds("tmp/main_cohort_final_first_new2_supp.rds")


# COPIED TO DATABRICKS NOTEBOOK - MAIN COHORT 2
# Note that the v of this in DB will still have those who had an LSOA outside England that has been replaced above




```

```{r, data_2, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}



main_cohort_final_first = 
main_cohort_final_first %>%
  mutate(OPERATION_YEAR=year(OPERATION_DATE))



# Derived in KPC_sensitivity_analysis notebook
lsoa_eth_new <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_outside_england_lsoa_selected")))

lsoa_2011_imd = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_2011_imd_lookup")))
lsoa_2001_imd = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_2001_imd_lookup")))
lsoa_imd_long = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_imd_lookup_long")))


pop_lsoa = dbGetQuery(con,sprintf(glue(select_all,"hds_population_estimates_mid_2020_age_lsoa_sex")))
pop_ethnicity = dbGetQuery(con,sprintf(glue(select_all,"hds_population_estimates_2021_ethnicity_age_sex")))


#Region Lookup
region_lookup = dbGetQuery(con,sprintf(glue(select_all_proj,"cur_lsoa_region_lookup")))


```

```{r, lookup_tables, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

icd10 <- dbGetQuery(con,"SELECT * FROM dss_corporate.icd10_group_chapter_v01")
opcs = dbGetQuery(con,"SELECT * FROM dss_corporate.opcs_codes_v02")
gdppr_ref = dbGetQuery(con,"SELECT * FROM dss_corporate.gdppr_cluster_refset")


```

```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

study_start_date = "2020-01-01"

tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


```


# Key Patient Characteristics

Having established a cohort, we now derive the key patient characteristics (KPCs) for each case.

* DOB - used to derive age
* SEX
* ETHNICITY
* LSOA - used to derive IMD (deprivation)


## Methodology

Whilst DOB and ETHNICITY are constants (mutable), LSOA is variable (immutable) as it changes over time.

SEX, in many datasets is described as 'the current gender of the patient. The classification is phenotypical rather than genotypical, i.e. it does not provide codes for medical or scientific purposes.', thus also variable.

<br>We employ a different selection process for the mutable and immutable KPCs.

Sources include:

* HES APC
* GDPPR - including SNOMED codes
* Other HES datasets - HES Outpatients and HES A&E
* NACSA and TAVI audits


## Variable KPCs - LSOA

For LSOA we picked the record **as close to the date of procedure** (in either direction) as possible, regardless of the source.
In cases of ties, we then use the source priority groups to pick the most appropriate record (prioritised in the following order: HES APC, HES Outpatients/HES A&E, GDPPR, GDPPR (SNOMED), NACSA/TAVI).


E.g. if an LSOA record was found as at the date of procedure in NACSA and HES APC, in the case that these records were not the same value, the record from HES APC would be selected.

Note that this method prioritises the date at which a KPC record was found over the data source that KPC records are obtained from. That is, if a record is found as close to the date of procedure as possible in NACSA/TAVI then this record is selected over any records that may be found in HES APC, HES Outpatients/HES A&E, GDPPR, GDPPR (SNOMED) because this record has the smallest difference in date between the date of operation and the date at which the LSOA record was found.

Where the closest LSOA was found to be a Non England LSOA we looked to the next closest LSOA to see if this was in England. This replaced ~10% (388/3445) of the Non England LSOAs.

LSOA can be mapped to IMD as at 2004,2007,2010,2015,2019.

For surgeries that occurred 2000-2006, the IMD at 2004 is used
For surgeries that occurred 2007-2009, the IMD at 2007 is used
For surgeries that occurred 2010-2014, the IMD at 2010 is used
For surgeries that occurred 2015-2018, the IMD at 2015 is used
For surgeries that occurred 2019-2023, the IMD at 2019 is used

## Constant KPCs - DOB, ETHNICITY, SEX

Note that we have used the same methodology to derive Sex here, as we have for DOB and Ethnicity but please do note that, as mentioned above, Sex is the 'current gender' of the patient therefore Sex may change over time if a person changes gender or due to data error issues.

For DOB, Sex and Ethnicity we have found the record that is the **most recent (in relation to the study end date).** 
We have indicated cases where Sex as at the most recent record is different to the closest to procedure record. 

Again, we do not prioritise data sources unless in the case of ties.


For DOB, sources are prioritised as follows (to account for HES Outpatients and HES A&E defining Age instead of DOB:
HES APC, GDPPR, GDPPR (SNOMED), NACSA/TAVI, HES Outpatients/HES A&E.

Where DOB was found to equal 1800-01-01 or 1801-01-01, the full KPC history was searched to see if these patients had any DOBs within the plausible range. In all cases no useable DOBs were found. These DOBs were marked as Unknown.


```{r, create, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


#lsoa_eth_new
skinny = skinny %>% 
  left_join(lsoa,by="PERSON_ID") %>%
  mutate(`_date_LSOA` = as.Date(`_date_LSOA`))


# Here we add in the updated LSOAs for a subset of those that had LSOAs outside England - see the end of the notebook. Note that this SA in databricks was only done for 3321 people (but here those with LSOAs outside England is for 3368) - this is because the cohort was taken from the end of this script after other exlcusions were applied.

skinny = skinny %>%
  #filter(!str_starts(LSOA,"E|Unknown")) %>%
  left_join(
    (lsoa_eth_new %>% 
       select(PERSON_ID, LSOA_new=LSOA,
              `_date_LSOA_new`=`_date_LSOA`,
              `_source_LSOA_new`=`_source_LSOA`,
              `_tie_LSOA_new`=`_tie_LSOA`)
     ),by="PERSON_ID"
    ) %>%
  #filter(!is.na(LSOA))
  mutate(LSOA=ifelse(!is.na(LSOA_new),LSOA_new,LSOA)) %>%
  mutate(`_date_LSOA`=if_else(!is.na(`_date_LSOA_new`),as.Date(`_date_LSOA_new`),as.Date(`_date_LSOA`))) %>%
  mutate(`_source_LSOA`=ifelse(!is.na(`_source_LSOA_new`),`_source_LSOA_new`,`_source_LSOA`)) %>%
  mutate(`_tie_LSOA`=ifelse(!is.na(`_tie_LSOA_new`),`_tie_LSOA_new`,`_tie_LSOA`)) %>%
  
  select(-LSOA_new,-`_date_LSOA_new`,-`_tie_LSOA_new`,-`_source_LSOA_new`)



```



```{r, eth_new, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

#For those not in detailed codes

detailed_codelist = c(
"A",
"B",
"C",
"D",
"E",
"F",
"G",
"H",
"J",
"K",
"L",
"M",
"N",
"P",
"R",
"S",
"T",
"W"
)

ethnicity_detailed = skinny %>% select(PERSON_ID,ETHNIC,`_source_ETHNIC`, ETHNIC, `_date_ETHNIC`) %>%
  filter(!ETHNIC %in% detailed_codelist
) %>%
  select(PERSON_ID,ETHNIC,`_source_ETHNIC`,`_date_ETHNIC`) %>%
  left_join(
    (skinny_eth %>% select(PERSON_ID,ETHNIC2=ETHNIC,
                           `_source_ETHNIC2`=`_source_ETHNIC`,
                           `_date_ETHNIC2`=`_date_ETHNIC`)%>%
       filter(!ETHNIC2 %in% c("X","Z")))
    ) %>%
  filter(!is.na(ETHNIC2)) %>%
  filter(ETHNIC2!="") %>%
  select(PERSON_ID,ETHNIC2, `_date_ETHNIC2`, `_source_ETHNIC2`)


skinny=skinny %>%
  left_join(ethnicity_detailed) %>% 
  mutate(ETHNIC2 = ifelse(!is.na(ETHNIC2), ETHNIC2, ETHNIC)) %>%
  mutate(`_source_ETHNIC2` = ifelse(!is.na(`_source_ETHNIC2`), `_source_ETHNIC2`, `_source_ETHNIC`)) %>%
  mutate(`_date_ETHNIC2` = ifelse(!is.na(`_date_ETHNIC2`), `_date_ETHNIC2`, `_date_ETHNIC`)) %>%
  mutate(`_source_ETHNIC2` = ifelse(ETHNIC2%in%detailed_codelist, `_source_ETHNIC2`, NA)) %>%
  mutate(`_date_ETHNIC2` = ifelse(ETHNIC2%in%detailed_codelist, `_date_ETHNIC2`, NA)) %>%
  mutate(`ETHNIC2` = ifelse(ETHNIC2%in%detailed_codelist, `ETHNIC2`, NA))




main_cohort_final_first = main_cohort_final_first %>% left_join(skinny)
  
```




```{r, working_kpc_one, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first = main_cohort_final_first  %>%
  mutate(SEX=case_when(
    SEX =="1. Male" ~ "Male",
    SEX =="1" ~ "Male",
    SEX =="2. Female" ~ "Female",
    SEX =="2" ~ "Female",
    SEX =="0" ~ "Unknown",
    SEX =="9" ~ "Not Specified (Indeterminate)",
    is.na(SEX) ~ "Unknown"
  ))


main_cohort_final_first = main_cohort_final_first %>%
  mutate(source_ETHNIC_PARENT = case_when(
    `_source_ETHNIC` =="nacsa" ~ "audits",
    `_source_ETHNIC` =="tavi" ~ "audits",
    `_source_ETHNIC` =="gdppr" ~ "gdppr",
    `_source_ETHNIC` =="gdppr_snomed" ~ "gdppr",
    `_source_ETHNIC` =="hes_apc" ~ "hes",
    `_source_ETHNIC` =="hes_op" ~ "hes",
    `_source_ETHNIC` =="hes_ae" ~ "hes"
  ))


ethnic_lookup = 
main_cohort_final_first  %>% 
  select(ETHNIC, `source_ETHNIC_PARENT`) %>%
  distinct() %>%

  left_join(ethnicty_codes, by=c("ETHNIC"="ETHNIC", "source_ETHNIC_PARENT"="RECORD_SOURCE_PARENT")) %>%
  arrange(source_ETHNIC_PARENT,ETHNIC) %>%
  filter(!is.na(ETHNIC)) %>%

  
  mutate(ETHNIC_DESC_HIGH_ONLY = case_when(
    
    #Audits
    `ETHNIC` =="1. White" ~ "White",
    `ETHNIC` =="2. Black" ~ "Black or Black British",
    `ETHNIC` =="3. Asian" ~ "Asian or Asian British",
    `ETHNIC` =="4. Chinese" ~ "Chinese",
    `ETHNIC` =="8. Other" ~ "Other ethnic groups", #No Mixed category in audits - have mixed been grouped as Other?
    `ETHNIC` =="9. Unknown" ~ "Unknown",
    
    #GDPPR
    `ETHNIC` =="A" & source_ETHNIC_PARENT=="gdppr" ~ "White",
    `ETHNIC` =="B" & source_ETHNIC_PARENT=="gdppr" ~ "White",
    `ETHNIC` =="C" & source_ETHNIC_PARENT=="gdppr" ~ "White",
    `ETHNIC` =="D" & source_ETHNIC_PARENT=="gdppr" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="E" & source_ETHNIC_PARENT=="gdppr" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="F" & source_ETHNIC_PARENT=="gdppr" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="G" & source_ETHNIC_PARENT=="gdppr" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="H" & source_ETHNIC_PARENT=="gdppr" ~ "Asian or Asian British",
    `ETHNIC` =="J" & source_ETHNIC_PARENT=="gdppr" ~ "Asian or Asian British",
    `ETHNIC` =="K" & source_ETHNIC_PARENT=="gdppr" ~ "Asian or Asian British",
    `ETHNIC` =="L" & source_ETHNIC_PARENT=="gdppr" ~ "Asian or Asian British",
    `ETHNIC` =="M" & source_ETHNIC_PARENT=="gdppr" ~ "Black or Black British",
    `ETHNIC` =="N" & source_ETHNIC_PARENT=="gdppr" ~ "Black or Black British",
    `ETHNIC` =="P" & source_ETHNIC_PARENT=="gdppr" ~ "Black or Black British",
    `ETHNIC` =="R" & source_ETHNIC_PARENT=="gdppr" ~ "Other ethnic groups",
    `ETHNIC` =="S" & source_ETHNIC_PARENT=="gdppr" ~ "Other ethnic groups",
    `ETHNIC` =="X" & source_ETHNIC_PARENT=="gdppr" ~ "Unknown",
    `ETHNIC` =="Z" & source_ETHNIC_PARENT=="gdppr" ~ "Unknown",
    #https://www.medrxiv.org/content/10.1101/2022.11.11.22282217v1.full
    `ETHNIC` =="W" & source_ETHNIC_PARENT=="gdppr" ~ "Other ethnic groups", #comes from SNOMED
    `ETHNIC` =="T" & source_ETHNIC_PARENT=="gdppr" ~ "White", #comes from SNOMED
    
    #HES
    `ETHNIC` =="0" & source_ETHNIC_PARENT=="hes" ~ "White",
    `ETHNIC` =="1" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="2" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="3" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="4" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="5" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="6" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="7" & source_ETHNIC_PARENT=="hes" ~ "Other ethnic groups",
    `ETHNIC` =="8" & source_ETHNIC_PARENT=="hes" ~ "Other ethnic groups",
    `ETHNIC` =="9" & source_ETHNIC_PARENT=="hes" ~ "Unknown",
    `ETHNIC` =="99" & source_ETHNIC_PARENT=="hes" ~ "Unknown",
    
    `ETHNIC` =="A" & source_ETHNIC_PARENT=="hes" ~ "White",
    `ETHNIC` =="B" & source_ETHNIC_PARENT=="hes" ~ "White",
    `ETHNIC` =="C" & source_ETHNIC_PARENT=="hes" ~ "White",
    `ETHNIC` =="D" & source_ETHNIC_PARENT=="hes" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="E" & source_ETHNIC_PARENT=="hes" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="F" & source_ETHNIC_PARENT=="hes" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="G" & source_ETHNIC_PARENT=="hes" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="H" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="J" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="K" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="L" & source_ETHNIC_PARENT=="hes" ~ "Asian or Asian British",
    `ETHNIC` =="M" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="N" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="P" & source_ETHNIC_PARENT=="hes" ~ "Black or Black British",
    `ETHNIC` =="R" & source_ETHNIC_PARENT=="hes" ~ "Other ethnic groups",
    `ETHNIC` =="S" & source_ETHNIC_PARENT=="hes" ~ "Other ethnic groups",
    `ETHNIC` =="X" & source_ETHNIC_PARENT=="hes" ~ "Unknown",
    `ETHNIC` =="Z" & source_ETHNIC_PARENT=="hes" ~ "Unknown"

  )) %>%
  
mutate(ETHNIC_DESC_DETAIL = case_when(

    #GDPPR
    `ETHNIC` =="A" ~ "British",
    `ETHNIC` =="B" ~ "Irish",
    `ETHNIC` =="C" ~ "Any other White background",
    `ETHNIC` =="D" ~ "White and Black Caribbean",
    `ETHNIC` =="E" ~ "White and Black African",
    `ETHNIC` =="F" ~ "White and Asian",
    `ETHNIC` =="G" ~ "Any other Mixed background",
    `ETHNIC` =="H" ~ "Indian",
    `ETHNIC` =="J" ~ "Pakistani",
    `ETHNIC` =="K" ~ "Bangladeshi",
    `ETHNIC` =="L" ~ "Any other Asian background",
    `ETHNIC` =="M" ~ "Caribbean",
    `ETHNIC` =="N" ~ "African",
    `ETHNIC` =="P" ~ "Any other Black background",
    `ETHNIC` =="R" ~ "Chinese",
    `ETHNIC` =="S" ~ "Any other ethnic group",
    #https://www.medrxiv.org/content/10.1101/2022.11.11.22282217v1.full
    `ETHNIC` =="W" ~ "Any other ethnic group", #comes from SNOMED - Arab
    `ETHNIC` =="T" ~ "Any other White background", #comes from SNOMED - Gypsy or Irish Traveller
    `ETHNIC` =="X" ~ "Unknown",
    `ETHNIC` =="Z" ~ "Unknown"


  )) %>%
  
mutate(ETHNIC_DESC_DETAIL_HIGH = case_when(

    #GDPPR
    `ETHNIC` =="A" ~ "White",
    `ETHNIC` =="B" ~ "White",
    `ETHNIC` =="C" ~ "White",
    `ETHNIC` =="D" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="E" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="F" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="G" ~ "Mixed or Multiple ethnic groups",
    `ETHNIC` =="H" ~ "Asian or Asian British",
    `ETHNIC` =="J" ~ "Asian or Asian British",
    `ETHNIC` =="K" ~ "Asian or Asian British",
    `ETHNIC` =="L" ~ "Asian or Asian British",
    `ETHNIC` =="M" ~ "Black or Black British",
    `ETHNIC` =="N" ~ "Black or Black British",
    `ETHNIC` =="P" ~ "Black or Black British",
    `ETHNIC` =="R" ~ "Other ethnic groups",
    `ETHNIC` =="S" ~ "Other ethnic groups",
    #https://www.medrxiv.org/content/10.1101/2022.11.11.22282217v1.full
    `ETHNIC` =="W" ~ "Other ethnic groups", #comes from SNOMED
    `ETHNIC` =="T" ~ "White", #comes from SNOMED
    `ETHNIC` =="X" ~ "Unknown",
    `ETHNIC` =="Z" ~ "Unknown"

  )) %>%
  
  select(ETHNIC,source_ETHNIC_PARENT,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL,ETHNIC_DESC_DETAIL_HIGH)




main_cohort_final_first =main_cohort_final_first %>% left_join(ethnic_lookup, by=c("ETHNIC","source_ETHNIC_PARENT")) %>%
left_join((ethnic_lookup%>%select(ETHNIC2=ETHNIC,
                                  source_ETHNIC_PARENT2=source_ETHNIC_PARENT,
                                  ETHNIC_DESC_HIGH_ONLY2=ETHNIC_DESC_HIGH_ONLY,
                                  ETHNIC_DESC_DETAIL2=ETHNIC_DESC_DETAIL,
                                  ETHNIC_DESC_DETAIL_HIGH2=ETHNIC_DESC_DETAIL_HIGH)),
          by=c("ETHNIC2"="ETHNIC2","source_ETHNIC_PARENT"="source_ETHNIC_PARENT2"))
  

main_cohort_final_first =main_cohort_final_first %>% 
  mutate(ETHNIC_DESC_HIGH_ONLY = ifelse(is.na(ETHNIC_DESC_HIGH_ONLY),"Unknown",ETHNIC_DESC_HIGH_ONLY)) %>% 
  mutate(ETHNIC_DESC_DETAIL = ifelse(is.na(ETHNIC_DESC_DETAIL),"Unknown",ETHNIC_DESC_DETAIL)) %>% 
  mutate(ETHNIC_DESC_DETAIL_HIGH = ifelse(is.na(ETHNIC_DESC_DETAIL_HIGH),"Unknown",ETHNIC_DESC_DETAIL_HIGH)) %>% 
  mutate(ETHNIC_DESC_HIGH_ONLY2 = ifelse(is.na(ETHNIC_DESC_HIGH_ONLY2),"Unknown",ETHNIC_DESC_HIGH_ONLY2)) %>% 
  mutate(ETHNIC_DESC_DETAIL2 = ifelse(is.na(ETHNIC_DESC_DETAIL2),"Unknown",ETHNIC_DESC_DETAIL2)) %>% 
  mutate(ETHNIC_DESC_DETAIL_HIGH2 = ifelse(is.na(ETHNIC_DESC_DETAIL_HIGH2),"Unknown",ETHNIC_DESC_DETAIL_HIGH2))


main_cohort_final_first = 
main_cohort_final_first %>% select(
  PERSON_ID,
  OPERATION_DATE,
  AS_EARLIST_DATE,
  procedure_as_number,
  overall_no_as_procedures,
  overall_no_procedures,
  AS_GROUP_START_DATE,
  AS_GROUP_END_DATE,
  combination,
  surgery_type,
  DOB,
  SEX,
  ETHNIC,
  ETHNIC_DESC_HIGH_ONLY,
  ETHNIC2,
  ETHNIC_DESC_DETAIL2,
  ETHNIC_DESC_DETAIL_HIGH2,
  LSOA,
  `_date_DOB`,`_source_DOB`,`_tie_DOB`,
  `_date_SEX`,`_source_SEX`,`_tie_SEX`,
  `_date_ETHNIC`,`_source_ETHNIC`,`_tie_ETHNIC`,
  `_date_ETHNIC2`,`_source_ETHNIC2`,
  `_date_LSOA`,`_source_LSOA`,`_tie_LSOA`
)


# main_cohort_final_first %>%
#   group_by(ETHNIC_DESC_HIGH_ONLY) %>% count() %>%
#   ungroup() %>% mutate(sum=sum(n))
# 
# main_cohort_final_first %>%
#   group_by(ETHNIC_DESC_DETAIL_HIGH2) %>% count() %>%
#   ungroup() %>% mutate(sum=sum(n))
# 
# main_cohort_final_first %>%
#   group_by(ETHNIC_DESC_DETAIL2) %>% count() %>%
#   ungroup() %>% mutate(sum=sum(n))


ethnic_lookup %>% write_rds("tmp/ethnic_lookup.rds")


```


```{r, working_kpc_excl, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first = main_cohort_final_first %>%
  mutate(age = as.numeric(difftime(OPERATION_DATE, DOB, units = "days")) / 365.25) %>%
    mutate(age_group = case_when(
    `age` <18 ~ "<18",
    `age` >=18 & age <25 ~ "18-24",
    `age` >=25 & age <30 ~ "25-29",
    `age` >=30 & age <35 ~ "30-34",
    `age` >=35 & age <40 ~ "35-39",
    `age` >=40 & age <45 ~ "40-44",
    `age` >=45 & age <50 ~ "45-49",
    `age` >=50 & age <55 ~ "50-54",
    `age` >=55 & age <60 ~ "55-59",
    `age` >=60 & age <65 ~ "60-64",
    `age` >=65 & age <70 ~ "65-69",
    `age` >=70 & age <75 ~ "70-74",
    `age` >=75 & age <80 ~ "75-79",
    `age` >=80 & age <85 ~ "80-84",
    `age` >=85 & age <90 ~ "85-89",
    `age` >=90 ~ ">=90"
  )
  )

# population age groups only go to 85+
main_cohort_final_first = main_cohort_final_first %>%
  mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group))



main_cohort_final_first = main_cohort_final_first %>%
  mutate(age=ifelse(DOB=="1800-01-01",NA,age)) %>%
  mutate(age=ifelse(DOB=="1801-01-01",NA,age)) %>%
  mutate(age_group=ifelse(DOB=="1800-01-01","Unknown",age_group)) %>%
  mutate(age_group=ifelse(DOB=="1801-01-01","Unknown",age_group)) %>%
  mutate(age_group = ifelse(is.na(DOB),"Unknown",age_group))


main_cohort_final_first = main_cohort_final_first %>% mutate(age_group=ifelse(is.na(age_group),"Unknown",age_group))


```


```{r, working_kpc_two, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first = main_cohort_final_first %>%
  mutate(treat = case_when(
    OPERATION_DATE >= "2000-01-01" & OPERATION_DATE < "2005-01-01" ~ "PRIOR - Beginning",
    OPERATION_DATE >= "2005-01-01" & OPERATION_DATE < "2015-01-01" ~ "PRIOR - Middle",
    OPERATION_DATE >= "2015-01-01" & OPERATION_DATE < "2020-01-01" ~ "PRIOR - End",
    OPERATION_DATE >= "2020-01-01" ~ "PANDEMIC - During"
  ))


main_cohort_final_first = main_cohort_final_first %>% mutate(LSOA = ifelse(is.na(LSOA),"Unknown", LSOA))

```


```{r, working_kpc_three, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


# LSOA WORKING

# Joining on all LSOA Years and IMD Years and choosing the IMD Year closest to the operation as possible - this also resolves the issue where LSOAs were not mapping to IMDs because we are also joining on the LSOA 2001s

# main_cohort_final_first = main_cohort_final_first %>%
#   left_join(lsoa_2011_imd, by=c("LSOA"="LSOA_2011")) %>%
#   mutate(IMD_2019_DECILES = ifelse(is.na(IMD_2019_DECILES),"Unknown",IMD_2019_DECILES)) %>%
#   mutate(IMD_2019_QUINTILES = ifelse(is.na(IMD_2019_QUINTILES),"Unknown",IMD_2019_QUINTILES))



lsoa_not_unknown = main_cohort_final_first %>%
  filter(LSOA!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,LSOA) %>%
  left_join(lsoa_imd_long) %>%
  mutate(DATE_DIFF = as.numeric(difftime(OPERATION_DATE, as.Date(paste0(IMD_YEAR,"-01-01")), units = "days")))

  
main_cohort_final_first_new =
lsoa_not_unknown %>%
  group_by(PERSON_ID) %>%
  summarise(
    keep_row = case_when(
      all(DATE_DIFF > 0) ~ min(DATE_DIFF),
      all(DATE_DIFF < 0) ~ min(abs(DATE_DIFF)),
      any(DATE_DIFF > 0) ~ min(DATE_DIFF[DATE_DIFF > 0]),
      any(DATE_DIFF == 0) ~ 0,
      TRUE ~ NA_integer_
    )
  ) %>%
  left_join(lsoa_not_unknown) %>%
  group_by(PERSON_ID) %>%
  mutate(
    type = case_when(
      all(DATE_DIFF > 0) ~ 0,
      all(DATE_DIFF < 0) ~ 0,
      any(DATE_DIFF == 0) ~ 1,
      any(DATE_DIFF > 0) ~ 1,
      TRUE ~ NA_integer_
    )
  ) %>%
  group_by(PERSON_ID) %>%
  filter(case_when(type==1 ~ DATE_DIFF==keep_row,
                   type==0 ~ abs(DATE_DIFF)==keep_row)) %>%
  select(-keep_row,-DATE_DIFF,-type) %>%
  #join back in those with LSOA Unknown or non England LSOA
  bind_rows(main_cohort_final_first %>%
  filter(!str_starts(LSOA,"E")) %>% select(PERSON_ID,OPERATION_DATE,LSOA)) %>%
  left_join(main_cohort_final_first)


main_cohort_final_first_new = main_cohort_final_first_new %>%
  
  mutate(DECILE = as.character(DECILE)) %>%
  mutate(QUINTILE = as.character(QUINTILE)) %>%
  mutate(LSOA_YEAR = as.character(LSOA_YEAR)) %>%
  mutate(IMD_YEAR = as.character(IMD_YEAR)) %>%
  
  mutate(DECILE = factor(DECILE, levels = c("1","2","3","4","5","6","7","8","9","10","Unknown"))) %>%
  mutate(QUINTILE = factor(QUINTILE, levels = c("1","2","3","4","5","Unknown"))) %>%
  
  mutate(LSOA_YEAR = ifelse(is.na(LSOA_YEAR),"Unknown",as.character(LSOA_YEAR))) %>%
  mutate(IMD_YEAR = ifelse(is.na(IMD_YEAR),"Unknown",as.character(IMD_YEAR))) %>%
  mutate(DECILE = ifelse(is.na(DECILE),"Unknown",as.character(DECILE))) %>%
  mutate(QUINTILE = ifelse(is.na(QUINTILE),"Unknown",as.character(QUINTILE)))

deaths_out_sing = dbGetQuery(con,sprintf(glue(select_all_proj,"cur_deaths_dars_nic_391419_j3w9t_archive_sing")))

deaths_dates = deaths_out_sing %>% select(PERSON_ID,DATE_DEATH=REG_DATE_OF_DEATH)

main_cohort_final_first_new = main_cohort_final_first_new %>% left_join(deaths_dates)


main_cohort_final_first_new = 
main_cohort_final_first_new %>%
  mutate(OPERATION_YEAR=year(OPERATION_DATE)) %>%
  mutate(treat_old = treat)

main_cohort_final_first_new =
main_cohort_final_first_new %>%
    mutate(treat = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_))

main_cohort_final_first_new %>% write_rds("tmp/main_cohort_final_first_new.rds")



```



```{r, working_kpc_four, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first_new = read_rds("tmp/main_cohort_final_first_new.rds")



kpc_exclusions_df = main_cohort_final_first_new %>%
  select(PERSON_ID,age,LSOA,SEX,ETHNIC_DESC_HIGH_ONLY,age_group,DECILE,QUINTILE,DATE_DEATH)



# APPLY KPC EXCLUSIONS
main_cohort_final_first_new1 = main_cohort_final_first_new %>% 
  filter(!is.na(age)) %>%
  # filter(str_starts(LSOA,"E|Unknown")) %>% 
  filter(age >=18) %>%
  filter(!(SEX=="Unknown" & ETHNIC_DESC_HIGH_ONLY == "Unknown" & DECILE == "Unknown")) %>%
  filter(SEX!="Not Specified (Indeterminate)") %>%
  filter(DATE_DEATH >= OPERATION_DATE | is.na(DATE_DEATH)) %>%
  mutate(ETHNIC_DESC_HIGH_ONLY = ifelse(ETHNIC_DESC_HIGH_ONLY=="Other ethnic groups",
                                        "Unknown",ETHNIC_DESC_HIGH_ONLY))
  #filter(ETHNIC_DESC_HIGH_ONLY != "Other ethnic groups")

  
main_cohort_final_first_new1 %>% write_rds("tmp/main_cohort_final_first_new1.rds")

main_cohort_final_first_new1 = read_rds("tmp/main_cohort_final_first_new1.rds")

main_cohort_final_first_new1 = 
main_cohort_final_first_new1 %>%
  mutate(SEX = factor(SEX, levels = c("Male", 
                  "Female",
                  #"Not Specified (Indeterminate)",
                  "Unknown"))) %>%
  mutate(age_group = factor(age_group, levels = c("18-24",
                  "25-29",
                  "30-34",
                  "35-39",
                  "40-44",
                  "45-49",
                  "50-54",
                  "55-59",
                  "60-64",
                  "65-69",
                  "70-74",
                  "75-79",
                  "80-84",
                  "85+",
                  
                  "Unknown"))) %>%
  # mutate(treat = factor(treat, levels = c("PRIOR - Beginning", 
  #                 "PRIOR - Middle",
  #                 "PRIOR - End",
  #                 "PANDEMIC - During"))) %>%
  mutate(treat = factor(treat, levels = c("2000-2004",
                                          "2005-2009",
                                          "2010-2014",
                                          "2015-2019",
                                          "2020-2023"
                                          ))) %>%
  mutate(DECILE = factor(DECILE, levels = c("1","2","3","4","5","6","7","8","9","10","Unknown"))) %>%
  mutate(QUINTILE = factor(QUINTILE, levels = c("1","2","3","4","5","Unknown")))





main_cohort_final_first_new1 %>% write_rds("tmp/main_cohort_final_first_new1.rds")

skinny_small = main_cohort_final_first_new1 %>% select(PERSON_ID,DOB,age,age_group,SEX,
                                                      DECILE,QUINTILE,
                                   ETHNIC_DESC_HIGH_ONLY,
                                   ETHNIC_DESC_DETAIL2,
                                   ETHNIC_DESC_DETAIL_HIGH2,
                                   treat,surgery_type) %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI"))


skinny_small %>% write_rds("tmp/skinny_small.rds")



#no of people who have different sex code at 
different_sex = skinny %>% select(PERSON_ID,SEX) %>%
  left_join((skinny_sex%>%select(PERSON_ID,SEX_OP=SEX))) %>%
  filter(SEX!=SEX_OP) %>% count() %>% pull()

different_sex %>% write_rds("tmp/different_sex.rds")



```



```{r, lsoa_explore, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# those who dont have a complete KPC set (using high level ethnicity)
# 1929
skinny_small %>% filter(SEX=="Unknown" | ETHNIC_DESC_HIGH_ONLY == "Unknown" | age_group == "Unknown" | DECILE == "Unknown")


```


## KPC Exclusions

Having derived the KPCs we now apply the following exclusions to the Main Cohort:

* Those with unknown DOB (and thus age) removed
* Age (at operation) < 18 years removed
* Persons who have no known KPCs (sex,ethnicity,imd all unknown) removed
* Those with Sex = Not Specified (Indeterminate) removed
* Those with DOD before Date of Surgery removed
* Other Ethnicity group set to 'Unknown' Ethnicity

Note that for those who have an LSOA outside of England we have went on to find any KPCs inside England for this group thus they will not be removed yet.


```{r, kpcExclusions_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first_new1 = read_rds("tmp/main_cohort_final_first_new1.rds")

kpc_exclusions = data.frame(
`Title` = c("Main Cohort"), `n` = c(kpc_exclusions_df %>% nrow())
) %>%
  mutate(n=(n)) %>%

  add_row(`Title`=c("Main Cohort - KPC Exclusions"), 
          `n`=c((main_cohort_final_first_new1 %>% nrow()))) %>%

     #apply SDC
     mutate(n = case_when(
     n < 10 ~ 10,
     TRUE ~ round(n / 5) * 5
     )) %>%
  mutate(n=format(n, big.mark = ","))

kpc_exclusions %>% write_rds("tmp/kpc_exclusions.rds")



```

```{r, kpcExclusions, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

kpc_exclusions = read_rds("tmp/kpc_exclusions.rds")
     
kableExtra::kbl(
  kpc_exclusions,
  col.names = NULL,
    booktabs = T, 
    caption=("KPC Exclusions"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling()%>%
  kableExtra::row_spec(2,bold=T,hline_after = T)
     


```


## Table 1 - stratified by treatment time period

```{r, tableOne, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

library(table1)

skinny_small = read_rds("tmp/skinny_small.rds")

main_cohort_final_first_new1 = read_rds("tmp/main_cohort_final_first_new1.rds")

main_cohort_final_first_new1 = main_cohort_final_first_new1


skinny_small$SEX <- 
  factor(skinny_small$SEX, levels=c("Male", 
                  "Female",
                  #"Not Specified (Indeterminate)",
                  "Unknown"))
 
skinny_small$age_group <- 
  factor(skinny_small$age_group,
         levels=c("18-24",
                  "25-29",
                  "30-34",
                  "35-39",
                  "40-44",
                  "45-49",
                  "50-54",
                  "55-59",
                  "60-64",
                  "65-69",
                  "70-74",
                  "75-79",
                  "80-84",
                  "85+",
                  "Unknown"))

# skinny_small$treat <- 
#   factor(skinny_small$treat,
#          levels=c("PRIOR - Beginning", 
#                   "PRIOR - Middle",
#                   "PRIOR - End",
#                   "PANDEMIC - During"))

skinny_small$treat <- 
  factor(skinny_small$treat,
         levels=c("2000-2004","2005-2009","2010-2014","2015-2019","2020-2023"))

skinny_small$DECILE <- 
  factor(skinny_small$DECILE,
         levels=c("1","2","3","4","5","6","7","8","9","10","Unknown"))

skinny_small$QUINTILE <- 
  factor(skinny_small$QUINTILE,
         levels=c("1","2","3","4","5","Unknown"))


eth_desc = unique((names(sort(table(skinny_small$ETHNIC_DESC_HIGH_ONLY), decreasing = TRUE))))
eth_desc_detail = unique((names(sort(table(skinny_small$ETHNIC_DESC_DETAIL2), decreasing = TRUE))))


skinny_small$ETHNIC_DESC_HIGH_ONLY <-
factor(skinny_small$ETHNIC_DESC_HIGH_ONLY, 
       levels = as.factor(c(eth_desc[-which((eth_desc) == "Unknown")],"Unknown"))
       )


skinny_small$ETHNIC_DESC_DETAIL2 <-
factor(skinny_small$ETHNIC_DESC_DETAIL2, 
       levels = as.factor(c(eth_desc_detail[-which((eth_desc_detail) == "Unknown")],"Unknown"))
       )



label(skinny_small$SEX)       <- "Sex"
label(skinny_small$age)       <- "Age"
label(skinny_small$age_group)       <- "Age group"
label(skinny_small$ETHNIC_DESC_HIGH_ONLY)       <- "Ethnicity (High-level only)"
label(skinny_small$ETHNIC_DESC_DETAIL2)       <- "Ethnicity (Detailed)"
label(skinny_small$DECILE)       <- "IMD Deciles"
label(skinny_small$QUINTILE)       <- "IMD Quintiles"

units(skinny_small$age)       <- "years"
units(skinny_small$age_group)       <- "years"





my.render.cat <- function(x) {
    c("", sapply(stats.default(x),
                 function(y) with(y,sprintf("%d (%0.0f %%)", if(FREQ>10){round(FREQ / 5) * 5}else{10}, PCT))
                 ))
}

my.render.strat <- function (label, n, ...) {

  sprintf("<span class='stratlabel'>%s<br><span class='stratn'>(N=%s)</span></span>", 
          label, prettyNum((round(as.numeric(n) / 5) * 5), big.mark=","))
}



footnote <- "SDC applied"

table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + DECILE + QUINTILE | treat, data=skinny_small,
               render.categorical=my.render.cat, footnote=footnote, render.strat = my.render.strat,
       render.missing = NULL #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
       )


```



## Table 1 - stratified by treatment type

```{r, tableOneType, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


skinny_small$SEX <- 
  factor(skinny_small$SEX, levels=c("Male", 
                  "Female",
                  #"Not Specified (Indeterminate)",
                  "Unknown"))
 
skinny_small$age_group <- 
  factor(skinny_small$age_group,
         levels=c("18-24",
                  "25-29",
                  "30-34",
                  "35-39",
                  "40-44",
                  "45-49",
                  "50-54",
                  "55-59",
                  "60-64",
                  "65-69",
                  "70-74",
                  "75-79",
                  "80-84",
                  "85+",
                  "Unknown"))

skinny_small$surgery_type <- 
  factor(skinny_small$surgery_type,
         levels=c("SAVR","TAVI"))

skinny_small$DECILE <- 
  factor(skinny_small$DECILE,
         levels=c("1","2","3","4","5","6","7","8","9","10","Unknown"))

skinny_small$QUINTILE <- 
  factor(skinny_small$QUINTILE,
         levels=c("1","2","3","4","5","Unknown"))


eth_desc = unique((names(sort(table(skinny_small$ETHNIC_DESC_HIGH_ONLY), decreasing = TRUE))))
eth_desc_detail = unique((names(sort(table(skinny_small$ETHNIC_DESC_DETAIL2), decreasing = TRUE))))


skinny_small$ETHNIC_DESC_HIGH_ONLY <-
factor(skinny_small$ETHNIC_DESC_HIGH_ONLY, 
       levels = as.factor(c(eth_desc[-which((eth_desc) == "Unknown")],"Unknown"))
       )


skinny_small$ETHNIC_DESC_DETAIL2 <-
factor(skinny_small$ETHNIC_DESC_DETAIL2, 
       levels = as.factor(c(eth_desc_detail[-which((eth_desc_detail) == "Unknown")],"Unknown"))
       )



label(skinny_small$SEX)       <- "Sex"
label(skinny_small$age)       <- "Age"
label(skinny_small$age_group)       <- "Age group"
label(skinny_small$ETHNIC_DESC_HIGH_ONLY)       <- "Ethnicity (High-level only)"
label(skinny_small$ETHNIC_DESC_DETAIL2)       <- "Ethnicity (Detailed)"
label(skinny_small$DECILE)       <- "IMD Deciles"
label(skinny_small$QUINTILE)       <- "IMD Quintiles"

units(skinny_small$age)       <- "years"
units(skinny_small$age_group)       <- "years"





my.render.cat <- function(x) {
    c("", sapply(stats.default(x),
                 function(y) with(y,sprintf("%d (%0.0f %%)", if(FREQ>10){round(FREQ / 5) * 5}else{10}, PCT))
                 ))
}

my.render.strat <- function (label, n, ...) {

  sprintf("<span class='stratlabel'>%s<br><span class='stratn'>(N=%s)</span></span>", 
          label, prettyNum((round(as.numeric(n) / 5) * 5), big.mark=","))
}



footnote <- "SDC applied"

table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + DECILE + QUINTILE | surgery_type, data=skinny_small,
               render.categorical=my.render.cat, footnote=footnote, render.strat = my.render.strat,
       render.missing = NULL #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
       )


```


### Ethnicity Detailed

Using more detailed ethnicity groups comes at the cost of losing ~0.7% of the cohort.



## KPCs over time


```{r, kpc_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs = main_cohort_final_first_new1 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,SEX,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))


full_date_df_kpcs %>% write_rds("tmp/kpcs_time_sex.rds")


```

```{r, sexPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Sex coverage"}


full_date_df = read_rds("tmp/kpcs_time_sex.rds") %>% ungroup()

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = SEX, color=SEX)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22", "Not Specified (Indeterminate)"="blue",
                                           "Unknown"="grey"))
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8") 


```






```{r, age_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs = main_cohort_final_first_new1 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,age,age_group,DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,age_group,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))


full_date_df_kpcs %>% write_rds("tmp/kpcs_time_age.rds")


```

```{r, agePlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Age coverage"}


full_date_df = read_rds("tmp/kpcs_time_age.rds") %>% ungroup()

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = age_group, color=age_group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1))
  # scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22", "Not Specified (Indeterminate)"="blue",
  #                                          "Unknown"="grey"))
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8") 


```



```{r, eth_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs = main_cohort_final_first_new1 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,
         ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,
         age,age_group,DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,ETHNIC_DESC_HIGH_ONLY,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))


full_date_df_kpcs %>% write_rds("tmp/kpcs_time_eth.rds")


```

```{r, ethPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Ethnicity coverage"}


full_date_df = read_rds("tmp/kpcs_time_eth.rds") %>% ungroup()

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 


custom_colors <- c("#FF042E", "#3CB08C", "#FF5100", "#7346CB", "#FFCD16", "grey") #"#B4E4EA"

ggplot((full_date_df %>%
          mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
                                              levels=c(
                                                "Asian or Asian British",
                                                "Black or Black British",
                                                "Mixed or Multiple ethnic groups",
                                                "Other ethnic groups",
                                                "White",
                                                "Unknown"
                                              )))
          ),
       aes(x = year_month_label, y = count, group = ETHNIC_DESC_HIGH_ONLY, color=ETHNIC_DESC_HIGH_ONLY)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  #scale_color_manual(values = custom_colors) +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100"))
  # scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22", "Not Specified (Indeterminate)"="blue",
  #                                          "Unknown"="grey"))
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8")  


```







```{r, deciles_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs = main_cohort_final_first_new1 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,
         ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,
         age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,DECILE,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))


full_date_df_kpcs %>% write_rds("tmp/kpcs_time_deciles.rds")


```

```{r, decilesPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC IMD Deciles coverage"}


full_date_df = read_rds("tmp/kpcs_time_deciles.rds") %>% ungroup()

full_date_df$DECILE <- 
  factor(full_date_df$DECILE,
         levels=c("1","2","3","4","5","6","7","8","9","10","Unknown"))

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = DECILE, color=DECILE)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
 
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                           "2"="#CB3428",
                                           "3"="#E56B42",
                                           "4"="#ECA75D",
                                           "5"="#EDD587",
                                           "6"="#D0E5EA",
                                           "7"="#A0CCDE",
                                           "8"="#6EA4C8",
                                           "9"="#4371AF",
                                           "10"="#333793",
                                          "Unknown"="grey"))
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8") 


```



```{r, quintiles_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs = main_cohort_final_first_new1 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,
         ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,
         age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,QUINTILE,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))


full_date_df_kpcs %>% write_rds("tmp/kpcs_time_quintiles.rds")


```

```{r, quintilesPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC IMD Quintiles coverage"}


full_date_df = read_rds("tmp/kpcs_time_quintiles.rds") %>% ungroup()

full_date_df$QUINTILE <- 
  factor(full_date_df$QUINTILE,
         levels=c("1","2","3","4","5","Unknown"))

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = QUINTILE, color=QUINTILE)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
 
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793",
                                          "Unknown"="grey"))
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8") 


```



## KPCs over time, per 100,000 population

### Population Denominator

Please note that there is no population estimates for Sex = Not Specified (Indeterminate)

```{r, join_pop_ests, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


ethnicity_population_mapping = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Any other ethnic group",
"Chinese",
"Any other White background",
"Any other White background",
"Any other White background",
"British",
"Irish"
),

ons_20b_detail = c(
"Asian, Asian British or Asian Welsh: Other Asian",
"Asian, Asian British or Asian Welsh: Bangladeshi",
"Asian, Asian British or Asian Welsh: Indian",
"Asian, Asian British or Asian Welsh: Pakistani",
"Black, Black British, Black Welsh, Caribbean or African: African",
"Black, Black British, Black Welsh, Caribbean or African: Other Black",
"Black, Black British, Black Welsh, Caribbean or African: Caribbean",
"Mixed or Multiple ethnic groups: Other Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups: White and Asian",
"Mixed or Multiple ethnic groups: White and Black African",
"Mixed or Multiple ethnic groups: White and Black Caribbean",
"Other ethnic group: Any other ethnic group",
"Other ethnic group: Arab",
"Asian, Asian British or Asian Welsh: Chinese",
"White: Gypsy or Irish Traveller",
"White: Roma",
"White: Other White",
"White: English, Welsh, Scottish, Northern Irish or British",
"White: Irish"
)

)


pop_ethnicity_mapped = 
pop_ethnicity %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping, by=c("ethnicity"="ons_20b_detail")) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age, n) %>%
  arrange(ethnicity,sex,age) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, sex, age) %>%
  summarise(n=sum(n)) %>%
  mutate(age=ifelse(age=="100 or over",as.numeric(101), as.numeric(age))) %>%
  ungroup() %>%
  filter(age>=18) %>%
  mutate(age_group =
          case_when(
age <18 ~ "<18",
(age >= 18 & age <= 25)~"18-24",
(age >= 25 & age <= 29)~ "25-29",
(age >= 30 & age <= 34)~ "30-34",
(age >= 35 & age <= 39)~ "35-39",
(age >= 40 & age <= 44)~ "40-44",
(age >= 45 & age <= 49)~ "45-49",
(age >= 50 & age <= 54)~ "50-54",
(age >= 55 & age <= 59)~ "55-59",
(age >= 60 & age <= 64)~ "60-64",
(age >= 65 & age <= 69)~ "65-69",
(age >= 70 & age <= 74)~ "70-74",
(age >= 75 & age <= 79)~ "75-79",
(age >= 80 & age <= 84)~ "80-84",
.default = "85+"
)) %>%
  ungroup() %>%
  select(-age) %>%
  group_by(ethnicity, sex, age_group) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  rename(SEX=sex,ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_age_sex=n)



pop_ethnicity_mapped_sex_only = pop_ethnicity_mapped %>%
  group_by(SEX) %>%
  summarise(n=sum(pop_ethnicity_age_sex)) %>%
  rename(pop_ethnicity_sex_only=n)

pop_ethnicity_mapped_age_group_only = pop_ethnicity_mapped %>%
  group_by(age_group) %>%
  summarise(n=sum(pop_ethnicity_age_sex)) %>%
  rename(pop_ethnicity_age_group_only=n)

pop_ethnicity_mapped_ethnicity_only = pop_ethnicity_mapped %>%
  group_by(ETHNIC_DESC_DETAIL2) %>%
  summarise(n=sum(pop_ethnicity_age_sex)) %>%
  rename(pop_ethnicity_ethnicity_only=n)


ethnicity_population_mapping_high = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Chinese",
"Any other White background",
"British",
"Irish"
),

project_high = c(
"Asian or Asian British",
"Asian or Asian British",
"Asian or Asian British",
"Asian or Asian British",
"Black or Black British",
"Black or Black British",
"Black or Black British",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Other ethnic groups",
"Other ethnic groups",
"White",
"White",
"White"
)

)


pop_ethnicity_mapped_ethnicity_high_only = 
pop_ethnicity_mapped_ethnicity_only %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high) %>%
  summarise(n=sum(pop_ethnicity_ethnicity_only)) %>%
  rename(pop_ethnicity_ethnicity_high_only=n)
  


main_cohort_final_first_new2 = 
main_cohort_final_first_new1 %>%
  left_join(pop_ethnicity_mapped, by=c("SEX","age_group","ETHNIC_DESC_DETAIL2")) %>%
  left_join(pop_ethnicity_mapped_sex_only, by=c("SEX")) %>%
  left_join(pop_ethnicity_mapped_age_group_only, by=c("age_group")) %>%
  left_join(pop_ethnicity_mapped_ethnicity_only, by=c("ETHNIC_DESC_DETAIL2")) %>%
  left_join(pop_ethnicity_mapped_ethnicity_high_only, by=c("ETHNIC_DESC_HIGH_ONLY"="project_high")) %>%
  rename(pop_ethnicity_ethnicity_high_only_ETHNIC_DESC_HIGH_ONLY = pop_ethnicity_ethnicity_high_only) %>%
  left_join(pop_ethnicity_mapped_ethnicity_high_only, by=c("ETHNIC_DESC_DETAIL_HIGH2"="project_high")) %>%
  rename(pop_ethnicity_ethnicity_high_only_ETHNIC_DESC_DETAIL_HIGH2 = pop_ethnicity_ethnicity_high_only)



main_cohort_final_first_new2 %>% ungroup() %>% write_rds("tmp/main_cohort_final_first_new2.rds")



```



```{r, cohort_new2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


main_cohort_final_first_new2 = read_rds("tmp/main_cohort_final_first_new2.rds")

```

```{r, kpc_working_pop, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs_pop = main_cohort_final_first_new2 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,SEX,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01"))) %>%
  left_join(pop_ethnicity_mapped_sex_only) %>%
  filter(SEX%in%c("Female","Male")) %>%
  mutate(count=(count/pop_ethnicity_sex_only)*100000)


full_date_df_kpcs_pop %>% write_rds("tmp/kpcs_time_sex_pop.rds")


```



```{r, sexPlotPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Sex coverage using population (2021) denominator"}

max_date_as_at = "2024-01"

full_date_df = read_rds("tmp/kpcs_time_sex_pop.rds") %>% ungroup()

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = SEX, color=SEX)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals,\nper 100,000 population"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
        legend.position = c(0.02, 0.85),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22", "Not Specified (Indeterminate)"="blue",
                                           "Unknown"="grey"))

```








```{r, age_working_pop, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs_pop = main_cohort_final_first_new2 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,age_group,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01"))) %>%
  left_join(pop_ethnicity_mapped_age_group_only) %>%
  mutate(count=(count/pop_ethnicity_age_group_only)*100000) %>%
  filter(age_group!="Unknown")


full_date_df_kpcs_pop %>% write_rds("tmp/kpcs_time_age_pop.rds")


```



```{r, agePlotPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Age coverage using population (2021) denominator"}

max_date_as_at = "2024-01"

full_date_df = read_rds("tmp/kpcs_time_age_pop.rds") %>% ungroup()


full_date_df$age_group <- 
  factor(full_date_df$age_group,
         levels=c("18-24",
                  "25-29",
                  "30-34",
                  "35-39",
                  "40-44",
                  "45-49",
                  "50-54",
                  "55-59",
                  "60-64",
                  "65-69",
                  "70-74",
                  "75-79",
                  "80-84",
                  "85+",
                  "Unknown"))

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 




ggplot((full_date_df
          ),
       aes(x = year_month_label, y = count, group = age_group, color=age_group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals,\nper 100,000 population"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
    legend.title=element_blank(),
        legend.position = c(0.02, 0.85),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1))
```




```{r, eth_working_pop, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_kpcs_pop = main_cohort_final_first_new2 %>%
  select(PERSON_ID,OPERATION_DATE,SEX,ETHNIC_DESC_HIGH_ONLY, ETHNIC_DESC_DETAIL2,age,age_group,
         DECILE,QUINTILE) %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  
  group_by(year_month,ETHNIC_DESC_HIGH_ONLY,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01"))) %>%
  left_join(pop_ethnicity_mapped_ethnicity_high_only,by=c("ETHNIC_DESC_HIGH_ONLY"="project_high")) %>%
  mutate(count=(count/pop_ethnicity_ethnicity_high_only)*100000) %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown")


full_date_df_kpcs_pop %>% write_rds("tmp/kpcs_time_eth_pop.rds")


```



```{r, ethPlotPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="KPC Ethnicity coverage using population (2021) denominator"}

max_date_as_at = "2024-01"

full_date_df = read_rds("tmp/kpcs_time_eth_pop.rds") %>% ungroup()



#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))



label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 



custom_colors <- c("#FF042E", "#3CB08C", "#FF5100", "#7346CB", "#FFCD16", "grey") #"#B4E4EA"


ggplot((full_date_df %>%
          mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
                                              levels=c(
                                                "Asian or Asian British",
                                                "Black or Black British",
                                                "Mixed or Multiple ethnic groups",
                                                "Other ethnic groups",
                                                "White",
                                                "Unknown"
                                              )))
          ),
       aes(x = year_month_label, y = count, group = ETHNIC_DESC_HIGH_ONLY, color=ETHNIC_DESC_HIGH_ONLY)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number of individuals,\nper 100,000 population"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
    legend.title=element_blank(),
        legend.position = c(0.02, 0.85),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100"))


```



# Supplementary Cohort - Linkage


## Audit Coverage

We know that there is a significant lag in coverage reporting for the NICOR audits.

Furthermore, it should be noted that coverage begins for these audits in different years (2013 for NACSA, 2018 for TAVI).

Figure \@ref(fig:auditsLag) shows this drop in coverage towards the end of 2022. For this reason an end date has been set such that this linkage exercise only compares the HES APC cohort to the audits between 2013-01 and 2022-08 for NACSA and 2018-01 and 2022-08 for TAVI. We expect a refresh of the NICOR audits in the coming months such that this end date could be extended into 2023.

Note that the linkage exercise has only considered a persons first surgery.



```{r, audit_date, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

audit_end_date = "2022-09-01"

```

```{r, prepare_linkage, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# HES filtered from 2013-01 to 2022-08 (as this when audit lags)
# HES also filtered to first procedures


hes_cohort_savr = main_cohort_final_first_new2 %>%
  filter(surgery_type=="savr") %>%
  #filter(overall_no_procedures==1) %>%
  filter(OPERATION_DATE<audit_end_date) %>%
  filter(OPERATION_DATE>="2013-01-01")

hes_cohort_tavi = main_cohort_final_first_new2 %>%
  filter(surgery_type=="tavi") %>%
  #filter(overall_no_procedures==1) %>%
  filter(OPERATION_DATE<audit_end_date) %>%
  filter(OPERATION_DATE>="2018-01-01")

nacsa_prepared = acs %>%
  rename(PERSON_ID=PERSON_ID_DEID) %>%
  filter(!is.na(PERSON_ID)) %>%
  mutate(DATE_AND_TIME_OF_OPERATION=as.Date(substr(DATE_AND_TIME_OF_OPERATION,1,10)))
  #filter(DATE_AND_TIME_OF_OPERATION<audit_end_date)

nacsa_prepared_savr = nacsa_prepared %>%
  filter(AORTIC_VALVE_HAEMODYN_PATH%in%c("1. Stenosis","3. Mixed"))%>%
  filter(AORTIC_VALVE_PROCEDURE%in%c("1. Replacement",
                                     "4. Repair without ring",
                                     "3. Repair with ring"
                                     ))

tavi_prepared = tavi %>%
  rename(PERSON_ID=PERSON_ID_DEID,DATE_AND_TIME_OF_OPERATION=`7_01_DATE_AND_TIME_OF_OPERATION`) %>%
  filter(!is.na(PERSON_ID)) %>%
  mutate(DATE_AND_TIME_OF_OPERATION=as.Date(substr(DATE_AND_TIME_OF_OPERATION,1,10)))
  #filter(DATE_AND_TIME_OF_OPERATION<audit_end_date)

```


```{r, auditsLag_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

full_date_df_nacsa = nacsa_prepared %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  mutate(DATE_AND_TIME_OF_OPERATION_MONTH = month(DATE_AND_TIME_OF_OPERATION),
         DATE_AND_TIME_OF_OPERATION_YEAR = year(DATE_AND_TIME_OF_OPERATION)) %>%
  mutate(year_month = paste(DATE_AND_TIME_OF_OPERATION_YEAR, DATE_AND_TIME_OF_OPERATION_MONTH, sep = "-")) %>%
  group_by(year_month,DATE_AND_TIME_OF_OPERATION_MONTH,DATE_AND_TIME_OF_OPERATION_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01"))) %>%
  mutate(group="nacsa")

full_date_df_tavi = tavi_prepared %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  mutate(DATE_AND_TIME_OF_OPERATION_MONTH = month(DATE_AND_TIME_OF_OPERATION),
         DATE_AND_TIME_OF_OPERATION_YEAR = year(DATE_AND_TIME_OF_OPERATION)) %>%
  mutate(year_month = paste(DATE_AND_TIME_OF_OPERATION_YEAR, DATE_AND_TIME_OF_OPERATION_MONTH, sep = "-")) %>%
  group_by(year_month,DATE_AND_TIME_OF_OPERATION_MONTH,DATE_AND_TIME_OF_OPERATION_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01"))) %>%
  mutate(group="tavi")

full_date_df = full_date_df_nacsa %>% bind_rows(full_date_df_tavi) %>% ungroup()

full_date_df %>% write_rds("tmp/audits_lag.rds")


```



```{r, auditsLag, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="Coverage of NACSA (all procedures) and TAVI Audits"}

full_date_df = read_rds("tmp/audits_lag.rds")

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label)) %>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))




ggplot((full_date_df%>%
          mutate(height=ifelse(group=="nacsa",3000,600))%>%
          mutate(group=ifelse(group=="nacsa","NACSA Audit","TAVI Audit"))
          ),
       aes(x = year_month_label, y = count, group = group, color=group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=c("2013-01","2014-01","2015-01","2016-01","2017-01","2018-01","2019-01","2020-01","2021-01","2022-01","2023-01"),labels=c(seq(2013,2023,1))) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.position = "none",
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("TAVI Audit" = tavi_colour, "NACSA Audit" = savr_colour)) +
  facet_wrap(group ~ ., scales='free', ncol= 1) +
  
  geom_vline(xintercept = "2022-08", linetype = "dashed", color = "#DC51E8") +
  geom_text(aes(x = "2022-08", y = height, label = "End Date"), size = 3, col = "#DC51E8", stat = "identity")
  #geom_text(aes(x = "2022-08", y=200, label = "End date"), hjust = -0.05, color = "#DC51E8") 


```





## HES to Audits

Here, we compare what linkage is available from the HES APC cohort to the audits. 

That is, how many of the HES APC cohort (procedures between 2013-01 to 2022-08) can be found in NACSA and how many of the HES APC cohort (procedures between 2018-01 to 2022-08) can be found in TAVI.

This will allow us to identify a false positive rate in HES APC and also begin to assess the quality of data in the audits.


### SAVR

Note that the NACSA audit contains data on all major heart operations, not just aortic valve procedures.

We have linked persons in the HES APC cohort to NACSA in general but also those identified as having SAVR in NACSA.

**How is one identified as having SAVR for AS in NACSA?**

The NICOR data dictionary states the following:

"There have been considerable problems with identifying if a valve has been operted upon.  Users have entered valve pathology even if the valve was not operated upon.  There is no Aortic/Mitral/Tricuspid or Pulmoary tickbox to select and thus SCTS has deemed a valve to be operated upon if procedure and pathology have been entered.  However there are no compulsory fields and so confusion has arisen with partial or incomplete entries.  Many software products do have an option to indicate which valve is operated upon and this is used to populate the relevant NICOR fields but is not in itself submitted to NICOR."

Thus we identify a case in NACSA as SAVR if:

* `AORTIC_VALVE_HAEMODYN_PATH` = Stenosis or Mixed **AND**
* `AORTIC_VALVE_PROCEDURE` = Replacement or Repair (with or without ring)


Note that we have included linkage to repair codes as our OPCS SAVR codelist includes replacements **and** repairs.

#### Insights

* As seen in Table \@ref(tab:linkageTable), 92.5% of the HES APC cohort can be found in NACSA, matching on `PERSON_ID` alone. If we match on `PERSON_ID` and `date of operation` this drops to 90.6%; however, matching on `date of operation` within 28 days brings the linkage up to 91.0%.

* The above was linking to the NACSA audit in general. When we link to the audit in which a person is identified as having SAVR for AS (e.g. `AORTIC_VALVE_HAEMODYN_PATH` = Stenosis | Mixed AND `AORTIC_VALVE_PROCEDURE` = Replacement | Repair (with or without ring)), the linkage drops across all matching methods. When matching on `PERSON_ID` and `date of operation` within 28 days, linkage drops from 91.0% to 80.6%. That is, 10.4% of the HES APC cohort have been found in NACSA but not for SAVR with AS.

**Given the methodology used to capture a TAVI case in HES APC (code combination found within 28 day washout period) we will take forward linkage between HES and the audits to be within 28 days too.**


To understand the above differences, Table \@ref(tab:missingSAVR) shows what these missing cases **are** being linked to in NACSA. 

* The largest group are cases who have been identified as having SAVR (replacement) but they had no pathology completed.

* The next largest group are cases who have been identified as having SAVR but for `Regurgitation` instead of AS. That is, 2,470 people in our cohort have been audited for having SAVR for regurgitation instead of stenosis.


* The group in which `AORTIC_VALVE_HAEMODYN_PATH` and `AORTIC_VALVE_PROCEDURE` are missing are cases that appear in the audit but for another major heart operation.




```{r, hes_to_nacsa, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


################################################################################
# PERSON
################################################################################

# will be the same when matching on date too because using first procedures only
hes_count_savr = hes_cohort_savr %>% nrow()

hes_count_savr_person = hes_cohort_savr %>% distinct(PERSON_ID) %>% nrow()


# HES to NACSA
hes_to_nacsa_persons = 
hes_cohort_savr %>%
  ungroup() %>%
  mutate(bin = ifelse(PERSON_ID%in%(nacsa_prepared%>%select(PERSON_ID)%>%pull()),1,0)) %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA")



# HES to NACSA SAVR
hes_to_nacsa_savr_persons = 
hes_cohort_savr %>%
  mutate(bin = ifelse(PERSON_ID%in%(nacsa_prepared_savr%>%select(PERSON_ID)%>%pull()),1,0)) %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA SAVR")




################################################################################
# PERSON and OPERATION_DATE
################################################################################

hes_to_nacsa_persons_date = 
hes_cohort_savr %>%
  left_join((nacsa_prepared%>%select(PERSON_ID,OPERATION_DATE=DATE_AND_TIME_OF_OPERATION)%>%mutate(bin=1)),
            by = join_by('PERSON_ID', 'OPERATION_DATE')) %>%
  mutate(bin = ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA")


hes_to_nacsa_savr_persons_date = 
hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(PERSON_ID,OPERATION_DATE=DATE_AND_TIME_OF_OPERATION)%>%mutate(bin=1)),
            by = join_by('PERSON_ID', 'OPERATION_DATE')) %>%
  mutate(bin = ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA SAVR")


################################################################################
# PERSON and OPERATION_DATE within 7 days
################################################################################

hes_to_nacsa_persons_date_7 = 
hes_cohort_savr %>%
  left_join((nacsa_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA")



hes_to_nacsa_savr_persons_date_7 = 
hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA SAVR")



################################################################################
# PERSON and OPERATION_DATE within 28 days
################################################################################

hes_to_nacsa_persons_date_28 = 
hes_cohort_savr %>%
  left_join((nacsa_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_28 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_28 = ifelse(DATE_28<0,DATE_28*-1,DATE_28)) %>%
  mutate(bin=ifelse(DATE_28<=28,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA")


hes_to_nacsa_savr_persons_date_28 = 
hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_28 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_28 = ifelse(DATE_28<0,DATE_28*-1,DATE_28)) %>%
  mutate(bin=ifelse(DATE_28<=28,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to NACSA SAVR")



```



```{r, hes_to_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


################################################################################
# PERSON
################################################################################

# will be the same when matching on date too because using first procedures only
hes_count_tavi = hes_cohort_tavi %>% nrow()

hes_count_tavi_person = hes_cohort_tavi %>% distinct(PERSON_ID) %>% nrow()


# HES to TAVI
hes_to_tavi_persons = 
hes_cohort_tavi %>%
  mutate(bin = ifelse(PERSON_ID%in%(tavi_prepared%>%select(PERSON_ID)%>%pull()),1,0)) %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to TAVI")



################################################################################
# PERSON and OPERATION_DATE
################################################################################

hes_to_tavi_persons_date = 
hes_cohort_tavi %>%
  left_join((tavi_prepared%>%select(PERSON_ID,OPERATION_DATE=DATE_AND_TIME_OF_OPERATION)%>%mutate(bin=1)),
            by = join_by('PERSON_ID', 'OPERATION_DATE')) %>%
  mutate(bin = ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to TAVI")



################################################################################
# PERSON and OPERATION_DATE within 7 days
################################################################################

hes_to_tavi_persons_date_7 = 
hes_cohort_tavi %>%
  left_join((tavi_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to TAVI")





################################################################################
# PERSON and OPERATION_DATE within 28 days
################################################################################

hes_to_tavi_persons_date_28 = 
hes_cohort_tavi %>%
  left_join((tavi_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_28 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_28 = ifelse(DATE_28<0,DATE_28*-1,DATE_28)) %>%
  mutate(bin=ifelse(DATE_28<=28,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  select(PERSON_ID, bin, OPERATION_DATE) %>%
  distinct() %>%
  count(bin) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  filter(bin==1) %>%
  mutate(bin="HES to TAVI")



```

```{r, linkage_results, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


linkage_hes_to_savr = 
data.frame(bin="HES",
           person_n=hes_count_savr,person_pct="100.0",
           person_date_n=hes_count_savr,person_date_pct="100.0",
           person_date_7_n=hes_count_savr,person_date_7_pct="100.0",
           person_date_28_n=hes_count_savr,person_date_28_pct="100.0"
           ) %>%
  bind_rows(
    (hes_to_nacsa_persons%>%rename(person_n=n,person_pct=pct) %>%
       left_join(hes_to_nacsa_persons_date%>%rename(person_date_n=n,person_date_pct=pct)) %>%
       left_join(hes_to_nacsa_persons_date_7%>%rename(person_date_7_n=n,person_date_7_pct=pct)) %>%
       left_join(hes_to_nacsa_persons_date_28%>%rename(person_date_28_n=n,person_date_28_pct=pct)) %>%
       mutate(across(contains("pct"), as.character))
     )) %>% bind_rows(
       (hes_to_nacsa_savr_persons%>%rename(person_n=n,person_pct=pct) %>%
          left_join(hes_to_nacsa_savr_persons_date%>%rename(person_date_n=n,person_date_pct=pct)) %>%
          left_join(hes_to_nacsa_savr_persons_date_7%>%rename(person_date_7_n=n,person_date_7_pct=pct)) %>%
          left_join(hes_to_nacsa_savr_persons_date_28%>%rename(person_date_28_n=n,person_date_28_pct=pct))) %>%
         mutate(across(contains("pct"), as.character))
       )



linkage_hes_to_tavi = 
data.frame(bin="HES",
           person_n=hes_count_tavi,person_pct="100.0",
           person_date_n=hes_count_tavi,person_date_pct="100.0",
           person_date_7_n=hes_count_tavi,person_date_7_pct="100.0",
           person_date_28_n=hes_count_tavi,person_date_28_pct="100.0"
           ) %>%
  bind_rows(
    (hes_to_tavi_persons%>%rename(person_n=n,person_pct=pct) %>%
       left_join(hes_to_tavi_persons_date%>%rename(person_date_n=n,person_date_pct=pct)) %>%
       left_join(hes_to_tavi_persons_date_7%>%rename(person_date_7_n=n,person_date_7_pct=pct)) %>%
       left_join(hes_to_tavi_persons_date_28%>%rename(person_date_28_n=n,person_date_28_pct=pct)) %>%
       mutate(across(contains("pct"), as.character))
     ))




linkage_hes_to_savr %>% write_rds("tmp/linkage_hes_to_savr.rds")
linkage_hes_to_tavi %>% write_rds("tmp/linkage_hes_to_tavi.rds")

```


```{r, linkageTable, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


linkage_hes_to_savr = read_rds("tmp/linkage_hes_to_savr.rds")
linkage_hes_to_tavi = read_rds("tmp/linkage_hes_to_tavi.rds")

linkage_savr_kable = 
linkage_hes_to_savr %>%
  #apply SDC
  mutate(person_n = case_when(person_n < 10 ~ 10, TRUE ~ round(person_n / 5) * 5)) %>%
  mutate(person_date_n = case_when(person_date_n < 10 ~ 10, TRUE ~ round(person_date_n / 5) * 5)) %>%
  mutate(person_date_7_n = case_when(person_date_7_n < 10 ~ 10, TRUE ~ round(person_date_7_n / 5) * 5)) %>%
  mutate(person_date_28_n = case_when(person_date_28_n < 10 ~ 10, TRUE ~ round(person_date_28_n / 5) * 5)) %>%
  mutate(across(contains("_n"), ~format(., big.mark = ","))) %>%
  mutate(person=paste0(person_n," (",person_pct, "%)")) %>%
  mutate(person_date=paste0(person_date_n," (",person_date_pct, "%)")) %>%
  mutate(person_date_7=paste0(person_date_7_n," (",person_date_7_pct, "%)")) %>%
  mutate(person_date_28=paste0(person_date_28_n," (",person_date_28_pct, "%)")) %>%
  select(" "=bin, person,person_date,person_date_7,person_date_28) %>%
  rename(Person=person,`Person + Date`=person_date, `Person + Date within 7 days`=person_date_7,
         `Person + Date within 28 days`=person_date_28)


kableExtra::kbl(
  linkage_savr_kable,
    booktabs = T, 
    caption=("Linkage from HES cohort (procedures between 2013-01 and 2022-08) to NACSA Audit<br> SDC applied"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```



```{r, missing, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

in_nacsa = 
hes_cohort_savr %>%
  left_join((nacsa_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)%>%distinct()),
            by = join_by('PERSON_ID')) %>%
  filter(!is.na(DATE_AND_TIME_OF_OPERATION)) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  #mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  filter(bin==1) %>%
  distinct() %>%
  select(-bin) %>% 
  select(PERSON_ID,OPERATION_DATE) %>%
  distinct()


not_in_savr = hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)%>%distinct()),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  distinct() %>%
  filter(bin==1) %>%
  select(-bin) %>%
  select(PERSON_ID,OPERATION_DATE) %>%
  distinct()


savr_missing = setdiff((in_nacsa),(not_in_savr)) %>%
  left_join(nacsa_prepared) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  filter(bin==1) %>% group_by(PERSON_ID) %>% mutate(count=n()) %>% ungroup() %>%
  select(PERSON_ID,OPERATION_DATE,DATE_AND_TIME_OF_OPERATION, AORTIC_VALVE_HAEMODYN_PATH,AORTIC_VALVE_PROCEDURE, count) %>%
  distinct()



savr_missing %>% write_rds("tmp/savr_missing.rds")

```


```{r, missingSAVR, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


# Note some duplication due to duplication in the audits (i.e. a person has more than 1 record in the audit as at the same `date of operation`).

savr_missing = read_rds("tmp/savr_missing.rds")

savr_missing_table = savr_missing %>%
  count(AORTIC_VALVE_HAEMODYN_PATH,AORTIC_VALVE_PROCEDURE) %>%
  arrange(desc(n)) %>%
  mutate(pct=round(n/sum(n)*100,1)) %>%
  mutate(n = case_when(n < 10 ~ 10, TRUE ~ round(n / 5) * 5)) %>%
  mutate(n = format(n, big.mark = ",")) %>%
  rename(N=n,PCT=pct) %>%
  mutate(AORTIC_VALVE_HAEMODYN_PATH=ifelse(is.na(AORTIC_VALVE_HAEMODYN_PATH),"NA",AORTIC_VALVE_HAEMODYN_PATH)) %>%
  mutate(AORTIC_VALVE_PROCEDURE=ifelse(is.na(AORTIC_VALVE_PROCEDURE),"NA",AORTIC_VALVE_PROCEDURE)) %>%
  mutate(AORTIC_VALVE_HAEMODYN_PATH=case_when(
    AORTIC_VALVE_HAEMODYN_PATH == "1. Stenosis" ~ "Stenosis",
    AORTIC_VALVE_HAEMODYN_PATH =="2. Regurgitation" ~ "Regurgitation",
    AORTIC_VALVE_HAEMODYN_PATH =="3. Mixed" ~ "Mixed",
    is.na(AORTIC_VALVE_HAEMODYN_PATH) ~  "NA"
  )) %>%
  mutate(AORTIC_VALVE_PROCEDURE=case_when(
    AORTIC_VALVE_PROCEDURE == "1. Replacement" ~ "Replacement",
    AORTIC_VALVE_PROCEDURE =="3. Repair with ring" ~ "Repair with ring",
    AORTIC_VALVE_PROCEDURE =="4. Repair without ring" ~ "Repair without ring",
    AORTIC_VALVE_PROCEDURE =="5. Isolated commisurotomy" ~ "Isolated commisurotomy",
    AORTIC_VALVE_PROCEDURE =="7. Inspection" ~ "Inspection",
    is.na(AORTIC_VALVE_PROCEDURE) ~  "NA"
  ))



kableExtra::kbl(
  savr_missing_table,
    booktabs = T, 
    caption=("Persons from HES cohort who have been identified in NACSA but not for SAVR with AS pathology <br> SDC applied"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )




```


### TAVI


```{r, linkageTableTavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


linkage_tavi_kable = 
linkage_hes_to_tavi %>%
  #apply SDC
  mutate(person_n = case_when(person_n < 10 ~ 10, TRUE ~ round(person_n / 5) * 5)) %>%
  mutate(person_date_n = case_when(person_date_n < 10 ~ 10, TRUE ~ round(person_date_n / 5) * 5)) %>%
  mutate(person_date_7_n = case_when(person_date_7_n < 10 ~ 10, TRUE ~ round(person_date_7_n / 5) * 5)) %>%
  mutate(person_date_28_n = case_when(person_date_28_n < 10 ~ 10, TRUE ~ round(person_date_28_n / 5) * 5)) %>%
  mutate(across(contains("_n"), ~format(., big.mark = ","))) %>%
  mutate(person=paste0(person_n," (",person_pct, "%)")) %>%
  mutate(person_date=paste0(person_date_n," (",person_date_pct, "%)")) %>%
  mutate(person_date_7=paste0(person_date_7_n," (",person_date_7_pct, "%)")) %>%
  mutate(person_date_28=paste0(person_date_28_n," (",person_date_28_pct, "%)")) %>%
  select(" "=bin, person,person_date,person_date_7,person_date_28) %>%
  rename(Person=person,`Person + Date`=person_date, `Person + Date within 7 days`=person_date_7,
         `Person + Date within 28 days`=person_date_28)


kableExtra::kbl(
  linkage_tavi_kable,
    booktabs = T, 
    caption=("Linkage from HES cohort (procedures between 2018-01 and 2022-08) to TAVI Audit<br> SDC applied"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```

#### Assumptions

Note that the `aortic valve pathology` field has **not been provisioned** in the TAVI audit. Thus for the following linkage exercise we are linking to all TAVI events in the audit which will include procedures for which the pathology is different to `Stenosis`. If this field cannot be provisioned we will use HES APC to confirm pathology.

#### Insights

* As seen in Table \@ref(tab:linkageTableTavi), 86.6% of the HES APC cohort can be found in TAVI, matching on `PERSON_ID` alone. Matching on `date of operation` within 28 days, as per the methodology defined above, brings the linkage to 86.0%. 


# Adhoc - Age Distribution Check


```{r, save, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


main_cohort_final_first_new2 = main_cohort_final_first_new2 %>%
    select(-pop_ethnicity_age_sex,
         -pop_ethnicity_sex_only,
         -pop_ethnicity_age_group_only,
         -pop_ethnicity_ethnicity_only,
         -pop_ethnicity_ethnicity_high_only_ETHNIC_DESC_HIGH_ONLY,
         -pop_ethnicity_ethnicity_high_only_ETHNIC_DESC_DETAIL_HIGH2
         )

main_cohort_final_first_new2 %>% ungroup() %>% write_rds("tmp/main_cohort_final_first_new2.rds")

```




```{r, sen_analysis, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# 3321 LSOAs outside of England (now 2946 as replaced 375)
main_cohort_final_first_new2 %>%
  filter(!str_starts(LSOA,"E|Unknown")) %>%
  nrow()

# 499 DOBs with DOB either 1800-01-01 or 1801-01-01
main_cohort_final_first_new2 %>%
  filter(DOB <="1801-01-01") %>%
  nrow()

# Now see Databricks CCU056-D07c-KPC_sensitivity_analysis


```


```{r, ageDist, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7}

analysis_cohort = read_rds("tmp/main_cohort_final_first_new2.rds")

analysis_cohort = analysis_cohort %>%
  mutate(age=ifelse(DOB=="1800-01-01",NA,age)) %>%
  mutate(age=ifelse(DOB=="1801-01-01",NA,age)) %>%
  mutate(age_group=ifelse(DOB=="1800-01-01","Unknown",age_group)) %>%
  mutate(age_group=ifelse(DOB=="1801-01-01","Unknown",age_group))
  
  
analysis_cohort %>% ungroup() %>% write_rds("tmp/main_cohort_final_first_new2.rds")

age_dist = analysis_cohort %>%
ggplot(aes(x = age)) +
  geom_histogram(binwidth = 2, fill = "#E9DDFF", color = "#985DFF") +
  labs(title = "Age Distribution", x = "Age", y = "Frequency")


age_dist
#ggsave("Plots/age_dist.png")

```
