---
title: "CCU056 Analysis"
subtitle: "Analysis 04 - Extra Descriptive Statistics"
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---


```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}

#libraries
library(survminer)
library(tidyverse)
library(odbc)
library(DBI)
library(glue)
library(cowplot)
library(bookdown)
library(kableExtra)
library(survival)
library(finalfit)

library(lubridate)



#connect
#con <- DBI::dbConnect(odbc:::odbc(), "databricks")

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



```{r, hes_emergency, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

hes_full = dbGetQuery(con,sprintf(glue(select_all_proj,"hes_full_cohort")))



kableExtra::kbl(
  (hes_full %>% group_by(ADMIMETH) %>% count() %>%
  mutate(ADMIMETH = ifelse(is.na(ADMIMETH),"Unknown",ADMIMETH)) %>%
       #apply SDC
     mutate(n = case_when(
     n < 10 ~ 10,
     TRUE ~ round(n / 5) * 5
     )) %>%
     mutate(n=format(n, big.mark = ","))
     ),
    booktabs = T, 
    caption=("Full Cohort: HES ADMIMETH"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```

```{r, data, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}



analysis_cohort = read_rds("tmp/main_cohort_final_first_new4.rds") %>%
  mutate(ETHNIC_DESC_HIGH_ONLY = factor(ETHNIC_DESC_HIGH_ONLY, levels = c("White","Asian or Asian British","Black or Black British","Mixed or Multiple ethnic groups"))) %>%
  mutate(SEX=factor(SEX,levels=c("Male","Female")))

audit_end_date = "2022-09-01"

hes_cohort_savr = analysis_cohort %>%
  filter(surgery_type=="savr") %>%
  #filter(overall_no_procedures==1) %>%
  filter(OPERATION_DATE<audit_end_date) %>%
  filter(OPERATION_DATE>="2013-01-01")

hes_cohort_tavi = analysis_cohort %>%
  filter(surgery_type=="tavi") %>%
  #filter(overall_no_procedures==1) %>%
  filter(OPERATION_DATE<audit_end_date) %>%
  filter(OPERATION_DATE>="2018-01-01")


acs <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_acs")))

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

hes_to_nacsa_savr_persons_date_28 = 
hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(-SEX)),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_28 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_28 = ifelse(DATE_28<0,DATE_28*-1,DATE_28)) %>%
  mutate(bin=ifelse(DATE_28<=28,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>% #64456
  filter(bin==1) #52219



```



```{r, tableOne, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

library(table1)



hes_to_nacsa_savr_persons_date_28$SEX <- 
  factor(hes_to_nacsa_savr_persons_date_28$SEX, levels=c("Male", 
                  "Female",
                  #"Not Specified (Indeterminate)",
                  "Unknown"))
 
hes_to_nacsa_savr_persons_date_28$age_group <- 
  factor(hes_to_nacsa_savr_persons_date_28$age_group,
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

# hes_to_nacsa_savr_persons_date_28$treat <- 
#   factor(hes_to_nacsa_savr_persons_date_28$treat,
#          levels=c("PRIOR - Beginning", 
#                   "PRIOR - Middle",
#                   "PRIOR - End",
#                   "PANDEMIC - During"))

hes_to_nacsa_savr_persons_date_28$treat <- 
  factor(hes_to_nacsa_savr_persons_date_28$treat,
         levels=c("2000-2004","2005-2009","2010-2014","2015-2019","2020-2023"))



hes_to_nacsa_savr_persons_date_28$QUINTILE <- 
  factor(hes_to_nacsa_savr_persons_date_28$QUINTILE,
         levels=c("1","2","3","4","5","Unknown"))


# eth_desc = unique((names(sort(table(hes_to_nacsa_savr_persons_date_28$ETHNIC_DESC_HIGH_ONLY), decreasing = TRUE))))
# 
# 
# hes_to_nacsa_savr_persons_date_28$ETHNIC_DESC_HIGH_ONLY <-
# factor(hes_to_nacsa_savr_persons_date_28$ETHNIC_DESC_HIGH_ONLY, 
#        levels = as.factor(c(eth_desc[-which((eth_desc) == "Unknown")],"Unknown"))
#        )




label(hes_to_nacsa_savr_persons_date_28$SEX)       <- "Sex"
label(hes_to_nacsa_savr_persons_date_28$age)       <- "Age"
label(hes_to_nacsa_savr_persons_date_28$age_group)       <- "Age group"
label(hes_to_nacsa_savr_persons_date_28$ETHNIC_DESC_HIGH_ONLY)       <- "Ethnicity (High-level only)"


label(hes_to_nacsa_savr_persons_date_28$QUINTILE)       <- "IMD Quintiles"

units(hes_to_nacsa_savr_persons_date_28$age)       <- "years"
units(hes_to_nacsa_savr_persons_date_28$age_group)       <- "years"





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

hes_to_nacsa_savr_persons_date_28 = hes_to_nacsa_savr_persons_date_28 %>%
  mutate(CARDIOGENIC_SHOCK_PRE_OP=ifelse(is.na(CARDIOGENIC_SHOCK_PRE_OP),"Unknown",CARDIOGENIC_SHOCK_PRE_OP))

# table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + QUINTILE | CARDIOGENIC_SHOCK_PRE_OP, data=hes_to_nacsa_savr_persons_date_28,
#                render.categorical=my.render.cat, footnote=footnote, render.strat = my.render.strat,
#        render.missing = NULL #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
#        )


```

```{r, tableOneEjection, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

hes_to_nacsa_savr_persons_date_28 = hes_to_nacsa_savr_persons_date_28 %>%
  mutate(EJECTION_FRACTION_CATEGORY=ifelse(is.na(EJECTION_FRACTION_CATEGORY),"Unknown",EJECTION_FRACTION_CATEGORY))

table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + QUINTILE | EJECTION_FRACTION_CATEGORY, data=hes_to_nacsa_savr_persons_date_28,
               render.categorical=my.render.cat, footnote=footnote, render.strat = my.render.strat,
       render.missing = NULL #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
       )



```



```{r, tableOneOperative, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


hes_to_nacsa_savr_persons_date_28 = hes_to_nacsa_savr_persons_date_28 %>%
  mutate(OPERATIVE_URGENCY=ifelse(is.na(OPERATIVE_URGENCY),"Unknown",OPERATIVE_URGENCY))

table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + QUINTILE | OPERATIVE_URGENCY, data=hes_to_nacsa_savr_persons_date_28,
               render.categorical=my.render.cat, footnote=footnote, render.strat = my.render.strat,
       render.missing = NULL #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
       )





```
