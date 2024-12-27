---
title: "CCU056 Pipeline"
subtitle: "Creating a research ready dataset - 03"
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

main_cohort_final_first = read_rds("tmp/main_cohort_final_first_new2.rds")

outcomes_hospitalisations <- dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_hospitalisations")))
outcomes_deaths <- dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_deaths")))


covariates = dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_covariates_1st_wide_icd_only")))

covariates_codelist = dbGetQuery(con,sprintf(glue(select_all_proj,"out_codelist_covariates")))


```

```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


```



```{r}

# seeing how LSOAs change if mapping to most appropriate year

lsoa_2011_imd = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_2011_imd_lookup")))

compare_df = main_cohort_final_first %>%
  select(PERSON_ID,LSOA,DECILE,QUINTILE) %>%
  left_join(lsoa_2011_imd,by=c("LSOA"="LSOA_2011")) %>%
  filter(DECILE!="Unknown") %>%
  filter(!is.na(IMD_2019_DECILES)) 
  

compare_df %>%
  filter(IMD_2019_QUINTILES!=QUINTILE)



```


# Covariates

```{r, covariatesWorking, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


covariate_groups = tibble(
  
name=c(
"cov_ami_flag",			
"cov_angina_flag",			
"cov_arrythmias_heart_failure_flag",				
"cov_asthma_flag",
"cov_atherosclerosis_flag",				
"cov_cerebrovascular_disease_flag",				
"cov_ckd_flag",				
"cov_copd_flag",			
"cov_diabetes_flag",			
"cov_hf_flag",
"cov_hypertension_flag",				
"cov_hypertensive_disease_flag",			
"cov_ischaemic_heart_disease_flag",				
"cov_pad_flag",			
"cov_stroke_hs_flag",			
"cov_stroke_is_flag",				
"cov_stroke_nos_flag",				
"cov_stroke_sah_flag",				
"cov_stroke_tia_flag",				
"cov_sudden_death_flag",
"cov_unstable_angina_flag"
),

group1=c(
"ami",			
"angina",			
"NA",				
"asthma",
"NA",				
"NA",				
"ckd",				
"copd",			
"diabetes",			
"heart_failure",
"hypertension",				
"NA",			
"NA",				
"pad",			
"stroke_hs",			
"stroke_is",				
"stroke_nos",				
"stroke_sah",				
"stroke_tia",				
"NA",
"unstable_angina"
),


group2=c(
"coronary_artery_disease",			
"coronary_artery_disease",			
"NA",				
"asthma",
"NA",				
"NA",				
"ckd",				
"copd",			
"diabetes",			
"heart_failure",
"hypertension",				
"NA",			
"NA",				
"pad",			
"cerebrovascular_disease",			
"cerebrovascular_disease",				
"cerebrovascular_disease",				
"cerebrovascular_disease",				
"cerebrovascular_disease",				
"NA",
"coronary_artery_disease"
),



group3=c(
"cardiovascular_risk_factors",			
"cardiovascular_risk_factors",			
"NA",				
"comorbidities",
"NA",				
"NA",				
"comorbidities",				
"comorbidities",			
"cardiovascular_risk_factors",			
"comorbidities",
"cardiovascular_risk_factors",				
"NA",			
"NA",				
"comorbidities",			
"comorbidities",			
"comorbidities",				
"comorbidities",				
"comorbidities",				
"comorbidities",				
"NA",
"cardiovascular_risk_factors"
)


)



person_covariates = covariates %>%
  select(-CENSOR_DATE_START,-CENSOR_DATE_END) %>%
  select(PERSON_ID,ends_with("flag")) %>%
  pivot_longer(cols=-c(PERSON_ID)) %>%
  left_join((covariate_groups%>%select(name,group1))) %>%
  filter(group1!="NA") %>%
  select(-name) %>%
  pivot_wider(names_from=group1,values_from=value) %>%
  replace(is.na(.), 0) %>%
  
  mutate(asthma_copd = asthma+copd) %>%
  mutate(asthma_copd = ifelse(asthma_copd==2,1,asthma_copd)) %>%
  select(-asthma,-copd) %>%
  
  mutate(cerebrovascular_disease = stroke_hs+stroke_is+stroke_nos+stroke_sah+stroke_tia) %>%
  mutate(cerebrovascular_disease = ifelse(cerebrovascular_disease>1,1,cerebrovascular_disease)) %>%
  select(-stroke_hs,-stroke_is,-stroke_nos,-stroke_sah,-stroke_tia) %>%
  
  mutate(coronary_artery_disease = ami+angina+unstable_angina) %>%
  mutate(coronary_artery_disease = ifelse(coronary_artery_disease>1,1,coronary_artery_disease)) %>%
  select(-ami,-angina,-unstable_angina) %>%
  
  mutate(across(everything(),as.character))
  
  # mutate(across(-c(PERSON_ID), ~as.logical(as.integer(.))))
  
  


main_cohort_final_first = main_cohort_final_first %>%
  left_join(person_covariates) %>%
  mutate(across(c("ckd","diabetes","heart_failure","hypertension","pad","asthma_copd",
                  "cerebrovascular_disease","coronary_artery_disease"), ~replace(., is.na(.), '0')))


```



# Outcomes

```{r}

main_cohort_final_first = main_cohort_final_first %>%
  left_join((outcomes_deaths%>%rename(DOD=DATE))) %>% left_join(outcomes_hospitalisations) %>%
  mutate(outcomes_death_description_high = ifelse(outcomes_death_description=="all_cause_death",
                                                  "all_cause_death",
                                           ifelse(is.na(outcomes_death_description),NA,
                                          "cvd_death"))) %>%
  mutate(outcomes_hosp_time_to_first = as.numeric(difftime(outcomes_hosp_first_date, OPERATION_DATE,
                                                             units = "days"))) %>%
  mutate(outcomes_time_to_death = as.numeric(difftime(DOD, OPERATION_DATE,
                                                             units = "days")))


```


# Final Cohort



```{r}

main_cohort_final_first = main_cohort_final_first %>%
  select(PERSON_ID,
         OPERATION_DATE,OPERATION_YEAR,treat,
         AS_EARLIST_DATE,procedure_as_number,overall_no_as_procedures,overall_no_procedures,
         surgery_type,
         QUINTILE,SEX,DOB,age,age_group,
         ETHNIC,ETHNIC_DESC_HIGH_ONLY,ETHNIC2,ETHNIC_DESC_DETAIL2,
         ckd,diabetes,heart_failure,hypertension,pad,asthma_copd,cerebrovascular_disease,coronary_artery_disease,
         DOD,outcomes_death_description,outcomes_death_description_high,
         outcomes_hosp_first_date,outcomes_hosp_admission_rate,
         outcomes_hosp_time_to_first,outcomes_time_to_death
         )


main_cohort_final_first %>% write_rds("tmp/main_cohort_final_first_new3.rds")

```



**Quality assurance rules**

* Rule 1: Year of birth is after the year of death
* Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
* Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date
* Rule 4: Remove those with only null/invalid dates of death
* Rule 5: Remove those where registered date of death before the actual date of death
* Rule 6: Pregnancy/birth codes for men
* Rule 7: HRT codes codes for men
* Rule 8: COCP codes codes for men
* Rule 9: Prostate cancer codes for women
* Rule 10: Patients have all missing record_dates and dates


