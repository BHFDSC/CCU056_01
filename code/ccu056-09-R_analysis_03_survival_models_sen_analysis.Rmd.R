---
title: "CCU056 Analysis"
subtitle: "Analysis 03 - Surgery Outcomes - Survival Analysis"
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---


Sensitivity Analysis - Treatment period 2020-2023 now only includes those who had date of operation <= 2021
Currently the date of operations go to December 2022

```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}


# Cox Hazard Models for time to:
# 
# CVD Death
# Non CVD Death
# CVD or Non CVD Death (All Cause Death)
# 
# CVD Hospitalisation
# Non CVD Hospitalisation
# CVD or Non CVD Hospitalisation (All Cause Hospitalisation)
# 
# Models:
# 
# Unadjusted for age
# Adjusted for age
# 
# 
# Disease-specific survival: considering disease-specific death comparing alive/Non CVD Death with CVD Death
# 
# Questions:
# 
# What has been the impact of COVID on overall outcomes
# 
# Are there any differences in outcomes for different socio-economic groups



# install.packages("tidyverse", contriburl = 'file:///mnt/resources/rstudio/miniCranRepo/src/contrib', dependencies = TRUE)
#install.packages("vctrs", contriburl = 'file:///mnt/resources/rstudio/miniCranRepo/src/contrib', dependencies = TRUE)



#libraries
#library(survminer)
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
library(broom)
library(cli)

library(emmeans)


library(performance)
#library(tidycmprsk)

#detach("package:tidyverse", unload=TRUE)



file_list <- list.files(path = "ggsurvfit", pattern = "\\.R$", full.names = TRUE)
# Source each file in the list
for (file in file_list) {
  source(file)
}



file_list <- list.files(path = "gtsummary", pattern = "\\.R$", full.names = TRUE)
# Source each file in the list
for (file in file_list) {
  source(file)
}

file_list <- list.files(path = "tidycmprsk", pattern = "\\.R$", full.names = TRUE)
# Source each file in the list
for (file in file_list) {
  source(file)
}



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



```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, results="hide"}

study_start_date = "2020-01-01"


tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


analysis_cohort = read_rds("tmp/main_cohort_final_first_new4.rds") %>%
  mutate(ETHNIC_DESC_HIGH_ONLY = factor(ETHNIC_DESC_HIGH_ONLY, levels = c("White","Asian or Asian British","Black or Black British","Mixed or Multiple ethnic groups"))) %>%
  mutate(SEX=factor(SEX,levels=c("Male","Female")))


esp = dbGetQuery(con,sprintf(glue(select_all_proj,"esp")))
esp_groups = dbGetQuery(con,sprintf(glue(select_all_proj,"esp_groups")))


analysis_cohort = 
analysis_cohort %>%
  select(-DOD,-outcomes_death_description,-outcomes_death_description_high,
         -outcomes_hosp_first_date,-outcomes_hosp_admission_rate,
         -outcomes_hosp_time_to_first,-outcomes_time_to_death)


outcomes_deaths = dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_deaths")))
outcomes_hospitalisations_raw = dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_hospitalisations_raw")))
outcomes_hospitalisations_raw_cvd = dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_hospitalisations_raw_cvd")))

outcomes_deaths_prepared = outcomes_deaths %>%
  select(-CODE) %>%
  distinct()

# check no one has more than one DOD - both 86947
outcomes_deaths_prepared %>% distinct(PERSON_ID,DATE) %>%
  select(PERSON_ID) %>% count()

outcomes_deaths_prepared %>% distinct(PERSON_ID,DATE) %>% count()

# check no one has cvd and non cvd death
outcomes_deaths_prepared %>%
  distinct(PERSON_ID,outcomes_death_description) %>% select(PERSON_ID) %>% count()


# people who have DOD <DOB - 3 people - will remove
outcomes_deaths_prepared %>%
  filter(DATE<CENSOR_DATE_START)

outcomes_deaths_prepared %>% #10,000 after study end date - again remove for now
  filter(DATE>CENSOR_DATE_END)


outcomes_deaths_prepared = 
outcomes_deaths_prepared %>%
  filter(DATE>=CENSOR_DATE_START) %>%
  filter(DATE<=CENSOR_DATE_END) %>%
  select(PERSON_ID,DOD=DATE,outcomes_death_description_high=outcomes_death_description)


analysis_cohort = 
analysis_cohort %>% left_join(outcomes_deaths_prepared) %>%
  mutate(outcomes_time_to_death = as.numeric(DOD-OPERATION_DATE)) %>%
  filter(OPERATION_YEAR<=2021)




```


```{r, parametersPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

pop_lsoa_mapped_quintile_age = read_rds("tmp/pop_lsoa_mapped_quintile_age.rds") %>%
  mutate(QUINTILE=as.character(QUINTILE))
pop_lsoa_mapped_quintile_sex_age = read_rds("tmp/pop_lsoa_mapped_quintile_sex_age.rds") %>%
  mutate(QUINTILE=as.character(QUINTILE))
pop_lsoa_mapped_sex_age = read_rds("tmp/pop_lsoa_mapped_sex_age.rds")

pop_ethnicity_mapped_ethnicity_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_age.rds")
pop_ethnicity_mapped_sex_age = read_rds("tmp/pop_ethnicity_mapped_sex_age.rds")
pop_ethnicity_mapped_ethnicity_sex_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_sex_age.rds")
pop_ethnicity_mapped_ethnicity_high_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_age.rds")
pop_ethnicity_mapped_ethnicity_high_sex_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_sex_age.rds")


# Using the mid population denominator only
pop_lsoa_mapped_quintile_age = pop_lsoa_mapped_quintile_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(QUINTILE,age_group,YEAR) %>% summarise(pop_lsoa_var_quintile_age= sum(pop_lsoa_var_quintile_age))

pop_lsoa_mapped_quintile_sex_age = pop_lsoa_mapped_quintile_sex_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(QUINTILE,sex,age_group,YEAR) %>% summarise(pop_lsoa_var_quintile_sex_age= sum(pop_lsoa_var_quintile_sex_age))

pop_lsoa_mapped_sex_age = pop_lsoa_mapped_sex_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(sex,age_group,YEAR) %>% summarise(pop_lsoa_var_sex_age= sum(pop_lsoa_var_sex_age))

pop_ethnicity_mapped_ethnicity_age = pop_ethnicity_mapped_ethnicity_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_sex_age = pop_ethnicity_mapped_sex_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_sex_age = pop_ethnicity_mapped_ethnicity_sex_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_high_age = pop_ethnicity_mapped_ethnicity_high_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_high_sex_age = pop_ethnicity_mapped_ethnicity_high_sex_age %>% filter(YEAR==2011)


```


```{r, esp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

ESP = 
esp_groups %>%
  select(age_group,population_ord_group) %>%
  add_row(age_group=c(">=90"),population_ord_group=c(1000)) %>%
  mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(age_group) %>% summarise(population_ord_group=sum(population_ord_group)) %>%
  mutate(prop=population_ord_group/sum(population_ord_group)) %>% select(-population_ord_group)

```


# Death - Time to CVD and All-Cause Death

Disease Specific Regression

* Underlying Cause of Death used

```{r, outcomesDf, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# Prepare dataset
outcomes_df = analysis_cohort %>%
  mutate(treatment_time = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_)) %>%
  select(surgery_type,Sex=SEX,age,age_group,
         OPERATION_DATE,
         treatment_time,
         #treatment_time=treat,
         Deprivation=QUINTILE,Ethnicity=ETHNIC_DESC_HIGH_ONLY,
         outcomes_death_description_high,
         outcomes_time_to_death,

         ckd,
diabetes,
heart_failure,
hypertension,
pad,
asthma_copd,
cerebrovascular_disease,
coronary_artery_disease,
OPERATION_YEAR) %>%
  ungroup() %>%
  filter(age_group!="Unknown") %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
  mutate(surgery_type=factor(surgery_type, levels=c("SAVR","TAVI"))) %>%
  # mutate(treatment_time = case_when(
  #   treatment_time == "PRIOR - Beginning" ~ "2000-2004",
  #   treatment_time == "PRIOR - Middle" ~ "2005-2014", 
  #   treatment_time == "PRIOR - End" ~ "2015-2019",
  #   treatment_time == "PANDEMIC - During" ~ "2020-2023",
  #   TRUE ~ NA_character_)) %>%
  mutate(treatment_time=factor(treatment_time,
                               levels=c("2000-2004",
                                        "2005-2009",
                                        "2010-2014",
                                        "2015-2019",
                                        "2020-2023"))) %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups",
                                                    "Other ethnic groups"))) %>%
  mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5"))) %>%
  
  mutate(Sex=factor(Sex,levels=c("Male","Female"))) %>%
  
  filter(!is.na(Ethnicity)) %>%
  filter(Sex!="Unknown") %>%
  filter(Deprivation!="Unknown") %>%
  rename(Age=age, `Treatment_Time` = treatment_time) %>%

  # REMOVE OTHER ETHNIC GROUP
  filter(Ethnicity != "Other ethnic groups") %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))
  




```



```{r, outcomesDfDeath, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

outcomes_death_df = outcomes_df %>%

# # Competing risks regression
# mutate(status_death_crr = ifelse(is.na(outcomes_time_to_death),0,
#                       ifelse(!is.na(outcomes_time_to_death)&outcomes_death_description_high=="cvd_death",1,
#                       ifelse(!is.na(outcomes_time_to_death)&outcomes_death_description_high=="all_cause_death",2,
#                       NA)))) %>% # 0=censored, 1=died of cvd, 2=died of non cvd
# mutate(time_death_crr =  ifelse(status_death_crr==0,
#                             difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
#                             outcomes_time_to_death
#                      )) %>%
  
# Overall Survival - CVD and Non CVD Death (All cause Death)
mutate(status_death_os = ifelse(is.na(outcomes_time_to_death),0,
                      ifelse(!is.na(outcomes_time_to_death),1,
                      NA))) %>% # 0=censored, 1=died of cvd or non cvd
mutate(time_death_os =  ifelse(status_death_os==0,
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_death
                     )) %>%
  
# Disease specific - CVD (Non CVD is censored but at time of death instead of study end date)
mutate(status_death_ds_cvd = ifelse(is.na(outcomes_time_to_death),0,
                      ifelse(!is.na(outcomes_time_to_death)&outcomes_death_description_high=="cvd_death",1,
                      ifelse(!is.na(outcomes_time_to_death)&outcomes_death_description_high=="non_cvd_death",0,
                      NA)))) %>% # 0=censored, 1=died of cvd, 0=died of non cvd
mutate(time_death_ds_cvd =  ifelse(is.na(outcomes_time_to_death),
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_death
                     )) 


outcomes_death_df_savr = outcomes_death_df %>% filter(surgery_type=="SAVR")
outcomes_death_df_tavi = outcomes_death_df %>% filter(surgery_type=="TAVI")

outcomes_death_df_savr %>% write_rds("tmp/outcomes_death_df_savr.rds")
outcomes_death_df_tavi %>% write_rds("tmp/outcomes_death_df_tavi.rds")

```


## SAVR

### Model 0 - Ethnicity


```{r, model0CVDDeath_SAVR, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

outcomes_death_df_savr = read_rds("tmp/outcomes_death_df_savr.rds")
outcomes_death_df_tavi = read_rds("tmp/outcomes_death_df_tavi.rds")

dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")

dependent_os_model0 = "Surv(time_death_os, status_death_os)"

os_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l",
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_savr_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 0 - Sex

```{r, model0CVDDeath_SAVR_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_savr_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 0 - Deprivation

```{r, model0CVDDeath_SAVR_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


#########


dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model0 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```

```{r, forrest_model0_savr_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Ethnicity + Age


```{r, model1CVDDeath_SAVR_comb_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_savr_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDDeath_SAVR_comb_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_savr_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Deprivation + Age


```{r, model1CVDDeath_SAVR_comb_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_savr_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDDeath_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model2 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model2 = "Surv(time_death_os, status_death_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_death_df_savr %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model2 = outcomes_death_df_savr %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```



```{r, forrest_model2_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_savr %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_SAVR_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_death_df_savr_t = outcomes_death_df_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```



```{r, model3CVDDeath_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model3 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model3 = "Surv(time_death_os, status_death_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_death_df_savr_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model3 = outcomes_death_df_savr_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model3_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_savr_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time



```{r, model4CVDDeath_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model4 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model4 = "Surv(time_death_os, status_death_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


ds_survival_model4 = outcomes_death_df_savr_t %>% 
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model4 = outcomes_death_df_savr_t %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model4_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_savr_t %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_savr_t %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```



## TAVI

### Model 0 - Ethnicity


```{r, model0CVDDeath_TAVI, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

outcomes_death_df_tavi = read_rds("tmp/outcomes_death_df_tavi.rds")
outcomes_death_df_tavi = read_rds("tmp/outcomes_death_df_tavi.rds")

dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")

dependent_os_model0 = "Surv(time_death_os, status_death_os)"

os_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l",
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_tavi_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 0 - Sex

```{r, model0CVDDeath_TAVI_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_tavi_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 0 - Deprivation

```{r, model0CVDDeath_TAVI_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


#########


dependent_ds_cvd_model0 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model0 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```

```{r, forrest_model0_tavi_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Ethnicity + Age


```{r, model1CVDDeath_TAVI_comb_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_tavi_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDDeath_TAVI_comb_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_tavi_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Deprivation + Age


```{r, model1CVDDeath_TAVI_comb_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model1 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model1 = "Surv(time_death_os, status_death_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model1 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model1_tavi_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDDeath_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model2 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model2 = "Surv(time_death_os, status_death_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_death_df_tavi %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model2 = outcomes_death_df_tavi %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```



```{r, forrest_model2_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_TAVI_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_death_df_tavi_t = outcomes_death_df_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```



```{r, model3CVDDeath_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model3 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model3 = "Surv(time_death_os, status_death_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_death_df_tavi_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model3 = outcomes_death_df_tavi_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))

```


```{r, forrest_model3_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time


```{r, model4CVDDeath_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

#########


dependent_ds_cvd_model4 = "Surv(time_death_ds_cvd, status_death_ds_cvd)"
dependent_os_model4 = "Surv(time_death_os, status_death_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


outcomes_death_df_tavi_tt = outcomes_death_df_tavi_t %>%
  filter(Treatment_Time!="2000-2004") %>%
  mutate(Treatment_Time=as.character(Treatment_Time)) %>%
  mutate(Treatment_Time=factor(Treatment_Time, levels=c("2005-2009","2010-2014","2015-2019","2020-2023")))

ds_survival_model4 = outcomes_death_df_tavi_tt %>%
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_ds_cvd, status_death_ds_cvd)")


os_survival_model4 = outcomes_death_df_tavi_tt %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_death_os, status_death_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "CVD Death" = 2, " "=1,"All-cause Death" = 2)) %>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model4_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_death_df_tavi_tt %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - CVD Death ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_death_df_tavi_tt %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Death ",
            table_text_size = 6,title_text_size = 20
            )



```








# Hospitalisations - Time to Heart Failure and All-Cause Hospitalisation

* Those who died have been censored as at date of death.
* A (re)hospitalisation accounts for the date of discharge of the date of surgery. In ~10% there was no date of discharge available and so the median time from Date or Surgery to Date of Discharge from the other 90% was used to impute. That is, 7 days was added on to Date of Surgery to impute a Date of Discharge.
* Code in primary position used.

```{r, Hosp_Working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

test=outcomes_hospitalisations_raw  %>% 
  #filter(PERSON_ID=="23HVI336A09Z7OO") %>%
  group_by(PERSON_ID,DATE) %>%
  distinct() %>%
  mutate(test=substr(CODE, start = 1, stop = 3)) %>%
  distinct() %>%
  relocate(test,.after="CODE") %>%
  group_by(PERSON_ID,test,DATE) %>% 
  #mutate(row_num=row_number()) %>%
  # relocate(row_num,.after="test") 
  pivot_wider(names_from=DIAG_DIGITS,values_from=c(CODE,outcomes_hosp_description)) %>%
  mutate(has_cvd_4 = map_lgl(outcomes_hosp_description_4, ~ "cvd_hosp" %in% .x)) %>%
  mutate(has_cvd_3 = map_lgl(outcomes_hosp_description_3, ~ "cvd_hosp" %in% .x))
  

test1 = test %>%
  mutate(outcomes_hosp_description=ifelse(has_cvd_3==FALSE & has_cvd_4==FALSE, "non_cvd_hosp","cvd_hosp"))





outcomes_hospitalisations_raw %>%
  filter(outcomes_hosp_description=="cvd_hosp") %>% distinct(CODE)

# outcomes_hospitalisations_raw %>%
#   filter(outcomes_hosp_description=="non_cvd_hosp") %>%
#   distinct(CODE) %>% filter(str_starts(CODE,"I50")) #I13, I139, I131, I11, I119, I500,I509,I501


outcomes_hosp_prepared = 
test1 %>%
  ungroup() %>%
  distinct(PERSON_ID,DATE,CENSOR_DATE_START,CENSOR_DATE_END,outcomes_hosp_description) %>%
  rename(DOH=DATE) %>%
  filter(DOH<=CENSOR_DATE_END) %>% 
  group_by(PERSON_ID,outcomes_hosp_description) %>%
  filter(DOH == min(DOH)) %>%
  ungroup() %>%
  group_by(PERSON_ID) %>% mutate(events_total = n()) %>% ungroup() %>%
  arrange(PERSON_ID,DOH) %>%
  group_by(PERSON_ID) %>%
  mutate(event_no = row_number()) %>%
  ungroup() %>%
  # get rid of those who have 2 evetns and the first is CVD and second Non CVD because the CVD will be All cause
  filter(!(events_total==2 & event_no==2 & outcomes_hosp_description=="non_cvd_hosp")) %>%
  group_by(PERSON_ID) %>% mutate(events_total = n()) %>% ungroup()
  # now everyone that has 1 event will have either cvd or non cvd (CVD counted in CVD and both counted in All Cause)
  # and if someone has 2 events they had a non cvd first then CVD - in the All Cause cause event no 1 is used but in the CVD case then event no 1 is ignored and event 2 is used

outcomes_hosp_prepared %>% write_rds("tmp/outcomes_hosp_prepared.rds")

  

```


```{r, Hosp_Working_read, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


outcomes_hosp_prepared = read_rds("tmp/outcomes_hosp_prepared.rds")


```



```{r, outcomesDfHosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df = analysis_cohort %>%
  mutate(treatment_time = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_)) %>%
  select(PERSON_ID,surgery_type,Sex=SEX,age,age_group,
         OPERATION_DATE,
         treatment_time,
         #treatment_time=treat,
         Deprivation=QUINTILE,Ethnicity=ETHNIC_DESC_HIGH_ONLY,
         outcomes_death_description_high,
         outcomes_time_to_death,

         ckd,
diabetes,
heart_failure,
hypertension,
pad,
asthma_copd,
cerebrovascular_disease,
coronary_artery_disease,
OPERATION_YEAR) %>%
  ungroup() %>%
  filter(age_group!="Unknown") %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
  mutate(surgery_type=factor(surgery_type, levels=c("SAVR","TAVI"))) %>%
  # mutate(treatment_time = case_when(
  #   treatment_time == "PRIOR - Beginning" ~ "2000-2004",
  #   treatment_time == "PRIOR - Middle" ~ "2005-2014", 
  #   treatment_time == "PRIOR - End" ~ "2015-2019",
  #   treatment_time == "PANDEMIC - During" ~ "2020-2023",
  #   TRUE ~ NA_character_)) %>%
  mutate(treatment_time=factor(treatment_time,
                               levels=c("2000-2004",
                                        "2005-2009",
                                        "2010-2014",
                                        "2015-2019",
                                        "2020-2023"))) %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups"))) %>%
  mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5"))) %>%
  
  mutate(Sex=factor(Sex,levels=c("Male","Female"))) %>%
  
  filter(!is.na(Ethnicity)) %>%
  filter(Sex!="Unknown") %>%
  filter(Deprivation!="Unknown") %>%
  rename(Age=age, `Treatment_Time` = treatment_time) %>%

  # REMOVE OTHER ETHNIC GROUP
  filter(Ethnicity != "Other ethnic groups") %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups"))) %>%
  left_join(outcomes_hosp_prepared) %>%
  mutate(outcomes_time_to_hosp = as.numeric(DOH-OPERATION_DATE))


outcomes_hosp_df_os = outcomes_hosp_df %>%
  filter(event_no==1 | is.na(event_no)) %>%
# Overall Survival - CVD and Non CVD Death (All cause Death)
mutate(status_hosp_os = ifelse(is.na(outcomes_time_to_hosp),0,
                      ifelse(!is.na(outcomes_time_to_hosp),1,
                      NA))) %>% # 0=censored
mutate(time_hosp_os =  ifelse(status_hosp_os==0,
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_hosp
                     )) %>%
  #censor those who died at appropriate time
mutate(time_hosp_os = ifelse(status_hosp_os == 0 & !is.na(outcomes_time_to_death), outcomes_time_to_death,time_hosp_os))
  
outcomes_hosp_df_ds_cvd = outcomes_hosp_df %>%
  mutate(outcomes_time_to_hosp=ifelse(events_total==1&outcomes_hosp_description=="non_cvd_hosp",
                                      NA,outcomes_time_to_hosp),
  outcomes_hosp_description=ifelse(events_total==1&outcomes_hosp_description=="non_cvd_hosp",
                                      NA,outcomes_hosp_description)
         ) %>%
  mutate(remove=ifelse((events_total==2&outcomes_hosp_description=="non_cvd_hosp" & event_no==1),1,0)) %>%
  filter(remove==0 | is.na(remove)) %>%
# Disease specific - CVD (Non CVD is censored but at time of death instead of study end date)
mutate(status_hosp_ds_cvd = ifelse(is.na(outcomes_time_to_hosp),0,
                      ifelse(!is.na(outcomes_time_to_hosp),1,
                      NA))) %>% # 0=censored, 1=died of cvd or non cvd
mutate(time_hosp_ds_cvd =  ifelse(status_hosp_ds_cvd==0,
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_hosp
                     )) %>%
  #censor those who died at appropriate time
mutate(time_hosp_ds_cvd = ifelse(status_hosp_ds_cvd == 0 & !is.na(outcomes_time_to_death), outcomes_time_to_death,time_hosp_ds_cvd))


outcomes_hosp_df_ds_cvd_savr = outcomes_hosp_df_ds_cvd %>% filter(surgery_type=="SAVR")
outcomes_hosp_df_ds_cvd_tavi = outcomes_hosp_df_ds_cvd %>% filter(surgery_type=="TAVI")

outcomes_hosp_df_os_savr = outcomes_hosp_df_os %>% filter(surgery_type=="SAVR")
outcomes_hosp_df_os_tavi = outcomes_hosp_df_os %>% filter(surgery_type=="TAVI")


```




## SAVR

### Model 0 - Ethnicity


```{r, model0CVDHospitalisation_SAVR, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_savr_Hosp_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 0 - Sex

```{r, model0CVDHospitalisation_SAVR_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model0_savr_Hosp_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```


### Model 0 - Deprivation

```{r, model0CVDHospitalisation_SAVR_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_savr_Hosp_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 1 - Ethnicity + Age


```{r, model1CVDHospitalisation_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDHospitalisation_SAVR_comb_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 1 - Deprivation + Age


```{r, model1CVDHospitalisation_SAVR_comb_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```



### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDHospitalisation_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model2 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model2 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```



```{r, forrest_model2_savr_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_SAVR_Hosp_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_savr_t = outcomes_hosp_df_os_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_savr_t = outcomes_hosp_df_ds_cvd_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model3CVDHospitalisation_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model3 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_hosp_df_ds_cvd_savr_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model3 = outcomes_hosp_df_os_savr_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model3_savr_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time


```{r, model4Summary_SAVR_Hosp_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_savr_t = outcomes_hosp_df_os_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_savr_t = outcomes_hosp_df_ds_cvd_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model4CVDHospitalisation_SAVR_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model4 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


ds_survival_model4 = outcomes_hosp_df_ds_cvd_savr_t %>% 
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model4 = outcomes_hosp_df_os_savr_t %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model4_savr_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr_t %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr_t %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```




## TAVI


### Model 0 - Ethnicity


```{r, model0CVDHospitalisation_TAVI, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>%
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_tavi_Hosp_eth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 0 - Sex

```{r, model0CVDHospitalisation_TAVI_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model0_tavi_Hosp_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```


### Model 0 - Deprivation

```{r, model0CVDHospitalisation_TAVI_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_tavi_Hosp_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 1 - Ethnicity + Age


```{r, model1CVDHospitalisation_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDHospitalisation_TAVI_comb_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp_sex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 1 - Deprivation + Age


```{r, model1CVDHospitalisation_TAVI_comb_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp_dep, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```



### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDHospitalisation_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model2 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model2 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```



```{r, forrest_model2_tavi_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_TAVI_Hosp_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_tavi_t = outcomes_hosp_df_os_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_tavi_t = outcomes_hosp_df_ds_cvd_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model3CVDHospitalisation_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model3 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_hosp_df_ds_cvd_tavi_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model3 = outcomes_hosp_df_os_tavi_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model3_tavi_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time


```{r, model4Summary_TAVI_Hosp_Working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_tavi_t = outcomes_hosp_df_os_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_tavi_t = outcomes_hosp_df_ds_cvd_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)



```


```{r, model4CVDHospitalisation_TAVI_comb, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model4 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


outcomes_hosp_df_ds_cvd_tavi_tt = outcomes_hosp_df_ds_cvd_tavi_t %>%
  filter(Treatment_Time!="2000-2004") %>%
  mutate(Treatment_Time=as.character(Treatment_Time)) %>%
  mutate(Treatment_Time=factor(Treatment_Time, levels=c("2005-2009","2010-2014","2015-2019","2020-2023")))

outcomes_hosp_df_os_tavi_tt = outcomes_hosp_df_os_tavi_t %>%
  filter(Treatment_Time!="2000-2004") %>%
  mutate(Treatment_Time=as.character(Treatment_Time)) %>%
  mutate(Treatment_Time=factor(Treatment_Time, levels=c("2005-2009","2010-2014","2015-2019","2020-2023")))


ds_survival_model4 = outcomes_hosp_df_ds_cvd_tavi_tt %>% 
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model4 = outcomes_hosp_df_os_tavi_tt %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model4_tavi_Hosp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi_tt %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi_tt %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```










# Hospitalisations - Time to CVD and All-Cause Hospitalisation

CVD Hospitalisation codes include the same CVD Death codes (minus sudden death) and HF codes.


```{r, Hosp_Working_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

test_cvd=outcomes_hospitalisations_raw_cvd  %>% 
  #filter(PERSON_ID=="23HVI336A09Z7OO") %>%
  group_by(PERSON_ID,DATE) %>%
  distinct() %>%
  mutate(test=substr(CODE, start = 1, stop = 3)) %>%
  distinct() %>%
  relocate(test,.after="CODE") %>%
  group_by(PERSON_ID,test,DATE) %>% 
  #mutate(row_num=row_number()) %>%
  # relocate(row_num,.after="test") 
  pivot_wider(names_from=DIAG_DIGITS,values_from=c(CODE,outcomes_hosp_description)) %>%
  mutate(has_cvd_4 = map_lgl(outcomes_hosp_description_4, ~ "cvd_hosp" %in% .x)) %>%
  mutate(has_cvd_3 = map_lgl(outcomes_hosp_description_3, ~ "cvd_hosp" %in% .x))
  

test1_cvd = test %>%
  mutate(outcomes_hosp_description=ifelse(has_cvd_3==FALSE & has_cvd_4==FALSE, "non_cvd_hosp","cvd_hosp"))



outcomes_hosp_prepared_cvd = 
test1_cvd %>%
  ungroup() %>%
  distinct(PERSON_ID,DATE,CENSOR_DATE_START,CENSOR_DATE_END,outcomes_hosp_description) %>%
  rename(DOH=DATE) %>%
  filter(DOH<=CENSOR_DATE_END) %>% 
  group_by(PERSON_ID,outcomes_hosp_description) %>%
  filter(DOH == min(DOH)) %>%
  ungroup() %>%
  group_by(PERSON_ID) %>% mutate(events_total = n()) %>% ungroup() %>%
  arrange(PERSON_ID,DOH) %>%
  group_by(PERSON_ID) %>%
  mutate(event_no = row_number()) %>%
  ungroup() %>%
  # get rid of those who have 2 evetns and the first is CVD and second Non CVD because the CVD will be All cause
  filter(!(events_total==2 & event_no==2 & outcomes_hosp_description=="non_cvd_hosp")) %>%
  group_by(PERSON_ID) %>% mutate(events_total = n()) %>% ungroup()
  # now everyone that has 1 event will have either cvd or non cvd (CVD counted in CVD and both counted in All Cause)
  # and if someone has 2 events they had a non cvd first then CVD - in the All Cause cause event no 1 is used but in the CVD case then event no 1 is ignored and event 2 is used

outcomes_hosp_prepared_cvd %>% write_rds("tmp/outcomes_hosp_prepared_cvd.rds")

  

```


```{r, Hosp_Working_read_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


outcomes_hosp_prepared_cvd = read_rds("tmp/outcomes_hosp_prepared_cvd.rds")

outcomes_hosp_prepared = outcomes_hosp_prepared_cvd

```



```{r, outcomesDfHosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df = analysis_cohort %>%
  mutate(treatment_time = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_)) %>%
  select(PERSON_ID,surgery_type,Sex=SEX,age,age_group,
         OPERATION_DATE,
         treatment_time,
         #treatment_time=treat,
         Deprivation=QUINTILE,Ethnicity=ETHNIC_DESC_HIGH_ONLY,
         outcomes_death_description_high,
         outcomes_time_to_death,

         ckd,
diabetes,
heart_failure,
hypertension,
pad,
asthma_copd,
cerebrovascular_disease,
coronary_artery_disease,
OPERATION_YEAR) %>%
  ungroup() %>%
  filter(age_group!="Unknown") %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
  mutate(surgery_type=factor(surgery_type, levels=c("SAVR","TAVI"))) %>%
  # mutate(treatment_time = case_when(
  #   treatment_time == "PRIOR - Beginning" ~ "2000-2004",
  #   treatment_time == "PRIOR - Middle" ~ "2005-2014", 
  #   treatment_time == "PRIOR - End" ~ "2015-2019",
  #   treatment_time == "PANDEMIC - During" ~ "2020-2023",
  #   TRUE ~ NA_character_)) %>%
  mutate(treatment_time=factor(treatment_time,
                               levels=c("2000-2004",
                                        "2005-2009",
                                        "2010-2014",
                                        "2015-2019",
                                        "2020-2023"))) %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups"))) %>%
  mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5"))) %>%
  
  mutate(Sex=factor(Sex,levels=c("Male","Female"))) %>%
  
  filter(!is.na(Ethnicity)) %>%
  filter(Sex!="Unknown") %>%
  filter(Deprivation!="Unknown") %>%
  rename(Age=age, `Treatment_Time` = treatment_time) %>%

  # REMOVE OTHER ETHNIC GROUP
  filter(Ethnicity != "Other ethnic groups") %>%
  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups"))) %>%
  left_join(outcomes_hosp_prepared) %>%
  mutate(outcomes_time_to_hosp = as.numeric(DOH-OPERATION_DATE))


outcomes_hosp_df_os = outcomes_hosp_df %>%
  filter(event_no==1 | is.na(event_no)) %>%
# Overall Survival - CVD and Non CVD Death (All cause Death)
mutate(status_hosp_os = ifelse(is.na(outcomes_time_to_hosp),0,
                      ifelse(!is.na(outcomes_time_to_hosp),1,
                      NA))) %>% # 0=censored
mutate(time_hosp_os =  ifelse(status_hosp_os==0,
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_hosp
                     )) %>%
  #censor those who died at appropriate time
mutate(time_hosp_os = ifelse(status_hosp_os == 0 & !is.na(outcomes_time_to_death), outcomes_time_to_death,time_hosp_os))
  
outcomes_hosp_df_ds_cvd = outcomes_hosp_df %>%
  mutate(outcomes_time_to_hosp=ifelse(events_total==1&outcomes_hosp_description=="non_cvd_hosp",
                                      NA,outcomes_time_to_hosp),
  outcomes_hosp_description=ifelse(events_total==1&outcomes_hosp_description=="non_cvd_hosp",
                                      NA,outcomes_hosp_description)
         ) %>%
  mutate(remove=ifelse((events_total==2&outcomes_hosp_description=="non_cvd_hosp" & event_no==1),1,0)) %>%
  filter(remove==0 | is.na(remove)) %>%
# Disease specific - CVD (Non CVD is censored but at time of death instead of study end date)
mutate(status_hosp_ds_cvd = ifelse(is.na(outcomes_time_to_hosp),0,
                      ifelse(!is.na(outcomes_time_to_hosp),1,
                      NA))) %>% # 0=censored, 1=died of cvd or non cvd
mutate(time_hosp_ds_cvd =  ifelse(status_hosp_ds_cvd==0,
                            difftime(ymd('2023-11-28'), OPERATION_DATE, units="days"), #study end date
                            outcomes_time_to_hosp
                     )) %>%
  #censor those who died at appropriate time
mutate(time_hosp_ds_cvd = ifelse(status_hosp_ds_cvd == 0 & !is.na(outcomes_time_to_death), outcomes_time_to_death,time_hosp_ds_cvd))


outcomes_hosp_df_ds_cvd_savr = outcomes_hosp_df_ds_cvd %>% filter(surgery_type=="SAVR")
outcomes_hosp_df_ds_cvd_tavi = outcomes_hosp_df_ds_cvd %>% filter(surgery_type=="TAVI")

outcomes_hosp_df_os_savr = outcomes_hosp_df_os %>% filter(surgery_type=="SAVR")
outcomes_hosp_df_os_tavi = outcomes_hosp_df_os %>% filter(surgery_type=="TAVI")


```



## SAVR

### Model 0 - Ethnicity


```{r, model0CVDHospitalisation_SAVR_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_savr_Hosp_eth_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 0 - Sex

```{r, model0CVDHospitalisation_SAVR_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model0_savr_Hosp_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```


### Model 0 - Deprivation

```{r, model0CVDHospitalisation_SAVR_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_savr_Hosp_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 1 - Ethnicity + Age


```{r, model1CVDHospitalisation_SAVR_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDHospitalisation_SAVR_comb_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 1 - Deprivation + Age


```{r, model1CVDHospitalisation_SAVR_comb_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_savr_Hosp_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```



### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDHospitalisation_SAVR_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model2 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_hosp_df_ds_cvd_savr %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model2 = outcomes_hosp_df_os_savr %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```



```{r, forrest_model2_savr_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_SAVR_Hosp_Working_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_savr_t = outcomes_hosp_df_os_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_savr_t = outcomes_hosp_df_ds_cvd_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model3CVDHospitalisation_SAVR_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model3 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_hosp_df_ds_cvd_savr_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model3 = outcomes_hosp_df_os_savr_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model3_savr_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time


```{r, model4Summary_SAVR_Hosp_Working_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_savr_t = outcomes_hosp_df_os_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_savr_t = outcomes_hosp_df_ds_cvd_savr %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model4CVDHospitalisation_SAVR_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model4 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


ds_survival_model4 = outcomes_hosp_df_ds_cvd_savr_t %>% 
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model4 = outcomes_hosp_df_os_savr_t %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("SAVR"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model4_savr_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_savr_t %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_savr_t %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="SAVR - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```




## TAVI


### Model 0 - Ethnicity


```{r, model0CVDHospitalisation_TAVI_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Ethnicity")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```

```{r, forrest_model0_tavi_Hosp_eth_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 0 - Sex

```{r, model0CVDHospitalisation_TAVI_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Sex")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model0_tavi_Hosp_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```


### Model 0 - Deprivation

```{r, model0CVDHospitalisation_TAVI_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}
#########


dependent_ds_cvd_model0 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
explanatory_model0 = c("Deprivation")


ds_survival_model0 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model0 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model0 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model0, explanatory_model0) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model0 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (multivariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model0%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (multivariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))


```

```{r, forrest_model0_tavi_Hosp_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model0, explanatory_model0,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

```

### Model 1 - Ethnicity + Age


```{r, model1CVDHospitalisation_TAVI_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Ethnicity","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 1 - Sex + Age


```{r, model1CVDHospitalisation_TAVI_comb_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Sex","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp_sex_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```

### Model 1 - Deprivation + Age


```{r, model1CVDHospitalisation_TAVI_comb_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model1 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model1 = c("Deprivation","Age")


ds_survival_model1 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model1 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model1 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model1, explanatory_model1) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model1 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model1%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model1_tavi_Hosp_dep_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model1, explanatory_model1,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```



### Model 2 - Ethnicity + Sex + Deprivation + Age


```{r, model2CVDHospitalisation_TAVI_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model2 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model2 = c("Ethnicity","Sex","Deprivation","Age")


ds_survival_model2 = outcomes_hosp_df_ds_cvd_tavi %>% 
    finalfit(dependent_ds_cvd_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model2 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model2 = outcomes_hosp_df_os_tavi %>% 
    finalfit(dependent_os_model2, explanatory_model2) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model2 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model2%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```



```{r, forrest_model2_tavi_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi %>% 
    hr_plot(dependent_ds_cvd_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi %>% 
    hr_plot(dependent_os_model2, explanatory_model2,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 3 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Year of Surgery


```{r, model3Summary_TAVI_Hosp_Working_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_tavi_t = outcomes_hosp_df_os_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_tavi_t = outcomes_hosp_df_ds_cvd_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model3CVDHospitalisation_TAVI_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model3 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model3 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Year_Surgery")


ds_survival_model3 = outcomes_hosp_df_ds_cvd_tavi_t %>% 
    finalfit(dependent_ds_cvd_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model3 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model3 = outcomes_hosp_df_os_tavi_t %>% 
    finalfit(dependent_os_model3, explanatory_model3) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model3 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model3%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model3_tavi_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi_t %>% 
    hr_plot(dependent_ds_cvd_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi_t %>% 
    hr_plot(dependent_os_model3, explanatory_model3,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```


### Model 4 - Ethnicity + Sex + Deprivation + Age + Comorbidities + Treatment Time


```{r, model4Summary_TAVI_Hosp_Working_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

outcomes_hosp_df_os_tavi_t = outcomes_hosp_df_os_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

outcomes_hosp_df_ds_cvd_tavi_t = outcomes_hosp_df_ds_cvd_tavi %>% rename(CKD=ckd,
Diabetes=diabetes,
`Heart_Failure`=heart_failure,
Hypertension = hypertension,
PAD = pad,
`Asthma_COPD` = asthma_copd,
`Cerebrovascular_Disease` = cerebrovascular_disease,
`Coronary_Artery_Disease` = coronary_artery_disease,
`Year_Surgery`=OPERATION_YEAR)

```


```{r, model4CVDHospitalisation_TAVI_comb_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}


dependent_ds_cvd_model4 = "Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)"
dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"
explanatory_model4 = c("Ethnicity","Sex","Deprivation","Age","CKD","Heart_Failure","Hypertension","PAD","Asthma_COPD","Cerebrovascular_Disease","Coronary_Artery_Disease","Treatment_Time")


outcomes_hosp_df_ds_cvd_tavi_tt = outcomes_hosp_df_ds_cvd_tavi_t %>%
  filter(Treatment_Time!="2000-2004") %>%
  mutate(Treatment_Time=as.character(Treatment_Time)) %>%
  mutate(Treatment_Time=factor(Treatment_Time, levels=c("2005-2009","2010-2014","2015-2019","2020-2023")))

outcomes_hosp_df_os_tavi_tt = outcomes_hosp_df_os_tavi_t %>%
  filter(Treatment_Time!="2000-2004") %>%
  mutate(Treatment_Time=as.character(Treatment_Time)) %>%
  mutate(Treatment_Time=factor(Treatment_Time, levels=c("2005-2009","2010-2014","2015-2019","2020-2023")))


ds_survival_model4 = outcomes_hosp_df_ds_cvd_tavi_tt %>% 
    finalfit(dependent_ds_cvd_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_ds_cvd, status_hosp_ds_cvd)")

dependent_os_model4 = "Surv(time_hosp_os, status_hosp_os)"

os_survival_model4 = outcomes_hosp_df_os_tavi_tt %>% 
    finalfit(dependent_os_model4, explanatory_model4) %>%
    rename("Overall survival" = "Dependent: Surv(time_hosp_os, status_hosp_os)")

kableExtra::kbl(
  ds_survival_model4 %>% 
    remove_rownames() %>% 
    separate(all,into=c("all","pct"),sep=" ") %>% 
    select(-"HR (univariable)")%>%
    mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct) %>%
    mutate("  "=" ") %>%
  cbind(os_survival_model4%>% 
          remove_rownames() %>% 
          separate(all,into=c("all","pct"),sep=" ")%>%
          rename(f = " ")%>%
          select(-"HR (univariable)")%>%mutate(all=as.numeric(all)) %>%
    mutate(all = 
             ifelse(`Overall survival` == "Age", all,
             ifelse(all<10,10,round(all/5)*5)
               )) %>%
    mutate(all=ifelse(`Overall survival` == "Age", as.character(all),
                      (as.character(all)))) %>%
    mutate(all=paste0(all, " ", pct)) %>%
    rename(All=all,`Overall Survival`=`Overall survival`) %>% select(-pct)%>%select(-"Overall Survival",-"f"))
    ,
    booktabs = T, 
    caption=("TAVI"),
    linesep = "",
    align="l"
    ) %>%
  add_header_above(header = c(" " = 2, "HF Hospitalisation" = 2, " "=1,"All-cause Hospitalisation" = 2))%>%
  kable_styling(latex_options = c("scale_down"))



```


```{r, forrest_model4_tavi_Hosp_cvd, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=20,fig.show="hold", out.width="50%"}


outcomes_hosp_df_ds_cvd_tavi_tt %>% 
    hr_plot(dependent_ds_cvd_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - HF Hospitalisaion ",
            table_text_size = 6,title_text_size = 20
            )

outcomes_hosp_df_os_tavi_tt %>% 
    hr_plot(dependent_os_model4, explanatory_model4,
            plot_opts=list(geom_point(aes(size = Total), shape=22, fill="#F51344")),
            prefix="TAVI - All-Cause Hospitalisation ",
            table_text_size = 6,title_text_size = 20
            )



```










